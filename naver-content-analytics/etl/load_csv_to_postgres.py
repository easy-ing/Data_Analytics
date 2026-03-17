import argparse
import json
import logging
import os
import uuid
from pathlib import Path
from typing import Iterable

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")
DEFAULT_RAW_DIR = ROOT / "data" / "raw"


def get_logger() -> logging.Logger:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    return logging.getLogger("etl.load_csv_to_postgres")


def get_engine() -> Engine:
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    db = os.getenv("DB_NAME", "analytics")
    user = os.getenv("DB_USER", "postgres")
    pw = os.getenv("DB_PASSWORD", "postgres")
    return create_engine(f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}")


def pick_contents_csv(raw_dir: Path) -> Path:
    candidates = [raw_dir / "contents.csv", raw_dir / "content.csv"]
    for c in candidates:
        if c.exists():
            return c
    raise FileNotFoundError("Missing contents.csv (or legacy content.csv)")


def read_csvs(raw_dir: Path, logger: logging.Logger) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    users_path = raw_dir / "users.csv"
    contents_path = pick_contents_csv(raw_dir)
    events_path = raw_dir / "events.csv"

    logger.info("Reading CSVs from %s", raw_dir)
    users = pd.read_csv(users_path)
    contents = pd.read_csv(contents_path)
    events = pd.read_csv(events_path)
    logger.info("Raw rows users=%d, contents=%d, events=%d", len(users), len(contents), len(events))
    return users, contents, events


def preprocess_users(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["user_id"] = pd.to_numeric(out["user_id"], errors="coerce")
    out = out.dropna(subset=["user_id"]).copy()
    out["user_id"] = out["user_id"].astype(int)

    out["signup_at"] = pd.to_datetime(out.get("signup_at"), errors="coerce")
    out["signup_at"] = out["signup_at"].fillna(pd.Timestamp("2025-01-01 00:00:00"))

    out["age_group"] = out.get("age_group", "unknown").fillna("unknown").astype(str)
    out["gender"] = out.get("gender", "U").fillna("U").astype(str)
    out["region"] = out.get("region", "unknown").fillna("unknown").astype(str)
    out["acquisition_channel"] = out.get("acquisition_channel", "unknown").fillna("unknown").astype(str)

    return out[["user_id", "signup_at", "age_group", "gender", "region", "acquisition_channel"]].drop_duplicates(
        subset=["user_id"]
    )


def preprocess_contents(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["content_id"] = pd.to_numeric(out["content_id"], errors="coerce")
    out = out.dropna(subset=["content_id"]).copy()
    out["content_id"] = out["content_id"].astype(int)

    out["content_title"] = out.get("content_title", "untitled").fillna("untitled").astype(str)
    out["content_category"] = out.get("content_category", "unknown").fillna("unknown").astype(str)
    out["content_type"] = out.get("content_type", "article").fillna("article").astype(str)
    out["author_id"] = pd.to_numeric(out.get("author_id", 0), errors="coerce").fillna(0).astype(int)
    out["publish_at"] = pd.to_datetime(out.get("publish_at"), errors="coerce")
    out["publish_at"] = out["publish_at"].fillna(pd.Timestamp("2025-01-01 00:00:00"))

    return out[
        ["content_id", "content_title", "content_category", "content_type", "author_id", "publish_at"]
    ].drop_duplicates(subset=["content_id"])


def normalize_event_id(v: str) -> str:
    raw = str(v).strip()
    try:
        return str(uuid.UUID(raw))
    except ValueError:
        if len(raw) == 32:
            return str(uuid.UUID(hex=raw))
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, raw))


def normalize_metadata(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "{}"
    txt = str(v).strip()
    if not txt:
        return "{}"
    try:
        json.loads(txt)
        return txt
    except json.JSONDecodeError:
        return json.dumps({"raw": txt}, ensure_ascii=True)


def preprocess_events(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    required = ["event_id", "event_ts", "user_id", "content_id", "session_id", "event_type"]
    missing = [c for c in required if c not in out.columns]
    if missing:
        raise ValueError(f"Missing required columns in events.csv: {missing}")

    out["event_id"] = out["event_id"].map(normalize_event_id)
    out["event_ts"] = pd.to_datetime(out["event_ts"], errors="coerce")
    out = out.dropna(subset=["event_ts"]).copy()
    out["event_date"] = out["event_ts"].dt.date

    out["user_id"] = pd.to_numeric(out["user_id"], errors="coerce")
    out["content_id"] = pd.to_numeric(out["content_id"], errors="coerce")
    out = out.dropna(subset=["user_id", "content_id"]).copy()
    out["user_id"] = out["user_id"].astype(int)
    out["content_id"] = out["content_id"].astype(int)

    out["session_id"] = out["session_id"].fillna("unknown_session").astype(str)
    out["event_type"] = out["event_type"].fillna("view").astype(str)
    out["dwell_seconds"] = pd.to_numeric(out.get("dwell_seconds", 0), errors="coerce").fillna(0).astype(int)
    out["dwell_seconds"] = out["dwell_seconds"].clip(lower=0)
    out["device_type"] = out.get("device_type", "mobile").fillna("mobile").astype(str)
    out["referrer"] = out.get("referrer", "unknown").fillna("unknown").astype(str)
    out["metadata"] = out.get("metadata", "{}").map(normalize_metadata)

    return out[
        [
            "event_id",
            "event_ts",
            "event_date",
            "user_id",
            "content_id",
            "session_id",
            "event_type",
            "dwell_seconds",
            "device_type",
            "referrer",
            "metadata",
        ]
    ].drop_duplicates(subset=["event_id"])


def chunked(rows: list[dict], size: int) -> Iterable[list[dict]]:
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


def ensure_partitions(engine: Engine, event_dates: pd.Series, logger: logging.Logger) -> None:
    if event_dates.empty:
        return
    months = sorted({d.replace(day=1) for d in pd.to_datetime(event_dates).dt.date})
    with engine.begin() as conn:
        for start in months:
            end = (pd.Timestamp(start) + pd.offsets.MonthBegin(1)).date()
            table_name = f"fact_events_{start.strftime('%Y_%m')}"
            sql = f"""
            CREATE TABLE IF NOT EXISTS analytics.{table_name}
            PARTITION OF analytics.fact_events
            FOR VALUES FROM ('{start}') TO ('{end}');
            """
            conn.execute(text(sql))
            logger.info("Ensured partition analytics.%s", table_name)


def upsert_dim_users(engine: Engine, users: pd.DataFrame, logger: logging.Logger) -> None:
    sql = text(
        """
        INSERT INTO analytics.dim_users
            (user_id, signup_at, age_group, gender, region, acquisition_channel)
        VALUES
            (:user_id, :signup_at, :age_group, :gender, :region, :acquisition_channel)
        ON CONFLICT (user_id) DO UPDATE SET
            signup_at = EXCLUDED.signup_at,
            age_group = EXCLUDED.age_group,
            gender = EXCLUDED.gender,
            region = EXCLUDED.region,
            acquisition_channel = EXCLUDED.acquisition_channel,
            updated_at = NOW();
        """
    )
    rows = users.to_dict("records")
    with engine.begin() as conn:
        for batch in chunked(rows, 1000):
            conn.execute(sql, batch)
    logger.info("Upserted dim_users: %d rows", len(rows))


def upsert_dim_contents(engine: Engine, contents: pd.DataFrame, logger: logging.Logger) -> None:
    sql = text(
        """
        INSERT INTO analytics.dim_contents
            (content_id, content_title, content_category, content_type, author_id, publish_at)
        VALUES
            (:content_id, :content_title, :content_category, :content_type, :author_id, :publish_at)
        ON CONFLICT (content_id) DO UPDATE SET
            content_title = EXCLUDED.content_title,
            content_category = EXCLUDED.content_category,
            content_type = EXCLUDED.content_type,
            author_id = EXCLUDED.author_id,
            publish_at = EXCLUDED.publish_at,
            updated_at = NOW();
        """
    )
    rows = contents.to_dict("records")
    with engine.begin() as conn:
        for batch in chunked(rows, 1000):
            conn.execute(sql, batch)
    logger.info("Upserted dim_contents: %d rows", len(rows))


def ensure_event_types(engine: Engine, events: pd.DataFrame, logger: logging.Logger) -> None:
    group_map = {"view": "consumption", "click": "exposure", "like": "engagement", "share": "engagement"}
    rows = [
        {
            "event_type_code": et,
            "event_group": group_map.get(et, "other"),
            "description": f"Auto-registered event type: {et}",
        }
        for et in sorted(events["event_type"].dropna().unique())
    ]
    sql = text(
        """
        INSERT INTO analytics.dim_event_types (event_type_code, event_group, description)
        VALUES (:event_type_code, :event_group, :description)
        ON CONFLICT (event_type_code) DO NOTHING;
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, rows)
    logger.info("Ensured dim_event_types: %d values", len(rows))


def fetch_maps(engine: Engine) -> tuple[dict[int, int], dict[int, int], dict[str, int]]:
    with engine.begin() as conn:
        user_rows = conn.execute(text("SELECT user_id, user_sk FROM analytics.dim_users")).fetchall()
        content_rows = conn.execute(text("SELECT content_id, content_sk FROM analytics.dim_contents")).fetchall()
        type_rows = conn.execute(text("SELECT event_type_code, event_type_sk FROM analytics.dim_event_types")).fetchall()

    user_map = {int(uid): int(sk) for uid, sk in user_rows}
    content_map = {int(cid): int(sk) for cid, sk in content_rows}
    type_map = {str(code): int(sk) for code, sk in type_rows}
    return user_map, content_map, type_map


def add_sks(
    events: pd.DataFrame,
    user_map: dict[int, int],
    content_map: dict[int, int],
    type_map: dict[str, int],
) -> pd.DataFrame:
    out = events.copy()
    out["user_sk"] = out["user_id"].map(user_map)
    out["content_sk"] = out["content_id"].map(content_map)
    out["event_type_sk"] = out["event_type"].map(type_map)
    out = out.dropna(subset=["user_sk", "content_sk", "event_type_sk"]).copy()
    out["user_sk"] = out["user_sk"].astype(int)
    out["content_sk"] = out["content_sk"].astype(int)
    out["event_type_sk"] = out["event_type_sk"].astype(int)
    return out


def insert_fact_events(engine: Engine, events: pd.DataFrame, logger: logging.Logger) -> int:
    sql = text(
        """
        INSERT INTO analytics.fact_events
            (event_id, event_ts, event_date, user_id, content_id, user_sk, content_sk, event_type_sk, session_id, dwell_seconds, device_type, referrer, metadata)
        VALUES
            (:event_id, :event_ts, :event_date, :user_id, :content_id, :user_sk, :content_sk, :event_type_sk, :session_id, :dwell_seconds, :device_type, :referrer, CAST(:metadata AS JSONB))
        ON CONFLICT (event_id) DO NOTHING;
        """
    )
    rows = events.to_dict("records")
    inserted = 0
    with engine.begin() as conn:
        for batch in chunked(rows, 5000):
            result = conn.execute(sql, batch)
            inserted += max(result.rowcount, 0)
    logger.info("Inserted fact_events: %d rows (attempted=%d)", inserted, len(rows))
    return inserted


def run_etl(raw_dir: Path, logger: logging.Logger) -> None:
    engine = get_engine()

    users_raw, contents_raw, events_raw = read_csvs(raw_dir, logger)
    users = preprocess_users(users_raw)
    contents = preprocess_contents(contents_raw)
    events = preprocess_events(events_raw)
    logger.info("Preprocessed rows users=%d, contents=%d, events=%d", len(users), len(contents), len(events))

    upsert_dim_users(engine, users, logger)
    upsert_dim_contents(engine, contents, logger)
    ensure_event_types(engine, events, logger)
    ensure_partitions(engine, events["event_date"], logger)

    user_map, content_map, type_map = fetch_maps(engine)
    events_with_sks = add_sks(events, user_map, content_map, type_map)
    logger.info("Events after FK/SK mapping=%d", len(events_with_sks))

    inserted = insert_fact_events(engine, events_with_sks, logger)
    logger.info("ETL completed successfully. inserted_fact_events=%d", inserted)


def main() -> None:
    parser = argparse.ArgumentParser(description="CSV -> PostgreSQL ETL loader")
    parser.add_argument(
        "--raw-dir",
        type=str,
        default=str(DEFAULT_RAW_DIR),
        help="CSV directory path (users.csv, contents.csv/content.csv, events.csv)",
    )
    args = parser.parse_args()

    logger = get_logger()
    logger.info("Starting CSV ETL")
    run_etl(Path(args.raw_dir), logger)


if __name__ == "__main__":
    main()
