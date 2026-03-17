import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

CONN_STR = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


def load_csvs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    users = pd.read_csv(RAW_DIR / "users.csv")
    content = pd.read_csv(RAW_DIR / "content.csv")
    events = pd.read_csv(RAW_DIR / "events.csv")

    # Type normalization
    users["signup_at"] = pd.to_datetime(users["signup_at"])
    content["publish_at"] = pd.to_datetime(content["publish_at"])
    events["event_ts"] = pd.to_datetime(events["event_ts"])
    events["dwell_seconds"] = events["dwell_seconds"].fillna(0).astype(int)

    return users, content, events


def ensure_schema(engine):
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics"))


def load_dimensions(engine, users: pd.DataFrame, content: pd.DataFrame):
    users.to_sql(
        name="dim_users",
        con=engine,
        schema="analytics",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=2000,
    )
    content.to_sql(
        name="dim_content",
        con=engine,
        schema="analytics",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=2000,
    )


def load_events(engine, events: pd.DataFrame):
    events.to_sql(
        name="fact_user_events",
        con=engine,
        schema="analytics",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000,
    )


def build_daily_mart(engine):
    sql = """
    INSERT INTO analytics.mart_content_daily (
        stat_date,
        content_id,
        impressions,
        clicks,
        unique_viewers,
        likes,
        comments,
        shares,
        avg_dwell_seconds,
        ctr,
        engagement_rate
    )
    SELECT
        e.event_date AS stat_date,
        e.content_id,
        COUNT(*) FILTER (WHERE e.event_type = 'impression') AS impressions,
        COUNT(*) FILTER (WHERE e.event_type = 'click') AS clicks,
        COUNT(DISTINCT CASE WHEN e.event_type IN ('click', 'view_start', 'view_end') THEN e.user_id END) AS unique_viewers,
        COUNT(*) FILTER (WHERE e.event_type = 'like') AS likes,
        COUNT(*) FILTER (WHERE e.event_type = 'comment') AS comments,
        COUNT(*) FILTER (WHERE e.event_type = 'share') AS shares,
        ROUND(AVG(NULLIF(e.dwell_seconds, 0))::NUMERIC, 2) AS avg_dwell_seconds,
        ROUND(
            COUNT(*) FILTER (WHERE e.event_type = 'click')::NUMERIC
            / NULLIF(COUNT(*) FILTER (WHERE e.event_type = 'impression'), 0),
            4
        ) AS ctr,
        ROUND(
            (COUNT(*) FILTER (WHERE e.event_type IN ('like','comment','share','bookmark'))::NUMERIC)
            / NULLIF(COUNT(*) FILTER (WHERE e.event_type = 'click'), 0),
            4
        ) AS engagement_rate
    FROM analytics.fact_user_events e
    WHERE e.content_id IS NOT NULL
    GROUP BY e.event_date, e.content_id
    ON CONFLICT (stat_date, content_id)
    DO UPDATE SET
        impressions = EXCLUDED.impressions,
        clicks = EXCLUDED.clicks,
        unique_viewers = EXCLUDED.unique_viewers,
        likes = EXCLUDED.likes,
        comments = EXCLUDED.comments,
        shares = EXCLUDED.shares,
        avg_dwell_seconds = EXCLUDED.avg_dwell_seconds,
        ctr = EXCLUDED.ctr,
        engagement_rate = EXCLUDED.engagement_rate;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


def main():
    engine = create_engine(CONN_STR)
    users, content, events = load_csvs()

    ensure_schema(engine)

    # NOTE: schema SQL (01_schema.sql) should be executed before this ETL.
    load_dimensions(engine, users, content)
    load_events(engine, events)
    build_daily_mart(engine)

    print("ETL complete")
    print(f"Loaded users={len(users)}, content={len(content)}, events={len(events)}")


if __name__ == "__main__":
    main()
