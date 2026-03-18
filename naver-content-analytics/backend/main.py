import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel, Field
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "analytics")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
app = FastAPI(title="Content Analytics API")

EVENT_GROUP_MAP = {
    "impression": "exposure",
    "click": "exposure",
    "view": "consumption",
    "view_start": "consumption",
    "view_end": "consumption",
    "like": "engagement",
    "comment": "engagement",
    "share": "engagement",
    "bookmark": "engagement",
}


class EventIn(BaseModel):
    event_ts: str
    user_id: int
    session_id: str
    content_id: int
    event_type: str = Field(default="view")
    dwell_seconds: int = 0
    device_type: str = "mobile"
    referrer: str = "naver_home"
    metadata: dict[str, Any] = Field(default_factory=dict)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def parse_event_ts(event_ts: str) -> datetime:
    normalized = event_ts.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized)


@app.post("/events")
def ingest_event(event: EventIn) -> dict[str, int]:
    parsed_ts = parse_event_ts(event.event_ts)
    event_group = EVENT_GROUP_MAP.get(event.event_type, "other")

    with engine.begin() as conn:
        user_sk = conn.execute(
            text(
                """
                INSERT INTO analytics.dim_users (user_id, signup_at, age_group, gender, region, acquisition_channel)
                VALUES (:user_id, NOW(), 'unknown', 'U', 'unknown', 'unknown')
                ON CONFLICT (user_id) DO UPDATE SET updated_at = NOW()
                RETURNING user_sk;
                """
            ),
            {"user_id": event.user_id},
        ).scalar_one()

        content_sk = conn.execute(
            text(
                """
                INSERT INTO analytics.dim_contents
                    (content_id, content_title, content_category, content_type, author_id, publish_at)
                VALUES
                    (:content_id, 'untitled', 'unknown', 'article', 0, NOW())
                ON CONFLICT (content_id) DO UPDATE SET updated_at = NOW()
                RETURNING content_sk;
                """
            ),
            {"content_id": event.content_id},
        ).scalar_one()

        conn.execute(
            text(
                """
                INSERT INTO analytics.dim_event_types (event_type_code, event_group, description)
                VALUES (:event_type_code, :event_group, :description)
                ON CONFLICT (event_type_code) DO NOTHING;
                """
            ),
            {
                "event_type_code": event.event_type,
                "event_group": event_group,
                "description": f"Auto-registered event type: {event.event_type}",
            },
        )

        event_type_sk = conn.execute(
            text(
                """
                SELECT event_type_sk
                FROM analytics.dim_event_types
                WHERE event_type_code = :event_type_code;
                """
            ),
            {"event_type_code": event.event_type},
        ).scalar_one()

        conn.execute(
            text(
                """
                INSERT INTO analytics.fact_events
                    (event_id, event_ts, event_date, user_id, content_id, user_sk, content_sk, event_type_sk,
                     session_id, dwell_seconds, device_type, referrer, metadata)
                VALUES
                    (:event_id, :event_ts, :event_date, :user_id, :content_id, :user_sk, :content_sk, :event_type_sk,
                     :session_id, :dwell_seconds, :device_type, :referrer, CAST(:metadata AS JSONB));
                """
            ),
            {
                "event_id": str(uuid.uuid4()),
                "event_ts": parsed_ts,
                "event_date": parsed_ts.date(),
                "user_id": event.user_id,
                "content_id": event.content_id,
                "user_sk": user_sk,
                "content_sk": content_sk,
                "event_type_sk": event_type_sk,
                "session_id": event.session_id,
                "dwell_seconds": max(event.dwell_seconds, 0),
                "device_type": event.device_type,
                "referrer": event.referrer,
                "metadata": json.dumps(event.metadata, ensure_ascii=True),
            },
        )

    return {"inserted": 1}


@app.get("/insights/dau")
def dau() -> dict[str, list[dict[str, Any]]]:
    sql = text(
        """
        SELECT event_date, COUNT(DISTINCT user_id) AS dau
        FROM analytics.fact_events
        GROUP BY event_date
        ORDER BY event_date DESC
        LIMIT 30;
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(sql).mappings().all()
    return {"rows": [dict(r) for r in rows]}
