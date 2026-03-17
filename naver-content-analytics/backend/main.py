import os
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel
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


class EventIn(BaseModel):
    event_ts: str
    user_id: int
    session_id: str
    content_id: int
    event_type: str
    dwell_seconds: int = 0
    device_type: str = "mobile"
    referrer: str = "naver_home"
    metadata: str = "{}"


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/events")
def ingest_event(event: EventIn):
    sql = text(
        """
        INSERT INTO analytics.fact_user_events
        (event_ts, user_id, session_id, content_id, event_type, dwell_seconds, device_type, referrer, metadata)
        VALUES
        (:event_ts, :user_id, :session_id, :content_id, :event_type, :dwell_seconds, :device_type, :referrer, CAST(:metadata AS JSONB))
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, event.model_dump())
    return {"inserted": 1}


@app.get("/insights/dau")
def dau():
    sql = text(
        """
        SELECT event_date, COUNT(DISTINCT user_id) AS dau
        FROM analytics.fact_user_events
        GROUP BY event_date
        ORDER BY event_date DESC
        LIMIT 30
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(sql).mappings().all()
    return {"rows": [dict(r) for r in rows]}
