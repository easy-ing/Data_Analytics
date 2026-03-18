import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "analytics")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

sql = """
INSERT INTO analytics.mart_content_daily (
    stat_date,
    content_sk,
    impressions,
    clicks,
    unique_users,
    view_completions,
    likes,
    comments,
    shares,
    bookmarks,
    avg_dwell_seconds,
    ctr,
    engagement_rate
)
SELECT
    f.event_date AS stat_date,
    f.content_sk,
    COUNT(*) FILTER (WHERE et.event_type_code = 'impression') AS impressions,
    COUNT(*) FILTER (WHERE et.event_type_code = 'click') AS clicks,
    COUNT(DISTINCT f.user_id) AS unique_users,
    COUNT(*) FILTER (WHERE et.event_type_code = 'view_end') AS view_completions,
    COUNT(*) FILTER (WHERE et.event_type_code = 'like') AS likes,
    COUNT(*) FILTER (WHERE et.event_type_code = 'comment') AS comments,
    COUNT(*) FILTER (WHERE et.event_type_code = 'share') AS shares,
    COUNT(*) FILTER (WHERE et.event_type_code = 'bookmark') AS bookmarks,
    ROUND(AVG(NULLIF(f.dwell_seconds, 0))::NUMERIC, 2) AS avg_dwell_seconds,
    ROUND(
        COUNT(*) FILTER (WHERE et.event_type_code = 'click')::NUMERIC /
        NULLIF(COUNT(*) FILTER (WHERE et.event_type_code = 'impression'), 0),
        4
    ) AS ctr,
    ROUND(
        (
            COUNT(*) FILTER (WHERE et.event_type_code IN ('like', 'comment', 'share', 'bookmark'))
        )::NUMERIC /
        NULLIF(COUNT(*) FILTER (WHERE et.event_type_code = 'click'), 0),
        4
    ) AS engagement_rate
FROM analytics.fact_events f
JOIN analytics.dim_event_types et
  ON f.event_type_sk = et.event_type_sk
GROUP BY f.event_date, f.content_sk
ON CONFLICT (stat_date, content_sk)
DO UPDATE SET
    impressions = EXCLUDED.impressions,
    clicks = EXCLUDED.clicks,
    unique_users = EXCLUDED.unique_users,
    view_completions = EXCLUDED.view_completions,
    likes = EXCLUDED.likes,
    comments = EXCLUDED.comments,
    shares = EXCLUDED.shares,
    bookmarks = EXCLUDED.bookmarks,
    avg_dwell_seconds = EXCLUDED.avg_dwell_seconds,
    ctr = EXCLUDED.ctr,
    engagement_rate = EXCLUDED.engagement_rate,
    updated_at = NOW();
"""

with engine.begin() as conn:
    conn.execute(text(sql))

print("mart_content_daily built")
