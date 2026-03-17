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
    content_id,
    impressions,
    clicks,
    unique_viewers,
    avg_dwell_seconds,
    ctr,
    engagement_rate
)
SELECT
    e.event_date,
    e.content_id,
    COUNT(*) FILTER (WHERE e.event_type = 'impression') AS impressions,
    COUNT(*) FILTER (WHERE e.event_type = 'click') AS clicks,
    COUNT(DISTINCT CASE WHEN e.event_type IN ('click', 'view_start', 'view_end') THEN e.user_id END) AS unique_viewers,
    ROUND(AVG(NULLIF(e.dwell_seconds, 0))::NUMERIC, 2) AS avg_dwell_seconds,
    ROUND(
        COUNT(*) FILTER (WHERE e.event_type = 'click')::NUMERIC /
        NULLIF(COUNT(*) FILTER (WHERE e.event_type = 'impression'), 0),
        4
    ) AS ctr,
    ROUND(
        COUNT(*) FILTER (WHERE e.event_type IN ('like', 'comment', 'share', 'bookmark'))::NUMERIC /
        NULLIF(COUNT(*) FILTER (WHERE e.event_type = 'click'), 0),
        4
    ) AS engagement_rate
FROM analytics.fact_user_events e
GROUP BY e.event_date, e.content_id
ON CONFLICT (stat_date, content_id)
DO UPDATE SET
    impressions = EXCLUDED.impressions,
    clicks = EXCLUDED.clicks,
    unique_viewers = EXCLUDED.unique_viewers,
    avg_dwell_seconds = EXCLUDED.avg_dwell_seconds,
    ctr = EXCLUDED.ctr,
    engagement_rate = EXCLUDED.engagement_rate;
"""

with engine.begin() as conn:
    conn.execute(text(sql))

print("mart_content_daily built")
