-- PostgreSQL schema for user behavior log-based content analytics system

CREATE SCHEMA IF NOT EXISTS analytics;

-- Dimension: users
CREATE TABLE IF NOT EXISTS analytics.dim_users (
    user_id            BIGINT PRIMARY KEY,
    signup_at          TIMESTAMP NOT NULL,
    age_group          TEXT,
    gender             TEXT,
    region             TEXT,
    acquisition_channel TEXT,
    created_at         TIMESTAMP DEFAULT NOW()
);

-- Dimension: content
CREATE TABLE IF NOT EXISTS analytics.dim_content (
    content_id         BIGINT PRIMARY KEY,
    content_title      TEXT NOT NULL,
    content_category   TEXT NOT NULL,
    content_type       TEXT NOT NULL, -- article, shortform, video
    publish_at         TIMESTAMP NOT NULL,
    author_id          BIGINT,
    created_at         TIMESTAMP DEFAULT NOW()
);

-- Event fact table (raw-ish but typed)
CREATE TABLE IF NOT EXISTS analytics.fact_user_events (
    event_id           BIGSERIAL PRIMARY KEY,
    event_ts           TIMESTAMP NOT NULL,
    event_date         DATE GENERATED ALWAYS AS (event_ts::DATE) STORED,
    user_id            BIGINT NOT NULL REFERENCES analytics.dim_users(user_id),
    session_id         TEXT NOT NULL,
    content_id         BIGINT REFERENCES analytics.dim_content(content_id),
    event_type         TEXT NOT NULL, -- impression, click, like, comment, share, bookmark, view_start, view_end
    dwell_seconds      INTEGER DEFAULT 0,
    device_type        TEXT,
    referrer           TEXT,
    metadata           JSONB DEFAULT '{}'::jsonb
);

-- Daily aggregate mart for fast KPI queries
CREATE TABLE IF NOT EXISTS analytics.mart_content_daily (
    stat_date          DATE NOT NULL,
    content_id         BIGINT NOT NULL REFERENCES analytics.dim_content(content_id),
    impressions        INTEGER NOT NULL DEFAULT 0,
    clicks             INTEGER NOT NULL DEFAULT 0,
    unique_viewers     INTEGER NOT NULL DEFAULT 0,
    likes              INTEGER NOT NULL DEFAULT 0,
    comments           INTEGER NOT NULL DEFAULT 0,
    shares             INTEGER NOT NULL DEFAULT 0,
    avg_dwell_seconds  NUMERIC(10,2) NOT NULL DEFAULT 0,
    ctr                NUMERIC(10,4) NOT NULL DEFAULT 0,
    engagement_rate    NUMERIC(10,4) NOT NULL DEFAULT 0,
    created_at         TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (stat_date, content_id)
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_fact_events_date ON analytics.fact_user_events(event_date);
CREATE INDEX IF NOT EXISTS idx_fact_events_user ON analytics.fact_user_events(user_id);
CREATE INDEX IF NOT EXISTS idx_fact_events_content ON analytics.fact_user_events(content_id);
CREATE INDEX IF NOT EXISTS idx_fact_events_type_date ON analytics.fact_user_events(event_type, event_date);

COMMENT ON TABLE analytics.fact_user_events IS 'User behavior event log fact table';
