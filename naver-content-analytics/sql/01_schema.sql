CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.dim_users (
    user_id BIGINT PRIMARY KEY,
    signup_at TIMESTAMP NOT NULL,
    age_group TEXT,
    region TEXT,
    acquisition_channel TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics.dim_content (
    content_id BIGINT PRIMARY KEY,
    content_title TEXT NOT NULL,
    content_category TEXT NOT NULL,
    content_type TEXT NOT NULL,
    publish_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics.fact_user_events (
    event_id BIGSERIAL PRIMARY KEY,
    event_ts TIMESTAMP NOT NULL,
    event_date DATE GENERATED ALWAYS AS (event_ts::DATE) STORED,
    user_id BIGINT NOT NULL REFERENCES analytics.dim_users(user_id),
    session_id TEXT NOT NULL,
    content_id BIGINT NOT NULL REFERENCES analytics.dim_content(content_id),
    event_type TEXT NOT NULL,
    dwell_seconds INT DEFAULT 0,
    device_type TEXT,
    referrer TEXT,
    metadata JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS analytics.mart_content_daily (
    stat_date DATE NOT NULL,
    content_id BIGINT NOT NULL REFERENCES analytics.dim_content(content_id),
    impressions INT NOT NULL DEFAULT 0,
    clicks INT NOT NULL DEFAULT 0,
    unique_viewers INT NOT NULL DEFAULT 0,
    avg_dwell_seconds NUMERIC(10,2) NOT NULL DEFAULT 0,
    ctr NUMERIC(10,4) NOT NULL DEFAULT 0,
    engagement_rate NUMERIC(10,4) NOT NULL DEFAULT 0,
    PRIMARY KEY (stat_date, content_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_date ON analytics.fact_user_events(event_date);
CREATE INDEX IF NOT EXISTS idx_fact_type_date ON analytics.fact_user_events(event_type, event_date);
CREATE INDEX IF NOT EXISTS idx_fact_content_date ON analytics.fact_user_events(content_id, event_date);
