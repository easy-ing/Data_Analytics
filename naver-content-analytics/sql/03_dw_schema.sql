-- =============================================
-- PostgreSQL DW Schema for Content Analytics
-- Star schema: dimensions + fact + aggregate mart
-- =============================================

CREATE SCHEMA IF NOT EXISTS analytics;

-- -----------------------------
-- Dimension: users
-- -----------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_users (
    user_sk               BIGSERIAL PRIMARY KEY,
    user_id               BIGINT NOT NULL UNIQUE,
    signup_at             TIMESTAMP NOT NULL,
    age_group             VARCHAR(20),
    gender                VARCHAR(10),
    region                VARCHAR(100),
    acquisition_channel   VARCHAR(50),
    is_active             BOOLEAN NOT NULL DEFAULT TRUE,
    created_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMP NOT NULL DEFAULT NOW()
);

-- -----------------------------
-- Dimension: contents
-- -----------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_contents (
    content_sk            BIGSERIAL PRIMARY KEY,
    content_id            BIGINT NOT NULL UNIQUE,
    content_title         TEXT NOT NULL,
    content_category      VARCHAR(100) NOT NULL,
    content_type          VARCHAR(30) NOT NULL,      -- article, shortform, video
    author_id             BIGINT,
    publish_at            TIMESTAMP NOT NULL,
    is_published          BOOLEAN NOT NULL DEFAULT TRUE,
    created_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMP NOT NULL DEFAULT NOW()
);

-- -----------------------------
-- Dimension: event type
-- -----------------------------
CREATE TABLE IF NOT EXISTS analytics.dim_event_types (
    event_type_sk         SMALLSERIAL PRIMARY KEY,
    event_type_code       VARCHAR(30) NOT NULL UNIQUE,   -- impression, click, view_start, view_end, like, comment, share, bookmark
    event_group           VARCHAR(30) NOT NULL,          -- exposure, consumption, engagement
    description           TEXT,
    created_at            TIMESTAMP NOT NULL DEFAULT NOW()
);

-- -----------------------------
-- Fact: events (partitioned)
-- -----------------------------
CREATE TABLE IF NOT EXISTS analytics.fact_events (
    event_sk              BIGSERIAL,
    event_id              UUID NOT NULL,
    event_ts              TIMESTAMP NOT NULL,
    event_date            DATE NOT NULL,

    -- business keys (optional traceability)
    user_id               BIGINT NOT NULL,
    content_id            BIGINT NOT NULL,

    -- dimensional foreign keys
    user_sk               BIGINT NOT NULL REFERENCES analytics.dim_users(user_sk),
    content_sk            BIGINT NOT NULL REFERENCES analytics.dim_contents(content_sk),
    event_type_sk         SMALLINT NOT NULL REFERENCES analytics.dim_event_types(event_type_sk),

    -- session / behavior attributes
    session_id            VARCHAR(100) NOT NULL,
    dwell_seconds         INTEGER NOT NULL DEFAULT 0,
    scroll_depth          NUMERIC(5,2),
    device_type           VARCHAR(30),
    referrer              VARCHAR(100),

    -- flexible payload
    metadata              JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at            TIMESTAMP NOT NULL DEFAULT NOW(),

    PRIMARY KEY (event_date, event_sk),
    UNIQUE (event_id)
) PARTITION BY RANGE (event_date);

-- Sample partition (monthly). Add scheduler for rolling partitions in production.
CREATE TABLE IF NOT EXISTS analytics.fact_events_2026_03
PARTITION OF analytics.fact_events
FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');

-- -----------------------------
-- Aggregate mart: daily content metrics
-- -----------------------------
CREATE TABLE IF NOT EXISTS analytics.mart_content_daily (
    stat_date             DATE NOT NULL,
    content_sk            BIGINT NOT NULL REFERENCES analytics.dim_contents(content_sk),

    impressions           INTEGER NOT NULL DEFAULT 0,
    clicks                INTEGER NOT NULL DEFAULT 0,
    unique_users          INTEGER NOT NULL DEFAULT 0,
    view_completions      INTEGER NOT NULL DEFAULT 0,
    likes                 INTEGER NOT NULL DEFAULT 0,
    comments              INTEGER NOT NULL DEFAULT 0,
    shares                INTEGER NOT NULL DEFAULT 0,
    bookmarks             INTEGER NOT NULL DEFAULT 0,

    avg_dwell_seconds     NUMERIC(10,2) NOT NULL DEFAULT 0,
    ctr                   NUMERIC(10,4) NOT NULL DEFAULT 0,
    engagement_rate       NUMERIC(10,4) NOT NULL DEFAULT 0,

    created_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMP NOT NULL DEFAULT NOW(),

    PRIMARY KEY (stat_date, content_sk)
);

-- -----------------------------
-- Indexes for analytics workload
-- -----------------------------
CREATE INDEX IF NOT EXISTS idx_fact_events_user_date
    ON analytics.fact_events (user_sk, event_date);

CREATE INDEX IF NOT EXISTS idx_fact_events_content_date
    ON analytics.fact_events (content_sk, event_date);

CREATE INDEX IF NOT EXISTS idx_fact_events_type_date
    ON analytics.fact_events (event_type_sk, event_date);

CREATE INDEX IF NOT EXISTS idx_fact_events_session
    ON analytics.fact_events (session_id);

CREATE INDEX IF NOT EXISTS idx_fact_events_metadata_gin
    ON analytics.fact_events USING GIN (metadata);

-- BRIN index is efficient for very large append-only event tables.
CREATE INDEX IF NOT EXISTS idx_fact_events_ts_brin
    ON analytics.fact_events USING BRIN (event_ts);

-- -----------------------------
-- Seed basic event types
-- -----------------------------
INSERT INTO analytics.dim_event_types (event_type_code, event_group, description)
VALUES
    ('impression', 'exposure', 'Content impression occurred'),
    ('click', 'exposure', 'User clicked content'),
    ('view_start', 'consumption', 'Playback/reading started'),
    ('view_end', 'consumption', 'Playback/reading finished'),
    ('like', 'engagement', 'User liked content'),
    ('comment', 'engagement', 'User commented on content'),
    ('share', 'engagement', 'User shared content'),
    ('bookmark', 'engagement', 'User bookmarked content')
ON CONFLICT (event_type_code) DO NOTHING;
