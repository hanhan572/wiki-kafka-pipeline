-- Raw events: one row per Wikipedia edit (columns match the Kafka message 1:1)
CREATE TABLE IF NOT EXISTS edits (
    event_id     TEXT PRIMARY KEY,          -- Wikimedia meta.id (UUID) -> idempotent inserts
    rc_id        BIGINT,                    -- recentchange id on that wiki
    event_time   TIMESTAMPTZ NOT NULL,      -- when the edit happened (meta.dt)
    wiki         TEXT NOT NULL,             -- category: enwiki, dewiki, ...
    domain       TEXT,
    event_type   TEXT,                      -- edit | new
    namespace    INTEGER,
    title        TEXT,
    user_name    TEXT,
    is_bot       BOOLEAN,
    is_minor     BOOLEAN,
    length_old   INTEGER,
    length_new   INTEGER,
    byte_delta   INTEGER,                   -- numeric: length_new - length_old
    comment      TEXT,
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS edits_time_idx ON edits (event_time DESC);
CREATE INDEX IF NOT EXISTS edits_wiki_time_idx ON edits (wiki, event_time DESC);

-- Derived result 1 (stream processor): tumbling 1-minute window per wiki
CREATE TABLE IF NOT EXISTS edit_stats_minute (
    wiki           TEXT NOT NULL,
    minute_start   TIMESTAMPTZ NOT NULL,
    edits          INTEGER NOT NULL,
    bot_edits      INTEGER NOT NULL,
    human_edits    INTEGER NOT NULL,
    new_pages      INTEGER NOT NULL,
    bytes_added    BIGINT NOT NULL,
    bytes_removed  BIGINT NOT NULL,
    unique_users   INTEGER NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (wiki, minute_start)
);
CREATE INDEX IF NOT EXISTS stats_minute_idx ON edit_stats_minute (minute_start DESC);

-- Derived result 2 (stream processor -> topic wikipedia.edit-alerts -> sink): rule-based alerts
CREATE TABLE IF NOT EXISTS alerts (
    alert_id    TEXT PRIMARY KEY,
    alert_time  TIMESTAMPTZ NOT NULL,
    rule        TEXT NOT NULL,              -- MASS_DELETION | EDIT_BURST | HIGH_RATE_USER
    severity    TEXT,
    wiki        TEXT,
    title       TEXT,
    user_name   TEXT,
    value       DOUBLE PRECISION,
    detail      TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS alerts_time_idx ON alerts (alert_time DESC);

-- Derived result 3 (anomaly detector, sequential model): forecast + anomaly score per 10 s bucket
CREATE TABLE IF NOT EXISTS rate_scores (
    series       TEXT NOT NULL,             -- 'ALL' or a wiki code
    bucket_time  TIMESTAMPTZ NOT NULL,      -- start of the 10-second bucket
    observed     INTEGER NOT NULL,
    forecast     DOUBLE PRECISION,          -- one-step-ahead forecast from the online AR model
    z_ar         DOUBLE PRECISION,          -- standardised residual of the AR forecast
    z_ewma       DOUBLE PRECISION,          -- baseline detector for comparison
    is_anomaly   BOOLEAN NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (series, bucket_time)
);
CREATE INDEX IF NOT EXISTS rate_scores_idx ON rate_scores (series, bucket_time DESC);
