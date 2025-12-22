CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.enriched_events
(
    event_id UUID,
    event_time DateTime,
    user_id UUID,
    content_id UUID,
    content_type Nullable(String),
    length_seconds Nullable(UInt32),
    duration_ms Nullable(Int32),
    engagement_seconds Nullable(Float64),
    engagement_pct Nullable(Float64)
)
ENGINE = MergeTree
ORDER BY (event_time, event_id);


