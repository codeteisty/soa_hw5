CREATE DATABASE IF NOT EXISTS cinema;

CREATE TABLE IF NOT EXISTS cinema.events
(
    event_id UUID,
    user_id String,
    movie_id String,
    event_type Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3, 'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id String,
    progress_seconds Int32,
    ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (toDate(timestamp), user_id, event_id);

CREATE TABLE IF NOT EXISTS cinema.kafka_events
(
    event_id UUID,
    user_id String,
    movie_id String,
    event_type Enum8('VIEW_STARTED' = 1, 'VIEW_FINISHED' = 2, 'VIEW_PAUSED' = 3, 'VIEW_RESUMED' = 4, 'LIKED' = 5, 'SEARCHED' = 6),
    timestamp DateTime64(3, 'UTC'),
    device_type Enum8('MOBILE' = 1, 'DESKTOP' = 2, 'TV' = 3, 'TABLET' = 4),
    session_id String,
    progress_seconds Int32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-1:9092,kafka-2:9092,kafka-3:9092',
    kafka_topic_list = 'movie-events',
    kafka_group_name = 'clickhouse-movie-events-v1',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 3,
    kafka_thread_per_consumer = 1,
    kafka_flush_interval_ms = 1000,
    kafka_skip_broken_messages = 0;

CREATE MATERIALIZED VIEW IF NOT EXISTS cinema.consume_events
TO cinema.events AS
SELECT event_id, user_id, movie_id, event_type, timestamp, device_type,
       session_id, progress_seconds, now64(3) AS ingested_at
FROM cinema.kafka_events;

CREATE TABLE IF NOT EXISTS cinema.metric_snapshots
(
    metric_date Date,
    metric_name LowCardinality(String),
    dimension String,
    value Nullable(Float64),
    computed_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(metric_date)
ORDER BY (metric_date, version, metric_name, dimension);

CREATE TABLE IF NOT EXISTS cinema.aggregation_runs
(
    metric_date Date,
    version UInt64,
    processed_events UInt64,
    computed_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (metric_date, version);

-- A completed version switches the whole date, including its movie ranking.
CREATE VIEW IF NOT EXISTS cinema.daily_metrics AS
SELECT metric_date, metric_name, dimension, value, computed_at
FROM cinema.metric_snapshots FINAL
WHERE (metric_date, version) IN
    (SELECT metric_date, max(version) FROM cinema.aggregation_runs GROUP BY metric_date);

CREATE USER IF NOT EXISTS grafana IDENTIFIED WITH sha256_password BY 'grafana_pass';
GRANT SELECT ON cinema.daily_metrics TO grafana;
GRANT SELECT ON cinema.metric_snapshots TO grafana;
GRANT SELECT ON cinema.aggregation_runs TO grafana;
ALTER USER grafana SETTINGS readonly = 1, max_execution_time = 30 CHANGEABLE_IN_READONLY;
