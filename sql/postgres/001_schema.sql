CREATE TABLE IF NOT EXISTS daily_metrics (
    metric_date date NOT NULL,
    metric_name text NOT NULL,
    dimension text NOT NULL DEFAULT '',
    value double precision,
    computed_at timestamptz NOT NULL,
    PRIMARY KEY (metric_date, metric_name, dimension)
);

CREATE TABLE IF NOT EXISTS aggregation_runs (
    metric_date date PRIMARY KEY,
    processed_events bigint NOT NULL,
    computed_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS export_jobs (
    metric_date date PRIMARY KEY REFERENCES aggregation_runs(metric_date),
    requested_at timestamptz NOT NULL,
    exported_at timestamptz,
    attempts integer NOT NULL DEFAULT 0,
    last_error text
);
