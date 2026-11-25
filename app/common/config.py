import os

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka-1:9092,kafka-2:9092,kafka-3:9092")
TOPIC = "movie-events"
SUBJECT = f"{TOPIC}-value"
SCHEMA_VERSION = 1
REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
CH_URL = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123")
CH_USER = os.getenv("CLICKHOUSE_USER", "cinema")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "cinema_pass")
PG_DSN = os.getenv("POSTGRES_DSN", "postgresql://cinema:cinema_pass@postgres:5432/cinema")
S3_URL = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin123")
S3_BUCKET = "movie-analytics"
