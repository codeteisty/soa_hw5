import json
import logging
from pathlib import Path

import httpx
import psycopg
from botocore.exceptions import ClientError
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from app.common.clients import ClickHouse, s3_client
from app.common.config import KAFKA_BROKERS, PG_DSN, REGISTRY_URL, S3_BUCKET, SCHEMA_VERSION, SUBJECT, TOPIC
from app.common.retry import retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
ROOT = Path(__file__).resolve().parent.parent


def initialize():
    admin = AdminClient({"bootstrap.servers": KAFKA_BROKERS})
    topic = NewTopic(TOPIC, num_partitions=3, replication_factor=3,
                     config={"min.insync.replicas": "2", "retention.ms": "604800000"})
    for future in admin.create_topics([topic]).values():
        try:
            future.result(30)
        except KafkaException as exc:
            if exc.args[0].name() != "TOPIC_ALREADY_EXISTS":
                raise
    metadata = admin.list_topics(TOPIC, timeout=10).topics[TOPIC]
    assert len(metadata.partitions) >= 3
    assert all(len(p.replicas) >= 3 for p in metadata.partitions.values())

    schema = (ROOT / "schemas/movie-event-v1.avsc").read_text()
    with httpx.Client(base_url=REGISTRY_URL, timeout=20) as client:
        response = client.put(f"/config/{SUBJECT}", json={"compatibility": "BACKWARD_TRANSITIVE"})
        response.raise_for_status()
        body = {"schemaType": "AVRO", "schema": schema}
        response = client.post(f"/subjects/{SUBJECT}/versions", json=body)
        response.raise_for_status()
        registered = client.post(f"/subjects/{SUBJECT}", json=body)
        registered.raise_for_status()
        info = registered.json()
        if info["version"] != SCHEMA_VERSION:
            raise RuntimeError(f"Expected schema version {SCHEMA_VERSION}, got {info['version']}")
        logging.info("schema registered subject=%s version=%s id=%s", SUBJECT, info["version"], info["id"])

    ch = ClickHouse()
    for statement in (ROOT / "sql/clickhouse/001_schema.sql").read_text().split(";"):
        if statement.strip():
            ch.execute(statement)
    with psycopg.connect(PG_DSN, connect_timeout=5) as conn:
        conn.execute((ROOT / "sql/postgres/001_schema.sql").read_text())

    s3 = s3_client()
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
    except ClientError as exc:
        if exc.response["Error"]["Code"] not in ("404", "NoSuchBucket"):
            raise
        s3.create_bucket(Bucket=S3_BUCKET)
    Path("/tmp/initialized").touch()
    logging.info("topic, schema, migrations and S3 bucket are ready")


if __name__ == "__main__":
    retry(initialize, attempts=12, delay=1)
