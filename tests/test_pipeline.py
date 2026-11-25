import json
from datetime import datetime, timezone

import httpx
import psycopg
import pytest
from confluent_kafka.admin import AdminClient, ConfigResource, ResourceType

from app.common.clients import ClickHouse, s3_client
from app.common.config import KAFKA_BROKERS, PG_DSN, REGISTRY_URL, S3_BUCKET, SUBJECT, TOPIC
from tests.conftest import AGGREGATION, PRODUCER, event, wait_for


def pg_metrics(day="2024-01-01"):
    with psycopg.connect(PG_DSN) as conn:
        rows = conn.execute("SELECT metric_name, dimension, value FROM daily_metrics WHERE metric_date = %s", (day,)).fetchall()
    return {(name, dimension): value for name, dimension, value in rows}


def test_schema_and_replication():
    response = httpx.get(f"{REGISTRY_URL}/subjects/{SUBJECT}/versions/1")
    response.raise_for_status()
    assert response.json()["version"] == 1
    assert json.loads(response.json()["schema"])["name"] == "MovieEvent"
    admin = AdminClient({"bootstrap.servers": KAFKA_BROKERS})
    partitions = admin.list_topics(TOPIC, timeout=10).topics[TOPIC].partitions
    assert len(partitions) >= 3
    assert all(len(partition.replicas) == 3 for partition in partitions.values())
    resource = ConfigResource(ResourceType.TOPIC, TOPIC)
    config = admin.describe_configs([resource])[resource].result(15)
    assert config["min.insync.replicas"].value == "2"


def test_every_field_survives_avro_and_ingestion(dataset):
    rows = ClickHouse().query("SELECT * FROM cinema.events FINAL WHERE startsWith(user_id, 'fixture-')")
    actual = {row["event_id"]: row for row in rows}
    assert len(actual) == len(dataset)
    for expected in dataset:
        row = actual[expected["event_id"]]
        for key in ("event_id", "user_id", "movie_id", "event_type", "device_type", "session_id", "progress_seconds"):
            assert row[key] == expected[key]
        received = datetime.fromisoformat(row["timestamp"]).replace(tzinfo=timezone.utc)
        assert received == datetime.fromisoformat(expected["timestamp"])


@pytest.mark.parametrize("field,value", [
    ("event_type", "BAD_EVENT"), ("event_id", "not-a-uuid"), ("progress_seconds", -1),
    ("timestamp", "2024-01-01T12:00:00"), ("device_type", "WATCH"), ("movie_id", ""),
    ("progress_seconds", True), ("unexpected", "field"),
])
def test_api_rejects_invalid_events(field, value):
    payload = event("invalid", "VIEW_STARTED")
    payload[field] = value
    response = httpx.post(f"{PRODUCER}/events", json=payload)
    assert response.status_code == 422, response.text


def test_daily_metrics_and_exact_day_retention(metrics):
    values = pg_metrics()
    assert metrics["processed_events"] == 9
    assert values[("dau", "")] == 4
    assert values[("views_started", "")] == 3
    assert values[("views_finished", "")] == 2
    assert values[("avg_watch_seconds", "")] == 900
    assert values[("conversion", "")] == pytest.approx(2 / 3)
    assert values[("cohort_size", "")] == 3
    assert values[("retention_day_0", "")] == 1
    assert values[("retention_d1", "")] == pytest.approx(2 / 3)
    assert values[("retention_day_2", "")] == pytest.approx(1 / 3)
    assert values[("retention_day_3", "")] == 0
    assert values[("retention_d7", "")] == pytest.approx(1 / 3)
    assert values[("movie_views", "movie-a")] == 2
    assert values[("movie_views", "movie-b")] == 1
    assert values[("device_users", "TV")] == 4


def test_recalculation_is_idempotent_and_matches_clickhouse(metrics):
    before = pg_metrics()
    response = httpx.post(f"{AGGREGATION}/recalculate", params={"date": "2024-01-01"}, timeout=120)
    assert response.status_code == 200, response.text
    assert pg_metrics() == before
    rows = ClickHouse().query("SELECT * FROM cinema.daily_metrics WHERE metric_date = '2024-01-01'")
    assert len(rows) == len(before)
    assert {(r["metric_name"], r["dimension"]): r["value"] for r in rows} == before


def test_empty_day_has_defined_nulls():
    response = httpx.post(f"{AGGREGATION}/recalculate", params={"date": "2023-12-01"}, timeout=120)
    assert response.status_code == 200, response.text
    values = pg_metrics("2023-12-01")
    assert values[("dau", "")] == 0
    for name in ("avg_watch_seconds", "conversion", "retention_d1", "retention_d7"):
        assert values[(name, "")] is None


def test_unfinished_cohort_days_are_null():
    now = datetime.now(timezone.utc)
    payload = event("immature", "VIEW_STARTED")
    payload["user_id"] = "immature-user"
    payload["timestamp"] = now.isoformat()
    response = httpx.post(f"{PRODUCER}/events", json=payload, timeout=120)
    assert response.status_code == 201, response.text
    ch = ClickHouse()
    wait_for(lambda: ch.query("SELECT event_id FROM cinema.events FINAL WHERE event_id = {id:UUID}", {"id": payload["event_id"]}))
    day = now.date().isoformat()
    response = httpx.post(f"{AGGREGATION}/recalculate", params={"date": day}, timeout=120)
    assert response.status_code == 200, response.text
    values = pg_metrics(day)
    assert values[("retention_day_0", "")] == 1
    assert values[("retention_d1", "")] is None
    assert values[("retention_d7", "")] is None


def test_late_arrival_updates_previous_daily_result():
    ch = ClickHouse()
    for kind, progress, expected in [("VIEW_STARTED", 0, 0), ("VIEW_FINISHED", 500, 1)]:
        payload = event("late", kind, day=31, progress=progress, offset=progress)
        payload["user_id"] = "late-user"
        response = httpx.post(f"{PRODUCER}/events", json=payload, timeout=120)
        assert response.status_code == 201, response.text
        wait_for(lambda: ch.query("SELECT event_id FROM cinema.events FINAL WHERE event_id = {id:UUID}", {"id": payload["event_id"]}))
        response = httpx.post(f"{AGGREGATION}/recalculate", params={"date": "2024-02-01"}, timeout=120)
        assert response.status_code == 200, response.text
        assert pg_metrics("2024-02-01")[("conversion", "")] == expected


def test_s3_export_contains_all_metrics_and_overwrites(metrics):
    s3 = s3_client()
    key = "daily/2024-01-01/aggregates.json"
    bodies = []
    for _ in range(2):
        response = httpx.post(f"{AGGREGATION}/export", params={"date": "2024-01-01"}, timeout=120)
        assert response.status_code == 200, response.text
        assert response.json()["key"] == key
        bodies.append(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    assert bodies[0] == bodies[1]
    document = json.loads(bodies[0])
    assert document["metric_date"] == "2024-01-01"
    assert document["computed_at"]
    assert {(r["metric_name"], r["dimension"]): r["value"] for r in document["metrics"]} == pg_metrics()
    assert all(r["metric_date"] == "2024-01-01" and r["computed_at"] for r in document["metrics"])
    objects = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix="daily/2024-01-01/")["Contents"]
    assert [obj["Key"] for obj in objects] == [key]


def test_grafana_datasource_and_all_dashboard_queries(metrics):
    with httpx.Client(base_url="http://grafana:3000", auth=("admin", "admin"), timeout=60) as client:
        health = client.get("/api/datasources/uid/clickhouse/health")
        assert health.status_code == 200, health.text
        dashboard = client.get("/api/dashboards/uid/online-cinema").json()["dashboard"]
        assert len(dashboard["panels"]) >= 3
        for panel in dashboard["panels"]:
            query = dict(panel["targets"][0], intervalMs=60000, maxDataPoints=1000)
            assert "cinema.daily_metrics" in query["rawSql"]
            response = client.post("/api/ds/query", json={
                "from": "1703980800000", "to": "1704758400000", "queries": [query],
            })
            assert response.status_code == 200, (panel["title"], response.text)
            result = response.json()["results"]["A"]
            assert not result.get("error"), (panel["title"], result)
            assert result.get("frames"), (panel["title"], result)
