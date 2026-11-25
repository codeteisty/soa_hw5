import os
import time
from datetime import datetime, timedelta, timezone
from uuid import NAMESPACE_URL, uuid5

import httpx
import pytest

from app.common.clients import ClickHouse

PRODUCER = os.getenv("PRODUCER_URL", "http://producer:8000")
AGGREGATION = os.getenv("AGGREGATION_URL", "http://aggregation:8000")


def wait_for(operation, timeout=90):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = operation()
            if result:
                return result
        except Exception as exc:
            last_error = exc
        time.sleep(1)
    raise AssertionError(f"Condition was not met within {timeout}s; last error: {last_error}")


def event(user, kind, day=0, progress=0, movie="movie-a", offset=0):
    timestamp = datetime(2024, 1, 1, 12, tzinfo=timezone.utc) + timedelta(days=day, seconds=offset)
    session = f"fixture-{user}-day-{day}"
    return {
        "event_id": str(uuid5(NAMESPACE_URL, f"{session}:{kind}:{offset}")),
        "user_id": f"fixture-{user}", "movie_id": movie,
        "event_type": kind, "timestamp": timestamp.isoformat(), "device_type": "TV",
        "session_id": session, "progress_seconds": progress,
    }


@pytest.fixture(scope="session")
def dataset():
    events = [
        event("a", "SEARCHED", day=-2, movie=""),
        event("a", "VIEW_STARTED"),
        event("a", "VIEW_PAUSED", progress=300, offset=300),
        event("a", "VIEW_RESUMED", progress=300, offset=360),
        event("a", "VIEW_FINISHED", progress=1200, offset=1260),
        event("b", "VIEW_STARTED", movie="movie-b"),
        event("b", "VIEW_FINISHED", movie="movie-b", progress=600, offset=600),
        event("c", "VIEW_STARTED"),
        event("c", "VIEW_PAUSED", progress=300, offset=300),
        event("search-only", "SEARCHED", movie=""),
        event("a", "VIEW_STARTED", day=1),
        event("a", "VIEW_FINISHED", day=1, progress=900, offset=900),
        event("b", "SEARCHED", day=1, movie=""),
        event("c", "SEARCHED", day=2, movie=""),
        event("a", "VIEW_STARTED", day=7),
        event("a", "VIEW_FINISHED", day=7, progress=900, offset=900),
    ]
    with httpx.Client(timeout=120) as client:
        for item in events + [events[1]]:
            response = client.post(f"{PRODUCER}/events", json=item)
            assert response.status_code == 201, response.text
            assert response.json()["event_id"] == item["event_id"]
    ch = ClickHouse()
    def all_ingested():
        rows = ch.query("SELECT * FROM cinema.events FINAL WHERE startsWith(user_id, 'fixture-')")
        return rows if len(rows) == len(events) else None
    wait_for(all_ingested)
    return events


@pytest.fixture(scope="session")
def metrics(dataset):
    response = httpx.post(f"{AGGREGATION}/recalculate", params={"date": "2024-01-01"}, timeout=120)
    assert response.status_code == 200, response.text
    return response.json()
