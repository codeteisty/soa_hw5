"""Executed by scripts/test.sh after stopping one broker."""
import httpx
from uuid import uuid4

from app.common.clients import ClickHouse
from tests.conftest import PRODUCER, event, wait_for


def main():
    ids = []
    for index in range(12):
        payload = event(f"failover-{index}", "VIEW_STARTED", day=60)
        payload["event_id"] = str(uuid4())
        payload["user_id"] = f"failover-{index}"
        response = httpx.post(f"{PRODUCER}/events", json=payload, timeout=120)
        assert response.status_code == 201, response.text
        ids.append(payload["event_id"])
    ch = ClickHouse()
    for event_id in ids:
        wait_for(lambda: ch.query("SELECT event_id FROM cinema.events FINAL WHERE event_id = {id:UUID}", {"id": event_id}))
    print("Failover OK: 12 events delivered with kafka-1 stopped")


if __name__ == "__main__":
    main()
