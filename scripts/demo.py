"""Small end-to-end demo using only the Python standard library."""
import base64
import json
import os
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from uuid import uuid4


def request(url, data=None, headers=None):
    response = urllib.request.urlopen(urllib.request.Request(url, data=data, headers=headers or {}), timeout=120)
    return response.read().decode()


def main():
    producer = f"http://localhost:{os.getenv('PRODUCER_PORT', '8005')}"
    aggregation = f"http://localhost:{os.getenv('AGGREGATION_PORT', '8006')}"
    start = (datetime.now(timezone.utc) - timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0)
    session = str(uuid4())
    ids = []
    for kind, progress in (("VIEW_STARTED", 0), ("VIEW_FINISHED", 900)):
        payload = {"event_id": str(uuid4()), "user_id": f"defense-{session}", "movie_id": "movie-defense",
                   "event_type": kind, "timestamp": (start + timedelta(seconds=progress)).isoformat(),
                   "device_type": "TV", "session_id": session, "progress_seconds": progress}
        print(request(producer + "/events", json.dumps(payload).encode(), {"Content-Type": "application/json"}))
        ids.append(payload["event_id"])
    auth = base64.b64encode(b"cinema:cinema_pass").decode()
    deadline = time.monotonic() + 90
    sql = "SELECT count() FROM cinema.events FINAL WHERE event_id IN (" + ",".join(f"'{i}'" for i in ids) + ")"
    while time.monotonic() < deadline:
        count = int(request("http://localhost:8123", sql.encode(), {"Authorization": f"Basic {auth}"}).strip())
        if count == 2:
            break
        time.sleep(1)
    else:
        raise RuntimeError("Events did not arrive in ClickHouse within 90 seconds")
    day = start.date().isoformat()
    print("ClickHouse: both events received")
    print(request(f"{aggregation}/recalculate?date={day}", b""))
    print(request(f"{aggregation}/export?date={day}", b""))
    print(f"Open http://localhost:3000/d/online-cinema and daily/{day}/aggregates.json in MinIO")


if __name__ == "__main__":
    main()
