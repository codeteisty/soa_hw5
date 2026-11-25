import heapq
import logging
import os
import random
import time
from datetime import datetime, time as day_time, timedelta, timezone
from pathlib import Path
from uuid import NAMESPACE_URL, uuid4, uuid5

from app.common.events import MovieEvent
from app.producer.publisher import Publisher

log = logging.getLogger(__name__)
HEARTBEAT = Path("/tmp/generator-heartbeat")
RETENTION = [1, 0.65, 0.52, 0.45, 0.40, 0.35, 0.30, 0.25]


def session_events(user, movie, start, duration, rng, session_id=None):
    session_id = session_id or str(uuid4())
    device = rng.choice(["MOBILE", "DESKTOP", "TV", "TABLET"])
    items = [(0, "SEARCHED", 0), (2, "VIEW_STARTED", 0)]
    pause = rng.random() < 0.6
    wait = 0
    if pause:
        progress = duration // 3
        wait = 10
        items += [(2 + progress, "VIEW_PAUSED", progress), (2 + progress + wait, "VIEW_RESUMED", progress)]
    if rng.random() < 0.8:
        items.append((2 + duration + wait, "VIEW_FINISHED", duration))
        if rng.random() < 0.45:
            items.append((5 + duration + wait, "LIKED", duration))
    elif not pause:
        items.append((2 + duration // 2, "VIEW_PAUSED", duration // 2))
    result = []
    for offset, event_type, progress in items:
        result.append({
            "event_id": str(uuid5(NAMESPACE_URL, f"{session_id}:{event_type}")),
            "user_id": user, "movie_id": "" if event_type == "SEARCHED" else movie,
            "event_type": event_type, "timestamp": start + timedelta(seconds=offset),
            "device_type": device, "session_id": session_id, "progress_seconds": progress,
        })
    return result


def seed_history(publisher):
    marker = Path("/data/history-seeded")
    if marker.exists():
        return
    anchor_path = Path("/data/history-anchor")
    if not anchor_path.exists():
        anchor_path.write_text(datetime.now(timezone.utc).date().isoformat())
    anchor = datetime.fromisoformat(anchor_path.read_text()).date()
    rng = random.Random(42)
    events = []
    for cohort_age in range(12, 0, -1):
        cohort = anchor - timedelta(days=cohort_age)
        for person in range(12):
            user = f"demo-{cohort}-{person}"
            for age in range(min(7, cohort_age - 1) + 1):
                if rng.random() > RETENTION[age]:
                    continue
                day = cohort + timedelta(days=age)
                start = datetime.combine(day, day_time(12, person), timezone.utc)
                events.extend(session_events(user, f"movie-{rng.randint(1, 8):02}", start,
                                             rng.randint(1800, 7200), rng, f"{user}-day-{age}"))
    # A restart uses the same anchor, seed and event IDs.
    for event in sorted(events, key=lambda item: item["timestamp"]):
        HEARTBEAT.touch()
        publisher.publish(MovieEvent(**event))
    marker.write_text(str(len(events)))
    log.info("historical seed finished events=%s", len(events))


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    HEARTBEAT.touch()
    if os.getenv("GENERATOR_ENABLED", "true").lower() != "true":
        while True:
            HEARTBEAT.touch()
            time.sleep(10)
    publisher = Publisher()
    if os.getenv("SEED_HISTORY", "true").lower() == "true":
        seed_history(publisher)
    rng = random.Random()
    queue = []
    active = set()
    next_session = 0
    sequence = 0
    interval = max(0.1, float(os.getenv("GENERATOR_INTERVAL_SECONDS", "3")))
    while True:
        HEARTBEAT.touch()
        now = datetime.now(timezone.utc)
        if time.monotonic() >= next_session:
            idle = [f"live-{i}" for i in range(100) if f"live-{i}" not in active]
            if idle:
                user = rng.choice(idle)
                events = session_events(user, f"movie-{rng.randint(1, 8):02}", now,
                                        rng.randint(30, 120), rng)
                active.add(user)
                for index, event in enumerate(events):
                    sequence += 1
                    heapq.heappush(queue, (event["timestamp"], sequence, event, index == len(events) - 1))
            next_session = time.monotonic() + interval
        while queue and queue[0][0] <= now:
            _, _, event, last = queue[0]
            try:
                publisher.publish(MovieEvent(**event))
            except Exception:
                log.exception("generator publication failed; keeping event for retry")
                break
            heapq.heappop(queue)
            if last:
                active.remove(event["user_id"])
        time.sleep(0.25)


if __name__ == "__main__":
    main()
