import random
from datetime import datetime, timedelta, timezone

from app.common.events import MovieEvent
from app.producer.generator import session_events


def test_generated_sessions_are_ordered_and_consistent():
    rng = random.Random(7)
    start = datetime.now(timezone.utc) - timedelta(days=1)
    kinds = set()
    for i in range(100):
        events = session_events(f"u-{i}", "movie-1", start, 120, rng)
        assert len({e["session_id"] for e in events}) == 1
        assert len({e["device_type"] for e in events}) == 1
        assert len({e["event_id"] for e in events}) == len(events)
        watching = [e for e in events if e["event_type"].startswith("VIEW_")]
        assert watching[0]["event_type"] == "VIEW_STARTED"
        assert [e["progress_seconds"] for e in watching] == sorted(e["progress_seconds"] for e in watching)
        assert [e["timestamp"] for e in events] == sorted(e["timestamp"] for e in events)
        for e in events:
            MovieEvent(**e)
            kinds.add(e["event_type"])
        if "VIEW_RESUMED" in [e["event_type"] for e in events]:
            names = [e["event_type"] for e in events]
            assert names.index("VIEW_PAUSED") < names.index("VIEW_RESUMED")
    assert len(kinds) == 6
