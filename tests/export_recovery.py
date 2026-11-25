"""Check a real S3 outage, followed by the next processing of the export queue."""
import sys

import httpx
import psycopg

from app.aggregation.service import export_pending
from app.common.clients import s3_client
from app.common.config import PG_DSN, S3_BUCKET
from tests.conftest import AGGREGATION

DAY = "2024-01-01"


def main():
    if sys.argv[1] == "unavailable":
        response = httpx.post(f"{AGGREGATION}/recalculate", params={"date": DAY}, timeout=120)
        assert response.status_code == 200, response.text
        response = httpx.post(f"{AGGREGATION}/export", params={"date": DAY}, timeout=120)
        assert response.status_code == 503, response.text
        with psycopg.connect(PG_DSN) as conn:
            row = conn.execute("SELECT exported_at, last_error FROM export_jobs WHERE metric_date = %s", (DAY,)).fetchone()
        assert row[0] is None and row[1]
        print("S3 outage OK: failed export remains in the durable queue")
    else:
        export_pending()
        with psycopg.connect(PG_DSN) as conn:
            row = conn.execute("SELECT exported_at, last_error FROM export_jobs WHERE metric_date = %s", (DAY,)).fetchone()
        assert row[0] is not None and row[1] is None
        s3_client().head_object(Bucket=S3_BUCKET, Key=f"daily/{DAY}/aggregates.json")
        print("S3 recovery OK: next queue cycle exported the pending date")


if __name__ == "__main__":
    main()
