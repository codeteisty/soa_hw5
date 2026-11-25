import json
import logging
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from app.common.clients import ClickHouse, s3_client
from app.common.config import PG_DSN, S3_BUCKET
from app.common.retry import retry

log = logging.getLogger(__name__)
SQL_DIR = Path(__file__).resolve().parents[2] / "sql/clickhouse"
LOCK = threading.RLock()


class MissingAggregate(Exception):
    pass


def calculate(day: date):
    with LOCK:
        started = time.monotonic()
        log.info("aggregation started date=%s", day)
        ch = ClickHouse()
        parameters = {"day": day, "as_of": datetime.now(timezone.utc).date() - timedelta(days=1)}
        daily = retry(lambda: ch.query((SQL_DIR / "daily_metrics.sql").read_text(), parameters))[0]
        cohort = retry(lambda: ch.query((SQL_DIR / "retention.sql").read_text(), parameters))
        movies = retry(lambda: ch.query("""
            SELECT movie_id, countIf(event_type = 'VIEW_STARTED') AS views
            FROM cinema.events FINAL
            WHERE toDate(timestamp) = {day:Date} AND event_type = 'VIEW_STARTED'
            GROUP BY movie_id ORDER BY views DESC, movie_id
        """, parameters))
        devices = retry(lambda: ch.query("""
            SELECT toString(device_type) AS device, uniqExact(user_id) AS users
            FROM cinema.events FINAL WHERE toDate(timestamp) = {day:Date}
            GROUP BY device_type
        """, parameters))

        computed_at = datetime.now(timezone.utc)
        version = time.time_ns()
        rows = []

        def add(name, value, dimension=""):
            rows.append({"metric_date": day.isoformat(), "metric_name": name, "dimension": dimension,
                         "value": float(value) if value is not None else None,
                         "computed_at": computed_at.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                         "version": version})

        for name in ("dau", "views_started", "views_finished", "avg_watch_seconds", "conversion"):
            add(name, daily[name])
        by_day = {int(item["day_number"]): item for item in cohort}
        add("cohort_size", int(cohort[0]["cohort_size"]) if cohort else 0)
        for number in range(8):
            item = by_day.get(number, {})
            add(f"retention_day_{number}", item.get("retention"))
        add("retention_d1", by_day.get(1, {}).get("retention"))
        add("retention_d7", by_day.get(7, {}).get("retention"))
        for movie in movies:
            add("movie_views", movie["views"], movie["movie_id"])
        for device in devices:
            add("device_users", device["users"], device["device"])

        # Unfinished snapshots are invisible to the dashboard.
        retry(lambda: ch.insert("cinema.metric_snapshots", rows))

        def save_postgres():
            with psycopg.connect(PG_DSN, connect_timeout=5) as conn:
                with conn.cursor() as cur:
                    cur.executemany("""
                        INSERT INTO daily_metrics(metric_date, metric_name, dimension, value, computed_at)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (metric_date, metric_name, dimension)
                        DO UPDATE SET value = EXCLUDED.value, computed_at = EXCLUDED.computed_at
                    """, [(day, row["metric_name"], row["dimension"], row["value"], computed_at) for row in rows])
                    for name in ("movie_views", "device_users"):
                        dimensions = [row["dimension"] for row in rows if row["metric_name"] == name]
                        cur.execute("""
                            DELETE FROM daily_metrics WHERE metric_date = %s AND metric_name = %s
                            AND NOT (dimension = ANY(%s))
                        """, (day, name, dimensions))
                    cur.execute("""
                        INSERT INTO aggregation_runs(metric_date, processed_events, computed_at)
                        VALUES (%s, %s, %s) ON CONFLICT (metric_date)
                        DO UPDATE SET processed_events = EXCLUDED.processed_events, computed_at = EXCLUDED.computed_at
                    """, (day, int(daily["processed_events"]), computed_at))
                    cur.execute("""
                        INSERT INTO export_jobs(metric_date, requested_at) VALUES (%s, %s)
                        ON CONFLICT (metric_date) DO UPDATE
                        SET requested_at = EXCLUDED.requested_at, exported_at = NULL, last_error = NULL
                    """, (day, computed_at))

        retry(save_postgres, exceptions=(psycopg.Error,))
        retry(lambda: ch.insert("cinema.aggregation_runs", [{
            "metric_date": day.isoformat(), "version": version,
            "processed_events": int(daily["processed_events"]),
            "computed_at": rows[0]["computed_at"],
        }]))
        elapsed = round(time.monotonic() - started, 3)
        log.info("aggregation finished date=%s processed_events=%s metrics=%s duration_seconds=%s",
                 day, daily["processed_events"], len(rows), elapsed)
        return {"date": day, "processed_events": int(daily["processed_events"]),
                "metrics": len(rows), "duration_seconds": elapsed}


def export_day(day: date):
    with LOCK:
        key = f"daily/{day.isoformat()}/aggregates.json"
        try:
            def read_metrics():
                with psycopg.connect(PG_DSN, connect_timeout=5, row_factory=dict_row) as conn:
                    rows = conn.execute("""
                        SELECT metric_date, metric_name, dimension, value, computed_at
                        FROM daily_metrics WHERE metric_date = %s ORDER BY metric_name, dimension
                    """, (day,)).fetchall()
                if not rows:
                    raise MissingAggregate(f"No aggregates for {day}; recalculate first")
                return rows

            rows = retry(read_metrics, exceptions=(psycopg.Error,))
            document = {"metric_date": day.isoformat(), "computed_at": rows[0]["computed_at"].isoformat(),
                        "metrics": rows}
            content = json.dumps(document, default=lambda value: value.isoformat(),
                                 ensure_ascii=False, sort_keys=True, allow_nan=False).encode()
            s3 = s3_client()
            retry(lambda: s3.put_object(Bucket=S3_BUCKET, Key=key, Body=content,
                                       ContentType="application/json; charset=utf-8"))
            def mark_exported():
                with psycopg.connect(PG_DSN, connect_timeout=5) as conn:
                    conn.execute("""
                        UPDATE export_jobs SET exported_at = now(), attempts = attempts + 1, last_error = NULL
                        WHERE metric_date = %s
                    """, (day,))
            retry(mark_exported, exceptions=(psycopg.Error,))
            log.info("export finished date=%s key=%s metrics=%s", day, key, len(rows))
            return {"date": day, "key": key, "metrics": len(rows)}
        except MissingAggregate:
            raise
        except Exception as exc:
            log.exception("export failed date=%s; will retry on next scheduled run", day)
            try:
                with psycopg.connect(PG_DSN, connect_timeout=5) as conn:
                    conn.execute("""
                        UPDATE export_jobs SET attempts = attempts + 1, last_error = %s,
                            exported_at = NULL WHERE metric_date = %s
                    """, (str(exc)[:2000], day))
            except psycopg.Error:
                log.exception("could not save export failure date=%s", day)
            raise


def export_pending():
    with psycopg.connect(PG_DSN, connect_timeout=5) as conn:
        days = conn.execute("""
            SELECT metric_date FROM export_jobs
            WHERE exported_at IS NULL AND metric_date < (now() AT TIME ZONE 'UTC')::date
            ORDER BY metric_date
        """).fetchall()
    for (day,) in days:
        try:
            export_day(day)
        except Exception:
            # The job stays in PostgreSQL, including after restarting this service.
            log.exception("pending export failed date=%s", day)
