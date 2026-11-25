import logging
import os
import threading
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone

import psycopg
from fastapi import FastAPI, HTTPException

from app.aggregation.service import MissingAggregate, calculate, export_day, export_pending
from app.common.clients import ClickHouse
from app.common.config import PG_DSN

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger(__name__)


def scheduler(stop):
    interval = max(1, int(os.getenv("AGGREGATION_INTERVAL_SECONDS", "60")))
    window = max(8, int(os.getenv("RECALCULATE_DAYS", "14")))
    while not stop.wait(interval):
        today = datetime.now(timezone.utc).date()
        for offset in range(window, -1, -1):
            if stop.is_set():
                return
            day = today - timedelta(days=offset)
            try:
                calculate(day)
            except Exception:
                log.exception("aggregation failed date=%s; will retry next cycle", day)
        try:
            export_pending()
        except Exception:
            log.exception("export queue unavailable; will retry next cycle")


@asynccontextmanager
async def lifespan(app):
    stop = threading.Event()
    thread = None
    if os.getenv("SCHEDULER_ENABLED", "true").lower() == "true":
        thread = threading.Thread(target=scheduler, args=(stop,), daemon=True)
        thread.start()
    yield
    stop.set()
    if thread:
        thread.join(timeout=5)


app = FastAPI(title="Cinema Aggregation", version="1.0", lifespan=lifespan)


def validate_day(day):
    if day > datetime.now(timezone.utc).date() or day < date(1970, 1, 1):
        raise HTTPException(422, "date must be between 1970-01-01 and today (UTC)")


@app.post("/recalculate")
def recalculate(date: date):
    validate_day(date)
    try:
        return calculate(date)
    except Exception:
        log.exception("manual calculation failed date=%s", date)
        raise HTTPException(503, "Aggregation failed; safe to retry")


@app.post("/export")
def export(date: date):
    validate_day(date)
    try:
        return export_day(date)
    except MissingAggregate as exc:
        raise HTTPException(404, str(exc))
    except Exception:
        raise HTTPException(503, "Export failed; queued jobs will be retried")


@app.get("/health")
def health():
    try:
        ClickHouse().query("SELECT 1")
        with psycopg.connect(PG_DSN, connect_timeout=3) as conn:
            conn.execute("SELECT 1 FROM daily_metrics LIMIT 1")
        return {"status": "ok"}
    except Exception:
        raise HTTPException(503, "ClickHouse or PostgreSQL is unavailable")
