import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from app.common.events import MovieEvent
from app.producer.publisher import Publisher

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


@asynccontextmanager
async def lifespan(app):
    app.state.publisher = Publisher()
    yield
    app.state.publisher.close()


app = FastAPI(title="Movie Service", version="1.0", lifespan=lifespan)


@app.post("/events", status_code=201)
def publish_event(event: MovieEvent):
    try:
        event_id = app.state.publisher.publish(event)
        return {"event_id": event_id}
    except Exception:
        logging.exception("publication failed event_id=%s", event.event_id)
        raise HTTPException(503, "Kafka publication failed; retry with the same event_id")


@app.get("/health")
def health():
    try:
        app.state.publisher.check()
        return {"status": "ok", "schema_version": 1}
    except Exception:
        raise HTTPException(503, "Kafka is unavailable")
