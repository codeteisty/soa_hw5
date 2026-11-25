import io
import json
import logging
import struct
import threading
import time

import httpx
from confluent_kafka import KafkaException, Producer
from fastavro import parse_schema, schemaless_writer
from fastavro.validation import validate

from app.common.config import KAFKA_BROKERS, REGISTRY_URL, SCHEMA_VERSION, SUBJECT, TOPIC
from app.common.retry import retry

log = logging.getLogger(__name__)


class Publisher:
    def __init__(self):
        def fetch_schema():
            response = httpx.get(f"{REGISTRY_URL}/subjects/{SUBJECT}/versions/{SCHEMA_VERSION}", timeout=10)
            response.raise_for_status()
            return response.json()

        registered = retry(fetch_schema, attempts=8)
        self.schema_id = registered["id"]
        self.schema = parse_schema(json.loads(registered["schema"]))
        self.lock = threading.Lock()
        self.producer = Producer({
            "bootstrap.servers": KAFKA_BROKERS,
            "client.id": "movie-service",
            "acks": "all",
            "enable.idempotence": True,
            "max.in.flight.requests.per.connection": 1,
            "retries": 10,
            "retry.backoff.ms": 200,
            "retry.backoff.max.ms": 5000,
            "delivery.timeout.ms": 30000,
            "request.timeout.ms": 10000,
        })

    def publish(self, event):
        record = event.avro_record()
        validate(record, self.schema, raise_errors=True)
        buffer = io.BytesIO()
        buffer.write(struct.pack(">bI", 0, self.schema_id))
        schemaless_writer(buffer, self.schema, record)
        payload = buffer.getvalue()

        # A lock keeps retries in order when API and generator share this class.
        with self.lock:
            def send():
                outcome = []

                def delivered(error, message):
                    outcome.append((error, message))

                self.producer.produce(TOPIC, key=event.user_id.encode(), value=payload, on_delivery=delivered)
                deadline = time.monotonic() + 35
                while not outcome and time.monotonic() < deadline:
                    self.producer.poll(0.1)
                if not outcome:
                    raise TimeoutError("Kafka delivery confirmation timed out")
                error, message = outcome[0]
                if error:
                    raise KafkaException(error)
                return message

            message = retry(send, attempts=3, exceptions=(KafkaException, BufferError, TimeoutError))
        log.info("published event_id=%s event_type=%s timestamp=%s partition=%s offset=%s",
                 event.event_id, event.event_type.value, event.timestamp.isoformat(),
                 message.partition(), message.offset())
        return str(event.event_id)

    def check(self):
        metadata = self.producer.list_topics(TOPIC, timeout=5)
        topic = metadata.topics.get(TOPIC)
        if not topic or topic.error or len(topic.partitions) < 3:
            raise RuntimeError("Kafka topic is unavailable")

    def close(self):
        self.producer.flush(10)
