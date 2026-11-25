from datetime import datetime, timezone
from enum import Enum
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class EventType(str, Enum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(str, Enum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


class MovieEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID = Field(default_factory=uuid4)
    user_id: str = Field(min_length=1, max_length=128)
    movie_id: str = Field(max_length=128)
    event_type: EventType
    timestamp: datetime
    device_type: DeviceType
    session_id: str = Field(min_length=1, max_length=128)
    progress_seconds: int = Field(strict=True, ge=0, le=2_147_483_647)

    @field_validator("timestamp")
    @classmethod
    def check_timestamp(cls, value):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("timestamp must contain a timezone")
        value = value.astimezone(timezone.utc)
        if value > datetime.now(timezone.utc):
            raise ValueError("timestamp cannot be in the future")
        if value.year < 1970 or value.year > 2100:
            raise ValueError("timestamp must be between 1970 and 2100")
        return value.replace(microsecond=value.microsecond // 1000 * 1000)

    @model_validator(mode="after")
    def check_event(self):
        if not self.user_id.strip() or not self.session_id.strip():
            raise ValueError("user_id and session_id cannot be blank")
        if self.event_type != EventType.SEARCHED and not self.movie_id.strip():
            raise ValueError("movie_id is required except for SEARCHED")
        if self.event_type in (EventType.SEARCHED, EventType.VIEW_STARTED) and self.progress_seconds != 0:
            raise ValueError("SEARCHED and VIEW_STARTED must have zero progress")
        return self

    def avro_record(self):
        record = self.model_dump(mode="json")
        delta = self.timestamp - datetime(1970, 1, 1, tzinfo=timezone.utc)
        record["timestamp"] = delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000
        return record
