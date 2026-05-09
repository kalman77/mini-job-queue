"""Job model and wire envelope.

Stage 2 split:
- `Job` mirrors a row in the `jobs` table. It's the truth.
- `WireEnvelope` is what travels through Redis: just the job id. Workers use
  it to look up the row and claim it under SELECT FOR UPDATE SKIP LOCKED.

The reason for the split: in Stage 1 the whole payload was on the Redis
list, which meant losing Redis was losing jobs. Now Redis is just a fast
ready-queue index — the row is durable.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _new_id() -> uuid.UUID:
    return uuid.uuid4()


class Job(BaseModel):
    """A job row. Mirrors columns in the jobs table."""
    id: uuid.UUID = Field(default_factory=_new_id)
    type: str
    args: dict[str, Any] = Field(default_factory=dict)
    status: str = "queued"   # queued | running | done | failed
    queue: str = "default"
    attempts: int = 0
    max_attempts: int = 3
    enqueued_at: datetime = Field(default_factory=_now)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    last_heartbeat_at: datetime | None = None
    locked_by: str | None = None
    error: str | None = None


class WireEnvelope(BaseModel):
    """What goes onto the Redis list. Tiny on purpose."""
    id: uuid.UUID

    def to_wire(self) -> bytes:
        # bytes, not str — redis-py handles either, but bytes is what BLPOP returns
        # on the worker side, so we keep both ends symmetric.
        return json.dumps({"id": str(self.id)}).encode("utf-8")

    @classmethod
    def from_wire(cls, raw: str | bytes) -> "WireEnvelope":
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        data = json.loads(raw)
        return cls(id=uuid.UUID(data["id"]))


# Producer HTTP schemas -------------------------------------------------------

class EnqueueRequest(BaseModel):
    type: str
    args: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int = 3


class EnqueueResponse(BaseModel):
    id: uuid.UUID
    queue: str
    status: str
