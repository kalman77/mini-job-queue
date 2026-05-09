"""Job model — the wire format flowing through Redis.

In Stage 1 the *whole job* travels through Redis as JSON. In Stage 2 we'll
flip this: Redis carries only the job ID, and the row in Postgres carries
the full state. Keeping the Job model centralized here means that change
is a single-file edit later.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _new_id() -> str:
    # uuid4 is fine for Stage 1. We'll switch to uuid7 (time-sortable) when we
    # need ordered enumeration in the dashboard.
    return uuid.uuid4().hex


class Job(BaseModel):
    id: str = Field(default_factory=_new_id)
    type: str  # handler name, e.g. "send_email"
    args: dict[str, Any] = Field(default_factory=dict)
    enqueued_at: datetime = Field(default_factory=_now)

    def to_wire(self) -> str:
        # model_dump_json handles datetime → ISO 8601 for free.
        return self.model_dump_json()

    @classmethod
    def from_wire(cls, raw: str | bytes) -> "Job":
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        return cls.model_validate_json(raw)


class EnqueueRequest(BaseModel):
    """What clients POST to /jobs. Note we don't accept an id — the server mints it."""
    type: str
    args: dict[str, Any] = Field(default_factory=dict)


class EnqueueResponse(BaseModel):
    id: str
    queue: str
