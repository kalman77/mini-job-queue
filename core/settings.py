"""Configuration. Env-driven so producer and worker share the same source of truth."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    # Postgres connection string. psycopg understands the standard libpq URL form.
    # For local dev: postgresql://mjq:mjq@localhost:5432/mjq
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql://mjq:mjq@localhost:5432/mjq"
    )
    # Single queue for Stage 1. We'll add priority queues in Stage 5.
    default_queue: str = os.getenv("QUEUE_NAME", "default")
    # BLPOP timeout in seconds. Short so workers stay responsive to SIGINT.
    blpop_timeout: int = int(os.getenv("BLPOP_TIMEOUT", "2"))
    # How often a running worker updates last_heartbeat_at on its row.
    heartbeat_interval_sec: float = float(os.getenv("HEARTBEAT_INTERVAL_SEC", "5"))
    # If a row's last_heartbeat_at is older than this, the reaper considers
    # the worker dead. Must be >> heartbeat_interval to avoid false positives
    # under brief load spikes. Rule of thumb: 4-6x the interval.
    heartbeat_timeout_sec: float = float(os.getenv("HEARTBEAT_TIMEOUT_SEC", "30"))
    # How often the reaper wakes up to scan for stale rows.
    reaper_interval_sec: float = float(os.getenv("REAPER_INTERVAL_SEC", "5"))
    # An orphan is a 'queued' row older than this with no Redis presence
    # (because the producer crashed between INSERT and RPUSH). Generous
    # default — orphan recovery shouldn't fight legitimate enqueue latency.
    orphan_timeout_sec: float = float(os.getenv("ORPHAN_TIMEOUT_SEC", "60"))


settings = Settings()


def queue_key(name: str) -> str:
    """Redis key for a queue list. Namespaced so we don't collide with other tools."""
    return f"mjq:queue:{name}"
