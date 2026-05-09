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


settings = Settings()


def queue_key(name: str) -> str:
    """Redis key for a queue list. Namespaced so we don't collide with other tools."""
    return f"mjq:queue:{name}"
