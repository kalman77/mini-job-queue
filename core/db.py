"""Database layer.

Why one file with both flavours: producer is async, workers are sync, and
both need to talk to Postgres. Centralising the pool construction means the
DSN parsing, defaults, and migration runner live in one place.

We use psycopg 3 (the modern rewrite of psycopg2). Its `psycopg_pool` ships
both `ConnectionPool` (sync) and `AsyncConnectionPool` (async) with the same
configuration surface, which keeps mental load low.

Design notes that you'll want to remember when extending:

- The pool is created lazily on first use, not at import time. Importing this
  module from a test or a CLI shouldn't open sockets.
- run_migrations() is idempotent. We'd swap to a real migration tool (alembic,
  yoyo, sqitch) the moment we have more than ~5 migrations, but at this stage
  one SQL file is honest about what we have.
- We deliberately don't expose connections — we expose `connection()` and
  `aconnection()` context managers so callers can't leak handles.
"""
from __future__ import annotations

import logging
import threading
from contextlib import asynccontextmanager, contextmanager
from importlib import resources
from typing import AsyncIterator, Iterator

import psycopg
import psycopg_pool

from core.settings import settings

log = logging.getLogger("core.db")

_sync_pool: psycopg_pool.ConnectionPool | None = None
_sync_pool_lock = threading.Lock()
_async_pool: psycopg_pool.AsyncConnectionPool | None = None


def _migration_sql() -> str:
    # Read the SQL alongside this module. Avoids hard-coding paths.
    return resources.files("core").joinpath("migrations.sql").read_text(encoding="utf-8")


# --- sync side (workers) -----------------------------------------------------

def get_sync_pool() -> psycopg_pool.ConnectionPool:
    """Lazily construct a process-wide sync pool."""
    global _sync_pool
    if _sync_pool is None:
        with _sync_pool_lock:
            if _sync_pool is None:
                _sync_pool = psycopg_pool.ConnectionPool(
                    conninfo=settings.database_url,
                    min_size=1,
                    max_size=8,
                    kwargs={"autocommit": False},
                    open=True,
                )
    return _sync_pool


@contextmanager
def connection() -> Iterator[psycopg.Connection]:
    """Borrow a sync connection from the pool. Returned on exit."""
    pool = get_sync_pool()
    with pool.connection() as conn:
        yield conn


def run_migrations_sync() -> None:
    """Apply migrations.sql. Safe to run on every worker startup."""
    sql = _migration_sql()
    with connection() as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()
    log.info("migrations applied")


# --- async side (producer) ---------------------------------------------------

async def get_async_pool() -> psycopg_pool.AsyncConnectionPool:
    global _async_pool
    if _async_pool is None:
        _async_pool = psycopg_pool.AsyncConnectionPool(
            conninfo=settings.database_url,
            min_size=1,
            max_size=8,
            kwargs={"autocommit": False},
            open=False,  # explicit open() so lifespan can await it
        )
        await _async_pool.open()
    return _async_pool


@asynccontextmanager
async def aconnection() -> AsyncIterator[psycopg.AsyncConnection]:
    pool = await get_async_pool()
    async with pool.connection() as conn:
        yield conn


async def run_migrations_async() -> None:
    sql = _migration_sql()
    async with aconnection() as conn, conn.cursor() as cur:
        await cur.execute(sql)
        await conn.commit()
    log.info("migrations applied")


async def aclose() -> None:
    """Close the async pool. Call from FastAPI lifespan shutdown."""
    global _async_pool
    if _async_pool is not None:
        await _async_pool.close()
        _async_pool = None


def close_sync() -> None:
    global _sync_pool
    if _sync_pool is not None:
        _sync_pool.close()
        _sync_pool = None
