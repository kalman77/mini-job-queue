"""Sync worker process.

Stage 3 changes vs Stage 2:
- Each worker has a stable identity (worker_id) it stamps onto the row
  when it claims. The heartbeat thread updates last_heartbeat_at while
  scoped to that identity, so a reaped-and-resubmitted job claimed by
  another worker won't be heartbeated by the original worker thread.
- The handler runs inside a Heartbeat context manager. The context manager
  starts a background thread that updates last_heartbeat_at on a timer.
"""
from __future__ import annotations

import logging
import os
import signal
import socket
import sys
import time
import traceback
import uuid
from types import FrameType

import redis

from core import db
from core.handlers import get as get_handler
from core.handlers import known_types
from core.heartbeat import Heartbeat
from core.job import WireEnvelope
from core.settings import queue_key, settings

# Side-effect import: registers the example handlers via @register.
import examples.handlers  # noqa: F401

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(f"worker[{os.getpid()}]")

# Stable identity for this worker process. host:pid:short-uuid is enough
# entropy that you can run dozens of workers across machines without collision,
# and the host/pid prefix is useful when reading the column in psql.
WORKER_ID = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"

_shutdown = False


def _on_signal(signum: int, _frame: FrameType | None) -> None:
    global _shutdown
    log.info("received signal %s, shutting down after current job", signum)
    _shutdown = True


def _claim_and_run(job_id: str) -> None:
    """Claim the row, run the handler under a heartbeat, mark final status."""
    with db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
               SET status = 'running',
                   attempts = attempts + 1,
                   started_at = COALESCE(started_at, now()),
                   last_heartbeat_at = now(),
                   locked_by = %s,
                   error = NULL
             WHERE id = (
                 SELECT id FROM jobs
                  WHERE id = %s AND status = 'queued'
                  FOR UPDATE SKIP LOCKED
             )
         RETURNING type, args
            """,
            (WORKER_ID, job_id),
        )
        row = cur.fetchone()
        if row is None:
            conn.commit()
            log.info("id=%s not claimable (already running/done, or locked)", job_id)
            return
        job_type, args = row
        conn.commit()

    log.info("running id=%s type=%s worker=%s", job_id, job_type, WORKER_ID)
    started = time.monotonic()

    try:
        handler = get_handler(job_type)
    except LookupError as e:
        _mark_failed(job_id, f"unknown handler: {e}")
        log.error("id=%s %s", job_id, e)
        return

    try:
        with Heartbeat(job_id, WORKER_ID):
            handler(**(args or {}))
    except Exception:
        tb = traceback.format_exc()
        _mark_failed(job_id, tb)
        elapsed = time.monotonic() - started
        log.error("id=%s FAILED after %.2fs", job_id, elapsed)
        log.error(tb)
        return

    _mark_done(job_id)
    elapsed = time.monotonic() - started
    log.info("id=%s done in %.2fs", job_id, elapsed)


def _mark_done(job_id: str) -> None:
    with db.connection() as conn, conn.cursor() as cur:
        # Predicate the update on locked_by: a reaped-and-reclaimed job
        # whose original worker eventually finishes must NOT mark itself done.
        # The new worker owns the outcome.
        cur.execute(
            """
            UPDATE jobs
               SET status='done',
                   completed_at=now(),
                   last_heartbeat_at=NULL,
                   locked_by=NULL,
                   error=NULL
             WHERE id=%s AND locked_by=%s
            """,
            (job_id, WORKER_ID),
        )
        conn.commit()


def _mark_failed(job_id: str, error: str) -> None:
    error = error[:8000]
    with db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
               SET status='failed',
                   completed_at=now(),
                   last_heartbeat_at=NULL,
                   locked_by=NULL,
                   error=%s
             WHERE id=%s AND locked_by=%s
            """,
            (error, job_id, WORKER_ID),
        )
        conn.commit()


def main() -> int:
    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    log.info("starting. id=%s queue=%s handlers=%s",
             WORKER_ID, settings.default_queue, known_types())

    db.run_migrations_sync()

    r = redis.Redis.from_url(settings.redis_url)
    try:
        r.ping()
    except redis.RedisError as e:
        log.error("redis unreachable: %s", e)
        return 1

    qkey = queue_key(settings.default_queue)
    while not _shutdown:
        try:
            popped = r.blpop([qkey], timeout=settings.blpop_timeout)
        except redis.RedisError as e:
            log.warning("redis error during BLPOP: %s — retrying in 1s", e)
            time.sleep(1)
            continue

        if popped is None:
            continue
        _, raw = popped

        try:
            envelope = WireEnvelope.from_wire(raw)
        except Exception:
            log.error("could not decode envelope, dropping. raw=%r", raw[:200])
            log.error(traceback.format_exc())
            continue

        _claim_and_run(str(envelope.id))

    log.info("clean shutdown")
    db.close_sync()
    return 0


if __name__ == "__main__":
    sys.exit(main())
