"""Sync worker process.

Stage 2 changes vs Stage 1:
- BLPOP returns just an ID. The worker fetches and claims the row with
  SELECT ... FOR UPDATE SKIP LOCKED, transitions it through running → done/failed,
  and writes errors back to the row.
- A handler exception no longer just logs — it sets status='failed' and stores
  the traceback in the `error` column. Retry logic comes in Stage 4.

Why SELECT FOR UPDATE SKIP LOCKED:
- FOR UPDATE: take a row-level lock so no other worker can grab the same job.
- SKIP LOCKED: if another worker already locked it, don't block — move on.
  This is what makes the queue scale: every worker proceeds independently.
- Combined with `WHERE status='queued'`: protects against double-delivery if
  Redis ever re-pushes the same ID (which it can, in failure modes we'll
  cover in later stages).
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import time
import traceback
from types import FrameType

import redis

from core import db
from core.handlers import get as get_handler
from core.handlers import known_types
from core.job import WireEnvelope
from core.settings import queue_key, settings

# Side-effect import: registers the example handlers via @register.
import examples.handlers  # noqa: F401

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(f"worker[{os.getpid()}]")

_shutdown = False


def _on_signal(signum: int, _frame: FrameType | None) -> None:
    global _shutdown
    log.info("received signal %s, shutting down after current job", signum)
    _shutdown = True


def _claim_and_run(job_id: str) -> None:
    """Claim the row, run the handler, mark final status.

    All three transitions (claim, success, failure) commit independently so a
    crash in handler code can't roll back the 'running' marker — the row is
    visible as 'running' in the dashboard, and Stage 3's reaper will reclaim
    it if the worker doesn't update it within the heartbeat window.
    """
    with db.connection() as conn, conn.cursor() as cur:
        # Claim. If the row was already claimed (status != queued) or another
        # worker holds the lock, this returns no rows and we just bail.
        cur.execute(
            """
            UPDATE jobs
               SET status = 'running',
                   attempts = attempts + 1,
                   started_at = now()
             WHERE id = (
                 SELECT id FROM jobs
                  WHERE id = %s AND status = 'queued'
                  FOR UPDATE SKIP LOCKED
             )
         RETURNING type, args
            """,
            (job_id,),
        )
        row = cur.fetchone()
        if row is None:
            conn.commit()
            log.info("id=%s not claimable (already running/done, or locked)", job_id)
            return
        job_type, args = row
        conn.commit()

    log.info("running id=%s type=%s", job_id, job_type)
    started = time.monotonic()

    # Run the handler OUTSIDE the DB transaction. Holding a DB connection
    # while running arbitrary user code is asking for connection-pool starvation.
    try:
        handler = get_handler(job_type)
        handler(**(args or {}))
    except LookupError as e:
        _mark_failed(job_id, f"unknown handler: {e}")
        log.error("id=%s %s", job_id, e)
        return
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
        cur.execute(
            "UPDATE jobs SET status='done', completed_at=now(), error=NULL WHERE id=%s",
            (job_id,),
        )
        conn.commit()


def _mark_failed(job_id: str, error: str) -> None:
    # Truncate so a wild stack trace can't bloat the row.
    error = error[:8000]
    with db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE jobs SET status='failed', completed_at=now(), error=%s WHERE id=%s",
            (error, job_id),
        )
        conn.commit()


def main() -> int:
    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    log.info("starting. queue=%s handlers=%s", settings.default_queue, known_types())

    # Ensure schema is up to date before workers start consuming.
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
