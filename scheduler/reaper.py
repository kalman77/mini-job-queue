"""Reaper: recovers jobs whose owning workers are dead or which were never
pushed to Redis after insert.

Two recovery flows, both safe to run concurrently with workers:

1) Stale running jobs.
   Symptom: row.status='running' AND last_heartbeat_at < now() - timeout.
   Cause:   the worker died / partitioned / paused (GC, OOM kill, kill -9).
   Action:  flip status back to 'queued' and re-push the id to Redis. The
            handler may run twice — that's the at-least-once contract.

2) Orphaned queued jobs.
   Symptom: row.status='queued' AND enqueued_at < now() - orphan_timeout
            AND id is not currently on the Redis list.
   Cause:   producer crashed between INSERT and RPUSH, or RPUSH failed.
   Action:  re-push the id. The DB row is already correct.

Why functions, not a class: the long-running loop (`run_forever`) is the
only stateful piece, and it's tiny. Pure functions for the recovery steps
are easy to unit test and easy to reason about when reading.

Concurrency: multiple reapers can run safely. The atomic UPDATE ... WHERE
status='running' AND last_heartbeat_at < cutoff guarantees only one reaper
flips a given row. If two reapers find the same stale row, one wins the
update and the other sees zero affected rows.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Iterable

import redis

from core import db
from core.job import WireEnvelope
from core.settings import queue_key, settings

log = logging.getLogger("reaper")


def reap_stale_running(r: redis.Redis) -> list[str]:
    """Move stale running rows back to 'queued' and re-push to Redis.

    Returns the list of revived job IDs (mostly for observability and tests).
    """
    cutoff_seconds = settings.heartbeat_timeout_sec
    with db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE jobs
               SET status = 'queued',
                   locked_by = NULL,
                   last_heartbeat_at = NULL
             WHERE status = 'running'
               AND last_heartbeat_at < now() - make_interval(secs => %s)
         RETURNING id, queue
            """,
            (cutoff_seconds,),
        )
        rows = cur.fetchall()
        conn.commit()

    revived: list[str] = []
    for job_id, queue in rows:
        envelope = WireEnvelope(id=job_id)
        try:
            r.rpush(queue_key(queue), envelope.to_wire())
            revived.append(str(job_id))
            log.warning("reaped stale running job id=%s queue=%s -> requeued", job_id, queue)
        except redis.RedisError as e:
            # If Redis is down the row stays 'queued' and we'll retry on the
            # next reaper tick (orphan path will pick it up too once it ages).
            log.error("could not requeue id=%s after reap: %s", job_id, e)
    return revived


def recover_orphans(r: redis.Redis) -> list[str]:
    """Re-push 'queued' rows older than orphan_timeout that aren't on Redis.

    The "aren't on Redis" check is best-effort: we LRANGE the queue list and
    diff. For small queues this is fine. For very large queues you'd want
    a different strategy (e.g. a separate "in_redis" SET maintained by the
    producer). At this stage we keep it simple.
    """
    with db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, queue
              FROM jobs
             WHERE status = 'queued'
               AND enqueued_at < now() - make_interval(secs => %s)
            """,
            (settings.orphan_timeout_sec,),
        )
        candidates = cur.fetchall()
        conn.commit()

    if not candidates:
        return []

    # Fetch which IDs are present on each queue list. One LRANGE per queue.
    queues = {q for _, q in candidates}
    on_redis: set[str] = set()
    for q in queues:
        try:
            members = r.lrange(queue_key(q), 0, -1)
        except redis.RedisError as e:
            log.error("orphan scan: cannot LRANGE queue=%s: %s", q, e)
            continue
        for raw in members:
            try:
                env = WireEnvelope.from_wire(raw)
                on_redis.add(str(env.id))
            except Exception:
                continue  # bad payload — workers will drop it

    recovered: list[str] = []
    for job_id, queue in candidates:
        if str(job_id) in on_redis:
            continue
        envelope = WireEnvelope(id=job_id)
        try:
            r.rpush(queue_key(queue), envelope.to_wire())
            recovered.append(str(job_id))
            log.warning("recovered orphan id=%s queue=%s", job_id, queue)
        except redis.RedisError as e:
            log.error("could not requeue orphan id=%s: %s", job_id, e)
    return recovered


def tick(r: redis.Redis) -> dict[str, list[str]]:
    """One full reaper iteration. Returns a small summary for observability."""
    return {
        "reaped": reap_stale_running(r),
        "recovered": recover_orphans(r),
    }


def run_forever(r: redis.Redis, stop: threading.Event) -> None:
    """Blocking loop. Caller controls shutdown via the stop Event."""
    while not stop.is_set():
        try:
            summary = tick(r)
            n = len(summary["reaped"]) + len(summary["recovered"])
            if n:
                log.info("reaper tick: reaped=%d recovered=%d",
                         len(summary["reaped"]), len(summary["recovered"]))
        except Exception:
            # Don't let one bad tick kill the reaper. Log and keep going.
            log.exception("reaper tick failed")
        stop.wait(settings.reaper_interval_sec)
