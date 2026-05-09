"""End-to-end smoke test for Stage 3.

Adds Stage 3 coverage on top of Stage 2:
  - heartbeat updates last_heartbeat_at while a handler is running
  - reaper requeues a stale 'running' row (simulating a dead worker)
  - reaper recovers an orphaned 'queued' row that's not on Redis
  - reaper does NOT touch a fresh 'running' row (no false positives)
  - completion under a different worker_id is rejected (fencing on locked_by)
"""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

# --- patch redis ---
# pyrefly: ignore [missing-import]
import fakeredis
import redis as sync_redis
import redis.asyncio as async_redis

# Both clients must share a single backend, otherwise the producer pushes
# to one fake instance and the reaper reads from another.
_fake_server = fakeredis.FakeServer()
_fake_async = fakeredis.FakeAsyncRedis(server=_fake_server, decode_responses=False)
_fake_sync = fakeredis.FakeRedis(server=_fake_server)

async_redis.from_url = lambda *a, **kw: _fake_async  # type: ignore[assignment]
sync_redis.Redis.from_url = staticmethod(lambda *a, **kw: _fake_sync)  # type: ignore[assignment]

# --- start an ephemeral postgres ---
# pyrefly: ignore [missing-import]
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://mjq:mjq@localhost:5432/mjq",
)

# Make heartbeat tight so the test runs in seconds, not minutes.
os.environ["HEARTBEAT_INTERVAL_SEC"] = "0.2"
os.environ["HEARTBEAT_TIMEOUT_SEC"] = "1.5"
os.environ["ORPHAN_TIMEOUT_SEC"] = "1.0"

# print(f"[setup] embedded postgres at {_server.get_uri()}")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fastapi.testclient import TestClient  # noqa: E402

import examples.handlers  # noqa: E402,F401
from core import db  # noqa: E402
from core.handlers import register  # noqa: E402
from core.heartbeat import Heartbeat  # noqa: E402
from core.job import WireEnvelope  # noqa: E402
from core.settings import queue_key, settings  # noqa: E402
from producer.main import app  # noqa: E402
from scheduler import reaper  # noqa: E402
from worker.main import WORKER_ID, _claim_and_run  # noqa: E402


# A handler that takes long enough that we can observe heartbeat ticks.
@register("hb_check")
def hb_check(*, seconds: float = 1.0) -> None:
    time.sleep(seconds)


def _row(job_id: uuid.UUID) -> dict:
    with db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT id, status, attempts, started_at, completed_at,
                      last_heartbeat_at, locked_by, error
                 FROM jobs WHERE id=%s""",
            (str(job_id),),
        )
        r = cur.fetchone()
        if r is None:
            return {}
        cols = [d.name for d in cur.description]
        return dict(zip(cols, r))


def _drain_redis() -> None:
    asyncio.run(_fake_async.delete(queue_key(settings.default_queue)))


def test_heartbeat_updates_during_handler() -> None:
    """While the handler runs, last_heartbeat_at must advance."""
    job_id = uuid.uuid4()
    with db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO jobs (id, type, args, status, queue, max_attempts,
                                  started_at, last_heartbeat_at, locked_by)
               VALUES (%s, 'noop', '{}'::jsonb, 'running', 'default', 3,
                       now(), now(), %s)""",
            (str(job_id), WORKER_ID),
        )
        conn.commit()

    before = _row(job_id)["last_heartbeat_at"]
    with Heartbeat(str(job_id), WORKER_ID, interval=0.1):
        time.sleep(0.5)  # let the heartbeat tick a few times
    after = _row(job_id)["last_heartbeat_at"]
    assert after > before, f"heartbeat did not advance: before={before} after={after}"
    print(f"[ok] heartbeat advanced {(after - before).total_seconds():.2f}s during handler")


def test_reaper_requeues_stale_running_job() -> None:
    """Simulate a dead worker: a 'running' row whose heartbeat is too old."""
    _drain_redis()

    job_id = uuid.uuid4()
    with db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO jobs (id, type, args, status, queue, max_attempts,
                                  attempts, started_at, last_heartbeat_at, locked_by)
               VALUES (%s, 'send_email', '{"to":"x@y.z","subject":"hi"}'::jsonb,
                       'running', 'default', 3, 1,
                       now() - interval '60 seconds',
                       now() - interval '60 seconds',
                       'dead-worker:0:abc')""",
            (str(job_id),),
        )
        conn.commit()

    # Sanity: row exists as 'running', no Redis presence.
    assert _row(job_id)["status"] == "running"
    qlen_before = _fake_sync.llen(queue_key(settings.default_queue))
    assert qlen_before == 0

    revived = reaper.reap_stale_running(_fake_sync)
    assert str(job_id) in revived
    assert _row(job_id)["status"] == "queued"
    assert _row(job_id)["locked_by"] is None
    assert _fake_sync.llen(queue_key(settings.default_queue)) == 1
    print(f"[ok] reaper requeued stale running job id={job_id}")

    # And it's runnable again. Drain the redis push, then claim and run.
    _fake_sync.lpop(queue_key(settings.default_queue))
    _claim_and_run(str(job_id))
    final = _row(job_id)
    assert final["status"] == "done", final
    # attempts started at 1 (from the simulated dead worker), so should be 2 now.
    assert final["attempts"] == 2
    print(f"[ok] reclaimed job ran to completion, attempts={final['attempts']}")


def test_reaper_does_not_touch_fresh_running_job() -> None:
    """A row with a recent heartbeat must NOT be reaped."""
    job_id = uuid.uuid4()
    with db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO jobs (id, type, args, status, queue, max_attempts,
                                  started_at, last_heartbeat_at, locked_by)
               VALUES (%s, 'noop', '{}'::jsonb, 'running', 'default', 3,
                       now(), now(), 'live-worker:0:def')""",
            (str(job_id),),
        )
        conn.commit()

    revived = reaper.reap_stale_running(_fake_sync)
    assert str(job_id) not in revived
    assert _row(job_id)["status"] == "running"
    print("[ok] reaper left a fresh running job alone")


def test_orphan_recovery_pushes_missing_id_to_redis() -> None:
    """A 'queued' row older than orphan_timeout with no Redis presence."""
    _drain_redis()
    job_id = uuid.uuid4()
    with db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO jobs (id, type, args, status, queue, max_attempts, enqueued_at)
               VALUES (%s, 'send_email', '{"to":"a@b.c","subject":"x"}'::jsonb,
                       'queued', 'default', 3,
                       now() - interval '60 seconds')""",
            (str(job_id),),
        )
        conn.commit()

    recovered = reaper.recover_orphans(_fake_sync)
    assert str(job_id) in recovered
    assert _fake_sync.llen(queue_key(settings.default_queue)) == 1
    print(f"[ok] orphan recovered to redis, id={job_id}")


def test_orphan_recovery_skips_jobs_already_on_redis() -> None:
    """If the id IS on Redis, the orphan path must skip it (no double-push)."""
    _drain_redis()
    # Clean up any leftover orphan rows from earlier tests so this test
    # only observes its own job's behaviour.
    with db.connection() as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM jobs WHERE status='queued'")
        conn.commit()

    job_id = uuid.uuid4()
    with db.connection() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO jobs (id, type, args, status, queue, max_attempts, enqueued_at)
               VALUES (%s, 'send_email', '{"to":"a@b.c","subject":"x"}'::jsonb,
                       'queued', 'default', 3,
                       now() - interval '60 seconds')""",
            (str(job_id),),
        )
        conn.commit()

    # Pre-seed redis with the envelope.
    _fake_sync.rpush(queue_key(settings.default_queue),
                     WireEnvelope(id=job_id).to_wire())

    recovered = reaper.recover_orphans(_fake_sync)
    assert str(job_id) not in recovered
    assert _fake_sync.llen(queue_key(settings.default_queue)) == 1, "must not double-push"
    print("[ok] orphan recovery is idempotent w.r.t. redis presence")


def test_completion_only_succeeds_for_owning_worker() -> None:
    """If a job was reclaimed by a new worker, the original worker's completion
    write must not affect it (locked_by fencing)."""
    _drain_redis()
    job_id = uuid.uuid4()
    other_worker = "other-worker:0:xyz"
    with db.connection() as conn, conn.cursor() as cur:
        # Row currently owned by other_worker
        cur.execute(
            """INSERT INTO jobs (id, type, args, status, queue, max_attempts,
                                  started_at, last_heartbeat_at, locked_by)
               VALUES (%s, 'noop', '{}'::jsonb, 'running', 'default', 3,
                       now(), now(), %s)""",
            (str(job_id), other_worker),
        )
        conn.commit()

    # Our worker tries to mark it done with our WORKER_ID — must be a no-op.
    from worker.main import _mark_done
    _mark_done(str(job_id))
    row = _row(job_id)
    assert row["status"] == "running", row
    assert row["locked_by"] == other_worker
    print("[ok] _mark_done refused to overwrite another worker's row")


if __name__ == "__main__":
    try:
        db.run_migrations_sync()
        test_heartbeat_updates_during_handler()
        test_reaper_requeues_stale_running_job()
        test_reaper_does_not_touch_fresh_running_job()
        test_orphan_recovery_pushes_missing_id_to_redis()
        test_orphan_recovery_skips_jobs_already_on_redis()
        test_completion_only_succeeds_for_owning_worker()
        print("\nAll Stage 3 smoke tests passed.")
    finally:
        db.close_sync()
        #_server.cleanup()
