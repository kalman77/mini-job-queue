"""Heartbeat thread.

Used as a context manager around handler execution:

    with Heartbeat(job_id, worker_id):
        handler(**args)

Why a thread and not a periodic await / signal alarm:
- The handler is sync user code. We can't assume it cooperates with an event
  loop or returns control on a schedule. A thread runs independently of what
  the handler is doing.
- We can't use SIGALRM either — many libraries install their own alarm
  handlers, and signals only fire on the main thread anyway.
- The Python GIL makes thread scheduling cooperative-ish, but `time.sleep()`
  and DB I/O both release the GIL, so the heartbeat reliably gets to run.

Why context manager and not a free-standing start/stop:
- Guarantees the thread stops even if the handler raises.
- Guarantees we issue one final heartbeat update if needed (we don't, but
  if we change the contract later, this is the place to add it).

Failure mode to remember:
- If the heartbeat thread itself can't reach the DB (network blip), the
  worker's row will go stale and the reaper will re-queue it. The handler
  will continue running and complete normally, but its result will be
  ignored — a second worker will run it. This is at-least-once delivery,
  which we documented in Stage 1. Handlers must be idempotent.
"""
from __future__ import annotations

import logging
import threading
import time

from core import db
from core.settings import settings

log = logging.getLogger("heartbeat")


class Heartbeat:
    def __init__(self, job_id: str, worker_id: str, interval: float | None = None) -> None:
        self.job_id = job_id
        self.worker_id = worker_id
        self.interval = interval if interval is not None else settings.heartbeat_interval_sec
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def __enter__(self) -> "Heartbeat":
        self._thread = threading.Thread(
            target=self._run,
            name=f"heartbeat-{self.job_id[:8]}",
            daemon=True,  # if the worker process dies, don't keep the heartbeat alive
        )
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self._stop.set()
        if self._thread is not None:
            # Don't wait forever — the thread is daemon and will be killed on
            # interpreter shutdown anyway. 2x interval is plenty for one tick.
            self._thread.join(timeout=self.interval * 2)

    def _run(self) -> None:
        # Tick immediately, then on the interval. This avoids a race where
        # a fast handler completes before the first heartbeat ever fires.
        while not self._stop.is_set():
            try:
                with db.connection() as conn, conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE jobs
                           SET last_heartbeat_at = now()
                         WHERE id = %s
                           AND status = 'running'
                           AND locked_by = %s
                        """,
                        (self.job_id, self.worker_id),
                    )
                    conn.commit()
            except Exception as e:  # noqa: BLE001 — never let heartbeat kill the worker
                log.warning("heartbeat failed for job=%s: %s", self.job_id, e)
            # wait() returns True when stopped, False on timeout — either way we loop
            self._stop.wait(self.interval)
