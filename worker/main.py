"""Sync worker process.

Why sync:
- Once we're executing user code, we want to be able to call ANY library —
  psycopg, requests, pandas, ffmpeg subprocess wrappers — without worrying
  whether it's async-safe. A sync worker just runs the function.
- Crash isolation: each worker is a separate process. One worker dies, the
  others keep going. (Run them under systemd or a process manager in prod.)
- Real CPU parallelism. Async gives you concurrency, not parallelism. For
  CPU-bound work N worker processes beat N async tasks.

The loop:
  1) BLPOP with a small timeout (so we can poll _shutdown between blocks).
  2) Decode the Job. If decoding fails, log loudly and drop — that payload
     is corrupt and re-trying won't help.
  3) Dispatch to the handler. Catch everything. In Stage 1 we just log; in
     Stage 4 this is where retries get scheduled.
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

from core.handlers import get as get_handler
from core.handlers import known_types
from core.job import Job
from core.settings import queue_key, settings

# Side-effect import: registers the example handlers via @register.
# In your own deploys you'd import whichever module(s) define your handlers.
import examples.handlers  # noqa: F401

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(f"worker[{os.getpid()}]")

_shutdown = False


def _on_signal(signum: int, _frame: FrameType | None) -> None:
    global _shutdown
    log.info("received signal %s, shutting down after current job", signum)
    _shutdown = True


def _process(raw: bytes) -> None:
    """Decode and run a single job. Exceptions are logged, not re-raised."""
    try:
        job = Job.from_wire(raw)
    except Exception:
        log.error("could not decode job payload, dropping. raw=%r", raw[:200])
        log.error(traceback.format_exc())
        return

    log.info("running id=%s type=%s", job.id, job.type)
    started = time.monotonic()
    try:
        handler = get_handler(job.type)
        handler(**job.args)
    except LookupError as e:
        # Unknown type — handler not registered in this worker. In Stage 4 we'd
        # send this to the DLQ; for now we just log and move on.
        log.error("id=%s %s", job.id, e)
        return
    except Exception:
        # Handler raised. Stage 4 will catch this and schedule a retry.
        elapsed = time.monotonic() - started
        log.error("id=%s FAILED after %.2fs", job.id, elapsed)
        log.error(traceback.format_exc())
        return

    elapsed = time.monotonic() - started
    log.info("id=%s done in %.2fs", job.id, elapsed)


def main() -> int:
    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    log.info("starting. queue=%s handlers=%s", settings.default_queue, known_types())
    r = redis.Redis.from_url(settings.redis_url)
    try:
        r.ping()
    except redis.RedisError as e:
        log.error("redis unreachable: %s", e)
        return 1

    qkey = queue_key(settings.default_queue)
    while not _shutdown:
        try:
            # BLPOP returns (queue_name, payload) or None on timeout.
            # The timeout is what lets us check _shutdown periodically.
            popped = r.blpop([qkey], timeout=settings.blpop_timeout)
        except redis.RedisError as e:
            # Connection blip — back off briefly and retry. redis-py reconnects on next call.
            log.warning("redis error during BLPOP: %s — retrying in 1s", e)
            time.sleep(1)
            continue

        if popped is None:
            continue  # timeout, loop and check _shutdown
        _, raw = popped
        _process(raw)

    log.info("clean shutdown")
    return 0


if __name__ == "__main__":
    sys.exit(main())
