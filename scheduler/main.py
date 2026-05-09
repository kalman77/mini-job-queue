"""Reaper process.

Run with `python -m scheduler.main`. Same shape as worker/main.py: signal-aware
shutdown, lazy DB pool via core.db, single Redis client.

This is its own process (not a thread inside the worker) for two reasons:
- Crash isolation. A worker handler crash shouldn't take the reaper down,
  and vice versa.
- Independent scaling. You usually want exactly one reaper per cluster
  (you can run more — the SQL is concurrency-safe — but one is plenty).
  Workers scale to N. Different deployment shapes, different processes.
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import threading
from types import FrameType

import redis

from core import db
from core.settings import settings
from scheduler.reaper import run_forever

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger(f"reaper[{os.getpid()}]")

_stop = threading.Event()


def _on_signal(signum: int, _frame: FrameType | None) -> None:
    log.info("received signal %s, stopping reaper", signum)
    _stop.set()


def main() -> int:
    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    log.info("starting. heartbeat_timeout=%ss orphan_timeout=%ss interval=%ss",
             settings.heartbeat_timeout_sec, settings.orphan_timeout_sec,
             settings.reaper_interval_sec)

    db.run_migrations_sync()

    r = redis.Redis.from_url(settings.redis_url)
    try:
        r.ping()
    except redis.RedisError as e:
        log.error("redis unreachable: %s", e)
        return 1

    run_forever(r, _stop)
    log.info("clean shutdown")
    db.close_sync()
    return 0


if __name__ == "__main__":
    sys.exit(main())
