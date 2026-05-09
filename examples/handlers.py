"""Example job handlers. Importing this module registers them.

These are deliberately tiny so the queue's behavior is what you observe,
not the handler's. Add real handlers as you extend the system.
"""
from __future__ import annotations

import logging
import time

from core.handlers import register

log = logging.getLogger("examples")


@register("send_email")
def send_email(*, to: str, subject: str, body: str = "") -> None:
    # Pretend to send. In a real system you'd call SMTP / SES / Resend here.
    log.info("send_email -> to=%s subject=%r body_len=%d", to, subject, len(body))
    time.sleep(0.2)


@register("slow_task")
def slow_task(*, seconds: float = 1.0) -> None:
    """Useful for watching multiple workers spread load."""
    log.info("slow_task starting, sleeping %.2fs", seconds)
    time.sleep(seconds)
    log.info("slow_task done")


@register("always_fails")
def always_fails(**_: object) -> None:
    """Used to sanity-check error handling. In Stage 4 this drives retry tests."""
    raise RuntimeError("this handler always fails on purpose")
