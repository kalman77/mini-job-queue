"""Handler registry.

A handler is a plain sync function that takes **kwargs and does work. Workers
look up handlers by job.type. We keep this dead simple — no decorators that
do magic, no async handlers (yet). The async/sync split runs along the
producer/worker boundary on purpose; mixing it inside handlers makes
backpressure and timeouts much harder to reason about.
"""
from __future__ import annotations

from typing import Any, Callable

Handler = Callable[..., Any]

_REGISTRY: dict[str, Handler] = {}


def register(name: str) -> Callable[[Handler], Handler]:
    """Decorator: @register("send_email") def send_email(to, subject): ..."""
    def deco(fn: Handler) -> Handler:
        if name in _REGISTRY:
            raise ValueError(f"handler already registered: {name!r}")
        _REGISTRY[name] = fn
        return fn
    return deco


def get(name: str) -> Handler:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise LookupError(
            f"no handler registered for job type {name!r}. "
            f"Known types: {sorted(_REGISTRY)}"
        ) from None


def known_types() -> list[str]:
    return sorted(_REGISTRY)
