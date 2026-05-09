"""Async producer.

Why async here:
- Submission is pure I/O (validate → LPUSH → respond). Async lets one process
  handle thousands of concurrent enqueues with one connection pool.
- This is also where you'd add auth, rate limiting, idempotency keys — all
  things that benefit from async middleware.

Why we still keep it tiny: every async footgun (event loop blocking, mixing
sync libs, forgotten awaits) pays interest forever. The producer should do
exactly one job — accept and enqueue — and nothing else.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException

from core.job import EnqueueRequest, EnqueueResponse, Job
from core.settings import queue_key, settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("producer")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # One pool per process. redis-py's async client pools internally.
    app.state.redis = aioredis.from_url(settings.redis_url, decode_responses=False)
    try:
        await app.state.redis.ping()
        log.info("connected to redis at %s", settings.redis_url)
        yield
    finally:
        await app.state.redis.aclose()


app = FastAPI(title="mini-job-queue producer", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    try:
        await app.state.redis.ping()
    except Exception as e:  # noqa: BLE001 — health endpoint must not raise
        raise HTTPException(status_code=503, detail=f"redis unreachable: {e}")
    return {"status": "ok"}


@app.post("/jobs", response_model=EnqueueResponse, status_code=202)
async def enqueue(req: EnqueueRequest) -> EnqueueResponse:
    """Accept a job and push it onto the default queue.

    202 Accepted (not 200) because the work hasn't been done — only enqueued.
    Stage 1 has no validation that req.type is a known handler; that lookup
    happens worker-side. We could add a registry on the producer too, but it
    would mean producer and worker must deploy in lockstep, which is annoying
    for a real system. Keep them loosely coupled.
    """
    job = Job(type=req.type, args=req.args)
    # RPUSH + BLPOP gives FIFO. (LPUSH + BRPOP would also work; pick one and stick.)
    await app.state.redis.rpush(queue_key(settings.default_queue), job.to_wire())
    log.info("enqueued id=%s type=%s queue=%s", job.id, job.type, settings.default_queue)
    return EnqueueResponse(id=job.id, queue=settings.default_queue)
