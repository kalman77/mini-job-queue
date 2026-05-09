"""Async producer.

Stage 2 changes vs Stage 1:
- Job rows are inserted into Postgres first (status='queued'), then the ID is
  pushed to Redis. Order matters: if the INSERT succeeds and the RPUSH fails,
  the row exists but is invisible to workers. Stage 3's reaper will sweep
  these orphans by re-pushing rows with status='queued' that aren't on a list.
- The wire envelope on Redis is now just `{"id": "..."}` — Postgres holds the
  full payload.
"""
from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, HTTPException

from core import db
from core.job import EnqueueRequest, EnqueueResponse, Job, WireEnvelope
from core.settings import queue_key, settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("producer")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Postgres pool + run migrations once on boot.
    await db.run_migrations_async()
    # Redis pool.
    app.state.redis = aioredis.from_url(settings.redis_url, decode_responses=False)
    try:
        await app.state.redis.ping()
        log.info("connected to redis at %s", settings.redis_url)
        yield
    finally:
        await app.state.redis.aclose()
        await db.aclose()


app = FastAPI(title="mini-job-queue producer", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    try:
        await app.state.redis.ping()
        async with db.aconnection() as conn, conn.cursor() as cur:
            await cur.execute("SELECT 1")
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"unhealthy: {e}")
    return {"status": "ok"}


@app.post("/jobs", response_model=EnqueueResponse, status_code=202)
async def enqueue(req: EnqueueRequest) -> EnqueueResponse:
    """Insert the row, commit, then push the ID to Redis.

    Why insert-then-push (and not the reverse): if we pushed first and crashed
    before INSERT, a worker could BLPOP an ID that has no row — every claim
    would fail forever. Insert-first means the worst case is a row visible in
    the table that no worker sees yet; the Stage 3 reaper handles that.
    """
    job = Job(type=req.type, args=req.args, max_attempts=req.max_attempts)

    async with db.aconnection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO jobs (id, type, args, status, queue, max_attempts)
            VALUES (%s, %s, %s::jsonb, 'queued', %s, %s)
            """,
            (str(job.id), job.type, json.dumps(job.args), job.queue, job.max_attempts),
        )
        await conn.commit()

    # Row is durable. Now signal workers.
    envelope = WireEnvelope(id=job.id)
    await app.state.redis.rpush(queue_key(job.queue), envelope.to_wire())
    log.info("enqueued id=%s type=%s queue=%s", job.id, job.type, job.queue)
    return EnqueueResponse(id=job.id, queue=job.queue, status="queued")
