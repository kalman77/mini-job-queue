# mini-job-queue

Async producer (FastAPI) → Redis broker → sync workers, with **Postgres as
the source of truth**.

You're now at the end of **Stage 2**. Compared to Stage 1 the queue is
durable: jobs survive Redis restarts, statuses are queryable, and failures
are recorded. The Stage 1 fire-and-forget contract is gone — every job
leaves an audit trail.

## Layout

```
core/
  __init__.py
  settings.py        # env config (REDIS_URL, DATABASE_URL, ...)
  job.py             # Job (DB row), WireEnvelope (Redis payload), HTTP schemas
  handlers.py        # @register decorator, registry lookup
  db.py              # sync + async psycopg pools, migration runner          [new]
  migrations.sql     # jobs table schema                                      [new]
producer/
  __init__.py
  main.py            # FastAPI: POST /jobs writes row, then RPUSH id
worker/
  __init__.py
  main.py            # BLPOP id, claim with FOR UPDATE SKIP LOCKED, run, mark
scheduler/           # empty for now (Stage 5: delayed jobs, retry promotion)
examples/
  __init__.py
  handlers.py        # send_email, slow_task, always_fails
smoke_test.py
requirements.txt
```

## Prerequisites

- Python 3.11+
- Redis on :6379
- Postgres on :5432 with a database the app can write to

The fastest path is Docker:
```bash
docker run --rm -d -p 6379:6379 --name mjq-redis redis:7-alpine
docker run --rm -d -p 5432:5432 --name mjq-pg \
  -e POSTGRES_USER=mjq -e POSTGRES_PASSWORD=mjq -e POSTGRES_DB=mjq \
  postgres:16-alpine
```

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
# Producer (terminal 1)
uvicorn producer.main:app --reload --port 8000

# Worker (terminal 2 — run several in parallel to feel concurrency)
python -m worker.main
```

Migrations run automatically on producer or worker startup.

## Try it

```bash
curl -X POST http://localhost:8000/jobs \
  -H "content-type: application/json" \
  -d '{"type": "send_email", "args": {"to": "you@example.com", "subject": "hi"}}'
```

Then inspect the row:
```bash
docker exec -it mjq-pg psql -U mjq -d mjq -c \
  "SELECT id, type, status, attempts, started_at, completed_at, error FROM jobs ORDER BY enqueued_at DESC LIMIT 5;"
```

You'll see the row transition `queued` → `running` → `done` (or `failed` if
you used `always_fails`).

## What's still missing

- **Heartbeats / reaping (Stage 3).** Kill a worker mid-job and the row is
  stuck in `running` forever. The reaper will detect this and re-enqueue.
- **Retries with backoff (Stage 4).** A failed job stays failed. We'll push
  it back via a `scheduled_at` ZSET on Redis with exponential backoff + jitter.
- **Priorities, scheduled jobs (Stage 5).** Multiple queue lists; future-dated jobs.
- **DLQ + dashboard (Stage 6).**

## Running the smoke test

```bash
python smoke_test.py
```

This uses fakeredis + an in-memory psycopg substitute, so it doesn't need a
running Redis or Postgres. It verifies enqueue inserts a row, the row claim
under SKIP LOCKED works, and status transitions land correctly.
