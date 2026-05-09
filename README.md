# mini-job-queue

A small task queue: async producer (FastAPI) → Redis broker → sync workers.

This is **Stage 1**: fire-and-forget. No Postgres, no retries, no DLQ yet. The
goal here is to prove the pipe works end-to-end before adding durability.

## Layout

```
core/         # shared: models, redis client, queue protocol, handler registry
producer/     # FastAPI app, async, exposes POST /jobs
worker/       # sync worker process, BLPOPs and runs jobs
scheduler/    # empty for now (Stage 5)
examples/     # example handlers (send_email, etc.)
```

## Prerequisites

- Python 3.11+
- Redis running locally on :6379 (`docker run -p 6379:6379 redis:7-alpine`
  is the fastest way)

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

Terminal 1 — Redis:
```bash
docker run --rm -p 6379:6379 redis:7-alpine
```

Terminal 2 — producer:
```bash
uvicorn producer.main:app --reload --port 8000
```

Terminal 3 — worker (you can run several in parallel):
```bash
python -m worker.main
```

## Try it

```bash
# enqueue a send_email job
curl -X POST http://localhost:8000/jobs \
  -H "content-type: application/json" \
  -d '{"type": "send_email", "args": {"to": "you@example.com", "subject": "hi"}}'

# enqueue a slow job to feel the concurrency
curl -X POST http://localhost:8000/jobs \
  -H "content-type: application/json" \
  -d '{"type": "slow_task", "args": {"seconds": 3}}'

# enqueue a job that always fails (it'll just print the traceback for now)
curl -X POST http://localhost:8000/jobs \
  -H "content-type: application/json" \
  -d '{"type": "always_fails", "args": {}}'
```

You'll see the worker pick it up and run the handler. Run multiple workers and
fire 20 slow_task jobs at once — they'll spread across the workers.

## What's intentionally missing

- **Persistence.** A crash anywhere loses jobs. Stage 2 adds Postgres.
- **Retries.** A failing handler just logs the traceback. Stage 4 adds backoff.
- **Heartbeats / reaping.** A killed worker leaves its job vanished. Stage 3.
- **Priorities.** Single queue. Stage 5.
- **DLQ, scheduling, dashboard.** Later stages.
