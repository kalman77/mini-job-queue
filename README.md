# mini-job-queue

Async producer (FastAPI) → Redis broker → sync workers → Postgres state →
**reaper** that recovers from worker crashes and producer-side orphans.

You're now at the end of **Stage 3**. The system is self-healing:
- A killed worker no longer leaves jobs stuck in `running` — heartbeats go
  stale, the reaper requeues them.
- A producer crash between INSERT and RPUSH no longer loses jobs — the
  reaper recovers orphans.
- Each worker has a stable identity (`locked_by`), so completion writes
  can't trample a row that was reclaimed by a different worker.

## Layout

```
core/
  __init__.py
  settings.py        # env config (heartbeat, reaper intervals)
  job.py             # Job (DB row), WireEnvelope (Redis payload)
  handlers.py        # @register decorator
  db.py              # sync + async psycopg pools, migration runner
  migrations.sql     # jobs table + indexes
  heartbeat.py       # Heartbeat context manager (background thread)         [new]
producer/
  main.py            # POST /jobs writes row, RPUSHes id
worker/
  main.py            # claims with FOR UPDATE SKIP LOCKED, runs under Heartbeat
scheduler/
  __init__.py
  reaper.py          # reap_stale_running, recover_orphans, run_forever     [new]
  main.py            # long-running reaper process entrypoint               [new]
examples/
  handlers.py        # send_email, slow_task, always_fails
smoke_test.py
requirements.txt
```

## Run

```bash
# Producer (terminal 1)
uvicorn producer.main:app --reload --port 8000

# Worker (terminal 2 — run several)
python -m worker.main

# Reaper (terminal 3 — exactly one in normal use)
python -m scheduler.main
```

## Configuration

Reasonable defaults; tune via env vars when you understand the trade-offs:

| Env var | Default | Meaning |
|---|---|---|
| `HEARTBEAT_INTERVAL_SEC` | 5 | How often a running worker bumps `last_heartbeat_at` |
| `HEARTBEAT_TIMEOUT_SEC` | 30 | Beyond this, the reaper considers the worker dead. **Must be ≫ interval** — 4-6× is a good rule. |
| `REAPER_INTERVAL_SEC` | 5 | Reaper scan period |
| `ORPHAN_TIMEOUT_SEC` | 60 | Age threshold for re-pushing `queued` rows that aren't on Redis |

## Try the failure modes

**Kill a worker mid-job:**
```bash
# enqueue a 30s job
curl -X POST http://localhost:8000/jobs \
  -H "content-type: application/json" \
  -d '{"type": "slow_task", "args": {"seconds": 30}}'
# kill the worker process while it's running (kill -9 to skip clean shutdown)
# inspect the row — status will be 'running' with a stale heartbeat
docker exec -it mjq-pg psql -U mjq -d mjq -c \
  "SELECT id, status, attempts, last_heartbeat_at, locked_by FROM jobs ORDER BY enqueued_at DESC LIMIT 5;"
# wait HEARTBEAT_TIMEOUT_SEC seconds, watch the reaper log
# row goes back to 'queued', another worker picks it up, attempts=2
```

**Producer crash between INSERT and RPUSH** (harder to reproduce — easiest is
to insert a row directly with psql and skip the RPUSH; the reaper picks it
up after `ORPHAN_TIMEOUT_SEC`).

## What's still missing

- **Retries with backoff (Stage 4).** Failures stay failed. We'll push
  failed jobs back to Redis (or a `scheduled` ZSET) with exponential backoff
  and jitter, capped by `max_attempts`.
- **Priorities, scheduled jobs (Stage 5).**
- **DLQ + dashboard (Stage 6).**

## Smoke test

```bash
python smoke_test.py
```

Six tests, all run against an embedded Postgres (no Docker required):
1. Heartbeat advances `last_heartbeat_at` while a handler runs.
2. Reaper requeues a stale running row, the next worker reclaims it.
3. Reaper does NOT touch a fresh running row.
4. Orphan recovery pushes a missing id back to Redis.
5. Orphan recovery is idempotent w.r.t. Redis presence.
6. `_mark_done` won't overwrite a row owned by a different worker (fencing).
