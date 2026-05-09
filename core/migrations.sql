-- mini-job-queue schema. Idempotent: safe to run on every startup.
-- Stage 2 only needs the jobs table. Stages 3-5 will add columns
-- (heartbeat, scheduled_at, priority) via additive migrations.

CREATE TABLE IF NOT EXISTS jobs (
    id            UUID         PRIMARY KEY,
    type          TEXT         NOT NULL,
    args          JSONB        NOT NULL DEFAULT '{}'::jsonb,
    status        TEXT         NOT NULL CHECK (status IN ('queued','running','done','failed')),
    queue         TEXT         NOT NULL DEFAULT 'default',
    attempts      INT          NOT NULL DEFAULT 0,
    max_attempts  INT          NOT NULL DEFAULT 3,
    enqueued_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    started_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    error         TEXT
);

-- Speeds up dashboard queries like "show me failed jobs on queue default"
-- and the worker's row-claim path. Status is the most selective column,
-- queue second, so leading with status is right.
CREATE INDEX IF NOT EXISTS jobs_status_queue_idx ON jobs (status, queue);
