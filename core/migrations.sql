-- mini-job-queue schema. Idempotent: safe to run on every startup.
-- Stage 3: adds heartbeat tracking and worker identity.

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

-- Stage 3 additions. Wrapped in DO blocks so they're idempotent on
-- existing tables. (ALTER TABLE ... ADD COLUMN IF NOT EXISTS works in pg 9.6+
-- but DO blocks make the intent explicit and let us add CHECKs/indexes safely.)
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS last_heartbeat_at TIMESTAMPTZ;
ALTER TABLE jobs ADD COLUMN IF NOT EXISTS locked_by         TEXT;

CREATE INDEX IF NOT EXISTS jobs_status_queue_idx ON jobs (status, queue);

-- Speeds up the reaper scan. Partial indexes are great here: only the rows
-- the reaper actually queries get indexed, keeping the index tiny even when
-- the table grows large with completed jobs.
CREATE INDEX IF NOT EXISTS jobs_running_heartbeat_idx
    ON jobs (last_heartbeat_at)
    WHERE status = 'running';

CREATE INDEX IF NOT EXISTS jobs_queued_enqueued_idx
    ON jobs (enqueued_at)
    WHERE status = 'queued';
