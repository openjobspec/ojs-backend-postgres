-- 001_create_ojs_jobs.sql: Core jobs table

CREATE TABLE IF NOT EXISTS ojs_jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type            TEXT NOT NULL,
    queue           TEXT NOT NULL DEFAULT 'default',
    args            JSONB NOT NULL DEFAULT '[]',
    meta            JSONB NOT NULL DEFAULT '{}',
    state           TEXT NOT NULL DEFAULT 'available',
    priority        INTEGER NOT NULL DEFAULT 0,
    attempt         INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    retry_policy    JSONB,
    unique_key      TEXT,
    result          JSONB,
    error           JSONB,
    errors          JSONB[] DEFAULT ARRAY[]::JSONB[],
    scheduled_at    TIMESTAMPTZ,
    expires_at      TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    enqueued_at     TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    cancelled_at    TIMESTAMPTZ,
    discarded_at    TIMESTAMPTZ,
    visibility_timeout TIMESTAMPTZ,
    worker_id       TEXT,
    tags            TEXT[],
    timeout_ms      INTEGER,
    workflow_id     UUID,
    workflow_step   INTEGER,
    parent_results  JSONB[],
    retry_delay_ms  BIGINT,
    visibility_timeout_ms INTEGER,
    unknown_fields  JSONB
);

-- Index for dequeue: fetch available jobs ordered by priority (desc) then id (FIFO)
CREATE INDEX IF NOT EXISTS idx_ojs_jobs_dequeue
    ON ojs_jobs (queue, priority DESC, id)
    WHERE state = 'available';

-- Index for scheduled job promotion
CREATE INDEX IF NOT EXISTS idx_ojs_jobs_scheduled
    ON ojs_jobs (scheduled_at)
    WHERE state = 'scheduled';

-- Index for retry job promotion
CREATE INDEX IF NOT EXISTS idx_ojs_jobs_retryable
    ON ojs_jobs (scheduled_at)
    WHERE state = 'retryable';

-- Index for stalled job reaping
CREATE INDEX IF NOT EXISTS idx_ojs_jobs_visibility
    ON ojs_jobs (visibility_timeout)
    WHERE state = 'active';

-- Index for workflow tracking
CREATE INDEX IF NOT EXISTS idx_ojs_jobs_workflow
    ON ojs_jobs (workflow_id, workflow_step)
    WHERE workflow_id IS NOT NULL;

-- Unique constraint for job deduplication (only across non-terminal states)
CREATE UNIQUE INDEX IF NOT EXISTS idx_ojs_jobs_unique_key
    ON ojs_jobs (unique_key)
    WHERE unique_key IS NOT NULL
      AND state NOT IN ('completed', 'cancelled', 'discarded');

-- Index for dead letter listing (discarded jobs ordered by most recent)
CREATE INDEX IF NOT EXISTS idx_ojs_jobs_dead_letter
    ON ojs_jobs (completed_at DESC)
    WHERE state = 'discarded';

-- Index for job pruning (completed jobs)
CREATE INDEX IF NOT EXISTS idx_ojs_jobs_completed
    ON ojs_jobs (completed_at)
    WHERE state = 'completed';
