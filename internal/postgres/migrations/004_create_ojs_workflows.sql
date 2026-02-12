-- 004_create_ojs_workflows.sql: Workflows table

CREATE TABLE IF NOT EXISTS ojs_workflows (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            TEXT,
    type            TEXT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'running',
    total           INTEGER NOT NULL DEFAULT 0,
    completed       INTEGER NOT NULL DEFAULT 0,
    failed          INTEGER NOT NULL DEFAULT 0,
    job_defs        JSONB,
    callbacks       JSONB,
    results         JSONB DEFAULT '{}',
    job_ids         TEXT[] DEFAULT ARRAY[]::TEXT[],
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ
);
