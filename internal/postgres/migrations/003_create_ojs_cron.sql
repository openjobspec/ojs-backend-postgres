-- 003_create_ojs_cron.sql: Cron jobs table

CREATE TABLE IF NOT EXISTS ojs_cron_jobs (
    name            TEXT PRIMARY KEY,
    expression      TEXT NOT NULL,
    timezone        TEXT,
    overlap_policy  TEXT NOT NULL DEFAULT 'allow',
    enabled         BOOLEAN NOT NULL DEFAULT true,
    job_template    JSONB NOT NULL,
    queue           TEXT NOT NULL DEFAULT 'default',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    next_run_at     TIMESTAMPTZ,
    last_run_at     TIMESTAMPTZ,
    instance_job_id UUID
);
