-- 005_create_ojs_workers.sql: Workers table

CREATE TABLE IF NOT EXISTS ojs_workers (
    worker_id         TEXT PRIMARY KEY,
    last_heartbeat    TIMESTAMPTZ NOT NULL DEFAULT now(),
    directive         TEXT NOT NULL DEFAULT 'continue',
    active_jobs_count INTEGER NOT NULL DEFAULT 0
);
