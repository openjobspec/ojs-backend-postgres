-- 002_create_ojs_queues.sql: Queue metadata table

CREATE TABLE IF NOT EXISTS ojs_queues (
    name            TEXT PRIMARY KEY,
    status          TEXT NOT NULL DEFAULT 'active',
    rate_limit      INTEGER,
    completed_count BIGINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Function to auto-register a queue when a job is inserted
CREATE OR REPLACE FUNCTION ojs_auto_register_queue()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO ojs_queues (name)
    VALUES (NEW.queue)
    ON CONFLICT (name) DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to auto-register queue on job insert
DROP TRIGGER IF EXISTS trg_ojs_auto_register_queue ON ojs_jobs;
CREATE TRIGGER trg_ojs_auto_register_queue
    BEFORE INSERT ON ojs_jobs
    FOR EACH ROW
    EXECUTE FUNCTION ojs_auto_register_queue();
