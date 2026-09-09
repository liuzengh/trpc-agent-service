ALTER TABLE execution_records
    DROP CONSTRAINT IF EXISTS execution_records_status_check;

ALTER TABLE execution_records
    ADD CONSTRAINT execution_records_status_check
    CHECK (status IN ('RUNNING','SUCCEEDED','FAILED','ABANDONED'));

CREATE INDEX IF NOT EXISTS idx_execution_stale_running
    ON execution_records (started_at, id)
    WHERE status = 'RUNNING';
