UPDATE execution_records
SET status = 'FAILED',
    error_message = CASE
        WHEN error_message = '' THEN 'downgraded_abandoned_execution'
        ELSE error_message
    END,
    completed_at = COALESCE(completed_at, now())
WHERE status = 'ABANDONED';

DROP INDEX IF EXISTS idx_execution_stale_running;

ALTER TABLE execution_records
    DROP CONSTRAINT IF EXISTS execution_records_status_check;

ALTER TABLE execution_records
    ADD CONSTRAINT execution_records_status_check
    CHECK (status IN ('RUNNING','SUCCEEDED','FAILED'));
