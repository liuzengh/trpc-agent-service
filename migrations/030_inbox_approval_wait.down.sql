DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM inbox_messages WHERE status='WAITING_APPROVAL'
    ) THEN
        RAISE EXCEPTION
            'cannot roll back inbox approval wait while waiting messages exist';
    END IF;
END
$$;

ALTER TABLE inbox_messages
    DROP CONSTRAINT IF EXISTS inbox_messages_approval_wait_check;

ALTER TABLE inbox_messages
    DROP CONSTRAINT IF EXISTS inbox_messages_status_check;

ALTER TABLE inbox_messages
    ADD CONSTRAINT inbox_messages_status_check
    CHECK (status IN (
        'RECEIVED','PROCESSING','RETRY_WAIT','WAITING_RECONCILIATION',
        'COMPLETED','DEAD_LETTERED'
    ));

ALTER TABLE inbox_messages
    DROP COLUMN IF EXISTS approval_deadline;

DROP INDEX IF EXISTS idx_inbox_claimable;
CREATE INDEX IF NOT EXISTS idx_inbox_claimable
    ON inbox_messages (status, next_attempt_at, lease_until, created_at)
    WHERE status IN ('RECEIVED','PROCESSING','RETRY_WAIT');
