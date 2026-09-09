-- A blocked execution must not be automatically retried. Operators first
-- reconcile the uncertain execution, then use the audited replay command.
ALTER TABLE inbox_messages
    DROP CONSTRAINT IF EXISTS inbox_messages_status_check;

ALTER TABLE inbox_messages
    ADD CONSTRAINT inbox_messages_status_check
    CHECK (status IN (
        'RECEIVED','PROCESSING','RETRY_WAIT','WAITING_RECONCILIATION',
        'COMPLETED','DEAD_LETTERED'
    ));

COMMENT ON COLUMN inbox_messages.status IS
    'RECEIVED, PROCESSING, RETRY_WAIT, WAITING_RECONCILIATION, COMPLETED, or DEAD_LETTERED';

DROP INDEX IF EXISTS idx_inbox_claimable;
CREATE INDEX IF NOT EXISTS idx_inbox_claimable
    ON inbox_messages (status, next_attempt_at, lease_until, created_at)
    WHERE status IN ('RECEIVED','PROCESSING','RETRY_WAIT');
