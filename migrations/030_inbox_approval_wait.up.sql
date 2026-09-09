-- Approval-gated Inbox work must not consume the ordinary transient retry
-- budget. The deadline makes the pause finite and prevents a permanently
-- unapproved invocation from blocking a Session FIFO indefinitely.
ALTER TABLE inbox_messages
    ADD COLUMN IF NOT EXISTS approval_deadline TIMESTAMPTZ;

ALTER TABLE inbox_messages
    DROP CONSTRAINT IF EXISTS inbox_messages_status_check;

ALTER TABLE inbox_messages
    ADD CONSTRAINT inbox_messages_status_check
    CHECK (status IN (
        'RECEIVED','PROCESSING','RETRY_WAIT','WAITING_APPROVAL',
        'WAITING_RECONCILIATION','COMPLETED','DEAD_LETTERED'
    ));

ALTER TABLE inbox_messages
    DROP CONSTRAINT IF EXISTS inbox_messages_approval_wait_check;

ALTER TABLE inbox_messages
    ADD CONSTRAINT inbox_messages_approval_wait_check
    CHECK (
        status <> 'WAITING_APPROVAL'
        OR (approval_deadline IS NOT NULL AND next_attempt_at IS NOT NULL)
    );

COMMENT ON COLUMN inbox_messages.approval_deadline IS
    'Operator approval expiry; required while status is WAITING_APPROVAL';

DROP INDEX IF EXISTS idx_inbox_claimable;
CREATE INDEX IF NOT EXISTS idx_inbox_claimable
    ON inbox_messages (status, next_attempt_at, lease_until, created_at)
    WHERE status IN ('RECEIVED','PROCESSING','RETRY_WAIT','WAITING_APPROVAL');
