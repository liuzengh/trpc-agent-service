DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM outbox_messages WHERE status='WAITING_RECONCILIATION'
    ) THEN
        RAISE EXCEPTION
            'cannot roll back outbox reconciliation state while blocked messages exist';
    END IF;
END
$$;

ALTER TABLE outbox_messages
    DROP CONSTRAINT IF EXISTS outbox_messages_status_check;

ALTER TABLE outbox_messages
    ADD CONSTRAINT outbox_messages_status_check
    CHECK (status IN ('REPLY_PENDING','DELIVERING','RETRY_WAIT','REPLIED','DEAD_LETTERED'));

COMMENT ON COLUMN outbox_messages.status IS
    'REPLY_PENDING, DELIVERING, RETRY_WAIT, REPLIED, or DEAD_LETTERED';

DROP INDEX IF EXISTS idx_outbox_claimable;
CREATE INDEX IF NOT EXISTS idx_outbox_claimable
    ON outbox_messages (status, next_attempt_at, lease_until, created_at)
    WHERE status IN ('REPLY_PENDING','DELIVERING','RETRY_WAIT');
