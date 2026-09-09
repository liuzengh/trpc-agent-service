DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM outbox_messages WHERE status='DISPATCH_STARTED') THEN
        RAISE EXCEPTION 'cannot roll back dispatch fence while DISPATCH_STARTED rows exist';
    END IF;
END $$;

DROP INDEX IF EXISTS idx_outbox_reap_dispatch_started;
DROP INDEX IF EXISTS idx_outbox_claimable;
ALTER TABLE outbox_messages
    DROP CONSTRAINT IF EXISTS outbox_messages_status_check;
ALTER TABLE outbox_messages
    ADD CONSTRAINT outbox_messages_status_check
    CHECK (status IN ('REPLY_PENDING','DELIVERING','RETRY_WAIT','WAITING_RECONCILIATION','REPLIED','DEAD_LETTERED'));
COMMENT ON COLUMN outbox_messages.status IS
    'REPLY_PENDING, DELIVERING, RETRY_WAIT, WAITING_RECONCILIATION, REPLIED, or DEAD_LETTERED';
CREATE INDEX IF NOT EXISTS idx_outbox_claimable
    ON outbox_messages (status, next_attempt_at, lease_until, created_at)
    WHERE status IN ('REPLY_PENDING','DELIVERING','RETRY_WAIT');
