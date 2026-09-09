-- Mark the durable point after which a provider call may have side effects.
-- Expired rows in this state require operator/provider reconciliation and must
-- never be automatically reclaimed by Delivery.
ALTER TABLE outbox_messages
    DROP CONSTRAINT IF EXISTS outbox_messages_status_check;

ALTER TABLE outbox_messages
    ADD CONSTRAINT outbox_messages_status_check
    CHECK (status IN ('REPLY_PENDING','DELIVERING','DISPATCH_STARTED','RETRY_WAIT','WAITING_RECONCILIATION','REPLIED','DEAD_LETTERED'));

COMMENT ON COLUMN outbox_messages.status IS
    'REPLY_PENDING, DELIVERING, DISPATCH_STARTED, RETRY_WAIT, WAITING_RECONCILIATION, REPLIED, or DEAD_LETTERED';

DROP INDEX IF EXISTS idx_outbox_claimable;
CREATE INDEX IF NOT EXISTS idx_outbox_claimable
    ON outbox_messages (status, next_attempt_at, lease_until, created_at)
    WHERE status IN ('REPLY_PENDING','DELIVERING','RETRY_WAIT');

CREATE INDEX IF NOT EXISTS idx_outbox_reap_dispatch_started
    ON outbox_messages (lease_until, id)
    WHERE status='DISPATCH_STARTED';
