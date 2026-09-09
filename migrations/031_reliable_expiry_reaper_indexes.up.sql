-- Expiry maintenance runs independently from ClaimInbox and ClaimOutbox.
-- These narrow partial indexes keep the bounded reaper candidate CTEs from
-- competing with the normal claim-order indexes as queues grow.
CREATE INDEX IF NOT EXISTS idx_inbox_reap_final_attempt
    ON inbox_messages (lease_until, id)
    WHERE status='PROCESSING' AND attempt_count >= max_attempts;

CREATE INDEX IF NOT EXISTS idx_inbox_reap_approval
    ON inbox_messages (approval_deadline NULLS FIRST, id)
    WHERE status='WAITING_APPROVAL';

CREATE INDEX IF NOT EXISTS idx_outbox_reap_final_attempt
    ON outbox_messages (lease_until, id)
    WHERE status='DELIVERING' AND attempt_count >= max_attempts;
