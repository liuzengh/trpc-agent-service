ALTER TABLE queue_outbox
    ADD COLUMN locked_by VARCHAR(128),
    ADD COLUMN locked_until TIMESTAMPTZ,
    ADD COLUMN last_error TEXT;

CREATE INDEX idx_queue_outbox_claimable
    ON queue_outbox (next_attempt_at, locked_until, created_at)
    WHERE status IN ('pending', 'publishing');
