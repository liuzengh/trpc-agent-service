ALTER TABLE outbound_message
    ADD COLUMN locked_by VARCHAR(128),
    ADD COLUMN locked_until TIMESTAMPTZ;

CREATE INDEX idx_outbound_message_claimable
    ON outbound_message (next_attempt_at, locked_until, created_at)
    WHERE status IN ('pending', 'sending');
