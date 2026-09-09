ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS claim_owner TEXT;
ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS claim_token TEXT;
ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;
ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS sent_at TIMESTAMPTZ;
ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_outbox_delivery_ready
  ON outbox_messages (status, retry_at, lease_until, created_at)
  WHERE status IN ('pending', 'retry', 'claimed', 'sending');
