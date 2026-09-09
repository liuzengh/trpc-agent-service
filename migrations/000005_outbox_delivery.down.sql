DROP INDEX IF EXISTS idx_outbox_delivery_ready;
ALTER TABLE outbox_messages DROP COLUMN IF EXISTS completed_at;
ALTER TABLE outbox_messages DROP COLUMN IF EXISTS sent_at;
ALTER TABLE outbox_messages DROP COLUMN IF EXISTS lease_until;
ALTER TABLE outbox_messages DROP COLUMN IF EXISTS claim_token;
ALTER TABLE outbox_messages DROP COLUMN IF EXISTS claim_owner;
