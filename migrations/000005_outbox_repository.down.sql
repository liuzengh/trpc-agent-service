DROP INDEX IF EXISTS ix_outbox_message_processing_expiry;
DROP INDEX IF EXISTS ix_outbox_message_ready;

ALTER TABLE dead_letter
    DROP CONSTRAINT IF EXISTS ck_dead_letter_last_error_size,
    DROP CONSTRAINT IF EXISTS ck_dead_letter_reason_size,
    DROP CONSTRAINT IF EXISTS ck_dead_letter_payload_size,
    DROP CONSTRAINT IF EXISTS ck_dead_letter_attempt_positive;

ALTER TABLE outbox_message
    DROP CONSTRAINT IF EXISTS ck_outbox_message_last_error_size,
    DROP CONSTRAINT IF EXISTS ck_outbox_message_locked_by_size,
    DROP CONSTRAINT IF EXISTS ck_outbox_message_dedup_key_size,
    DROP CONSTRAINT IF EXISTS ck_outbox_message_identity_size,
    DROP CONSTRAINT IF EXISTS ck_outbox_message_payload_size,
    DROP CONSTRAINT IF EXISTS ck_outbox_message_state_lock,
    DROP CONSTRAINT IF EXISTS ck_outbox_message_attempt_positive;
