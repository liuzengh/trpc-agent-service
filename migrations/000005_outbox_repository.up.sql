UPDATE outbox_message
SET attempt = 1
WHERE attempt < 1;

UPDATE dead_letter
SET attempt = 1
WHERE attempt < 1;

ALTER TABLE outbox_message
    ADD CONSTRAINT ck_outbox_message_attempt_positive
        CHECK (attempt >= 1),
    ADD CONSTRAINT ck_outbox_message_state_lock
        CHECK (
            (status = 'processing'
                AND locked_by IS NOT NULL
                AND length(btrim(locked_by)) > 0
                AND locked_until IS NOT NULL)
            OR
            (status <> 'processing'
                AND locked_by IS NULL
                AND locked_until IS NULL)
        ),
    ADD CONSTRAINT ck_outbox_message_payload_size
        CHECK (octet_length(payload::text) <= 1048576),
    ADD CONSTRAINT ck_outbox_message_identity_size
        CHECK (
            octet_length(tenant_id) <= 128
            AND octet_length(outbox_id) <= 256
            AND octet_length(kind) <= 128
            AND octet_length(aggregate_id) <= 256
        ),
    ADD CONSTRAINT ck_outbox_message_dedup_key_size
        CHECK (dedup_key IS NULL OR octet_length(dedup_key) BETWEEN 1 AND 256),
    ADD CONSTRAINT ck_outbox_message_locked_by_size
        CHECK (locked_by IS NULL OR octet_length(locked_by) BETWEEN 1 AND 256),
    ADD CONSTRAINT ck_outbox_message_last_error_size
        CHECK (last_error IS NULL OR octet_length(last_error) BETWEEN 1 AND 128);

ALTER TABLE dead_letter
    ADD CONSTRAINT ck_dead_letter_attempt_positive
        CHECK (attempt >= 1),
    ADD CONSTRAINT ck_dead_letter_payload_size
        CHECK (octet_length(payload::text) <= 1048576),
    ADD CONSTRAINT ck_dead_letter_reason_size
        CHECK (octet_length(reason) BETWEEN 1 AND 128),
    ADD CONSTRAINT ck_dead_letter_last_error_size
        CHECK (last_error IS NULL OR octet_length(last_error) BETWEEN 1 AND 128);

CREATE INDEX ix_outbox_message_ready
    ON outbox_message (tenant_id, next_attempt_at, created_at, outbox_id)
    WHERE status IN ('pending', 'retry');

CREATE INDEX ix_outbox_message_processing_expiry
    ON outbox_message (tenant_id, locked_until, outbox_id)
    WHERE status = 'processing';
