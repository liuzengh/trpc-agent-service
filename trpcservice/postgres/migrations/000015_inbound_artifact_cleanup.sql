ALTER TABLE platform.inbound_artifact
    ADD COLUMN cleanup_attempts INTEGER NOT NULL DEFAULT 0
        CHECK (cleanup_attempts >= 0),
    ADD COLUMN cleanup_next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN cleanup_owner TEXT,
    ADD COLUMN cleanup_lease_until TIMESTAMPTZ,
    ADD COLUMN cleanup_last_error TEXT NOT NULL DEFAULT '',
    ADD COLUMN cleanup_completed_at TIMESTAMPTZ,
    ADD CONSTRAINT inbound_artifact_cleanup_lease_check CHECK (
        (cleanup_owner IS NULL AND cleanup_lease_until IS NULL)
        OR (cleanup_owner IS NOT NULL AND cleanup_lease_until IS NOT NULL)
    );

CREATE INDEX inbound_artifact_cleanup_claim_idx
    ON platform.inbound_artifact (cleanup_next_attempt_at, updated_at, external_message_id, item_no)
    WHERE cleanup_completed_at IS NULL;
