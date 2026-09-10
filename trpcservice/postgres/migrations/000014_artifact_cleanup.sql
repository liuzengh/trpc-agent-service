ALTER TABLE platform.artifact
    ADD COLUMN config_version TEXT NOT NULL DEFAULT '',
    ADD COLUMN cleanup_attempts INTEGER NOT NULL DEFAULT 0
        CHECK (cleanup_attempts >= 0),
    ADD COLUMN cleanup_next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN cleanup_owner TEXT,
    ADD COLUMN cleanup_lease_until TIMESTAMPTZ,
    ADD COLUMN cleanup_last_error TEXT NOT NULL DEFAULT '',
    ADD COLUMN cleanup_completed_at TIMESTAMPTZ,
    ADD CONSTRAINT artifact_cleanup_lease_check CHECK (
        (cleanup_owner IS NULL AND cleanup_lease_until IS NULL)
        OR (cleanup_owner IS NOT NULL AND cleanup_lease_until IS NOT NULL)
    );

CREATE INDEX artifact_cleanup_claim_idx
    ON platform.artifact (cleanup_next_attempt_at, updated_at, artifact_id)
    WHERE object_key <> '' AND cleanup_completed_at IS NULL;
