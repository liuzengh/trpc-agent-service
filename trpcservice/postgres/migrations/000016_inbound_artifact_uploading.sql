ALTER TABLE platform.inbound_artifact
    DROP CONSTRAINT IF EXISTS inbound_artifact_status_check,
    ADD COLUMN IF NOT EXISTS upload_token TEXT,
    ADD COLUMN IF NOT EXISTS upload_lease_until TIMESTAMPTZ,
    ADD CONSTRAINT inbound_artifact_status_check CHECK (status IN ('UPLOADING', 'PENDING', 'ATTACHED', 'DELETED'));

ALTER TABLE platform.inbound_artifact
    DROP CONSTRAINT IF EXISTS inbound_artifact_upload_lease_check,
    ADD CONSTRAINT inbound_artifact_upload_lease_check CHECK (
        (upload_token IS NULL AND upload_lease_until IS NULL)
        OR (upload_token IS NOT NULL AND upload_lease_until IS NOT NULL)
    );

CREATE INDEX inbound_artifact_uploading_claim_idx
    ON platform.inbound_artifact (upload_lease_until, updated_at, external_message_id, item_no)
    WHERE status = 'UPLOADING' AND cleanup_completed_at IS NULL;
