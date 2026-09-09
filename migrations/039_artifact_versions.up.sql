CREATE TABLE IF NOT EXISTS artifact_versions (
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    app_name VARCHAR(128) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    session_id VARCHAR(512) NOT NULL,
    filename VARCHAR(512) NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 0),
    object_key TEXT NOT NULL UNIQUE,
    mime_type VARCHAR(255) NOT NULL,
    size_bytes BIGINT NOT NULL CHECK (size_bytes > 0 AND size_bytes <= 16777216),
    content_sha256 CHAR(64) NOT NULL CHECK (content_sha256 ~ '^[0-9a-fA-F]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    deleted_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, app_name, user_id, session_id, filename, version),
    CHECK (octet_length(app_name) > 0 AND octet_length(user_id) > 0
        AND octet_length(session_id) > 0 AND octet_length(filename) > 0)
);

CREATE INDEX IF NOT EXISTS idx_artifact_versions_latest
    ON artifact_versions (tenant_id, app_name, user_id, session_id, filename, version DESC)
    WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_artifact_versions_tombstones
    ON artifact_versions (deleted_at, tenant_id)
    WHERE deleted_at IS NOT NULL;

