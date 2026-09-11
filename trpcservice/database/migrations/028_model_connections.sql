-- Model credentials are encrypted by the application with a separately held
-- deployment key. A connection is immutable; new credentials mean a new ID.
CREATE TABLE model_connection (
    tenant_id TEXT NOT NULL REFERENCES tenant(tenant_id),
    connection_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    model_name TEXT NOT NULL,
    base_url TEXT NOT NULL,
    encrypted_key BYTEA NOT NULL,
    key_id TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, connection_id),
    CHECK (octet_length(encrypted_key) BETWEEN 29 AND 16412)
);
REVOKE ALL ON model_connection FROM PUBLIC;
