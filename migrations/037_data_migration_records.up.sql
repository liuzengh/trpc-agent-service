-- Generic payload table used by the concrete Redis -> PostgreSQL migration
-- adapter.  Domain-specific services may project these rows into their own
-- tables after cutover; the migration protocol itself only moves opaque,
-- content-addressed records.
CREATE TABLE IF NOT EXISTS data_migration_records (
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    domain VARCHAR(32) NOT NULL CHECK (domain IN ('session','memory','summary','artifact','knowledge')),
    record_key TEXT NOT NULL,
    payload BYTEA NOT NULL,
    deleted BOOLEAN NOT NULL DEFAULT FALSE,
    version BIGINT NOT NULL CHECK (version >= 1),
    content_hash CHAR(64) NOT NULL CHECK (content_hash ~ '^[0-9a-fA-F]{64}$'),
    CONSTRAINT data_migration_records_deleted_payload
        CHECK (NOT deleted OR octet_length(payload) = 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, domain, record_key)
);

CREATE INDEX IF NOT EXISTS idx_data_migration_records_version
    ON data_migration_records (tenant_id, domain, version, record_key);
