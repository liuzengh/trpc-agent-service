-- Every production operation reads this route while holding the same
-- tenant/domain advisory gate as the migration coordinator. Terminal routes
-- remain authoritative for workers constructed before a profile switch.
CREATE TABLE data_migration_live_routes (
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id),
    domain VARCHAR(32) NOT NULL CHECK (domain IN ('session','knowledge','artifact')),
    migration_id VARCHAR(128) NOT NULL UNIQUE REFERENCES data_migrations(id),
    source_backend VARCHAR(32) NOT NULL,
    target_backend VARCHAR(32) NOT NULL,
    source_identity TEXT NOT NULL CHECK (octet_length(source_identity) BETWEEN 1 AND 2048),
    target_identity TEXT NOT NULL CHECK (octet_length(target_identity) BETWEEN 1 AND 2048),
    compatibility TEXT NOT NULL CHECK (octet_length(compatibility) BETWEEN 1 AND 4096),
    read_profile VARCHAR(128) NOT NULL,
    write_profile VARCHAR(128) NOT NULL,
    mirroring BOOLEAN NOT NULL DEFAULT TRUE,
    config_version BIGINT NOT NULL CHECK (config_version > 0),
    scan_cursor TEXT NOT NULL DEFAULT '',
    scan_done BOOLEAN NOT NULL DEFAULT FALSE,
    actor VARCHAR(256) NOT NULL,
    reason TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, domain),
    CHECK (source_identity <> target_identity)
);

-- An intent is committed before invoking the source backend. A crash or
-- ambiguous response leaves a key that must be read and reconciled before
-- another mutation or a route change can proceed.
CREATE TABLE data_migration_live_intents (
    migration_id VARCHAR(128) NOT NULL REFERENCES data_migrations(id),
    key_hash CHAR(64) NOT NULL,
    record_key TEXT NOT NULL CHECK (octet_length(record_key) BETWEEN 1 AND 4096),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (migration_id, key_hash),
    CHECK (key_hash ~ '^[0-9a-f]{64}$')
);

-- Immutable versions retain tombstones between deletion and recreation.
-- projected_at is written only after the real target read matches the hash.
CREATE TABLE data_migration_live_journal (
    sequence BIGSERIAL PRIMARY KEY,
    migration_id VARCHAR(128) NOT NULL REFERENCES data_migrations(id),
    key_hash CHAR(64) NOT NULL,
    record_key TEXT NOT NULL CHECK (octet_length(record_key) BETWEEN 1 AND 4096),
    payload BYTEA NOT NULL CHECK (octet_length(payload) <= 16777216),
    content_hash CHAR(64) NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    deleted BOOLEAN NOT NULL,
    projected_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CHECK (key_hash ~ '^[0-9a-f]{64}$'),
    CHECK (NOT deleted OR octet_length(payload) = 0)
);
CREATE INDEX idx_live_journal_key
    ON data_migration_live_journal (migration_id, key_hash, sequence DESC);
CREATE INDEX idx_live_journal_pending
    ON data_migration_live_journal (migration_id, sequence)
    WHERE projected_at IS NULL;
