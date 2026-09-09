-- Durable control state for tenant-scoped backend migrations. Payloads remain
-- in the source/target adapters; this table only stores checkpoints, leases,
-- and operator-visible decisions.
CREATE TABLE IF NOT EXISTS data_migrations (
    id VARCHAR(128) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id),
    domain VARCHAR(32) NOT NULL CHECK (domain IN ('session','memory','summary','artifact','knowledge')),
    source_profile VARCHAR(128) NOT NULL,
    target_profile VARCHAR(128) NOT NULL,
    phase VARCHAR(32) NOT NULL CHECK (phase IN (
        'PREPARE','SNAPSHOT_COPY','DUAL_WRITE','CATCH_UP','VALIDATE',
        'READ_SHADOW','CUTOVER','ROLLBACK_WINDOW','COMPLETE','ROLLED_BACK'
    )),
    paused BOOLEAN NOT NULL DEFAULT FALSE,
    cursor TEXT NOT NULL DEFAULT '',
    snapshot_watermark BIGINT NOT NULL DEFAULT 0 CHECK (snapshot_watermark >= 0),
    applied_watermark BIGINT NOT NULL DEFAULT 0 CHECK (applied_watermark >= 0),
    lease_owner VARCHAR(128),
    lease_version BIGINT NOT NULL DEFAULT 0 CHECK (lease_version >= 0),
    lease_until TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (source_profile <> target_profile)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_active_data_migration_tenant_domain
    ON data_migrations (tenant_id, domain)
    WHERE phase NOT IN ('COMPLETE','ROLLED_BACK');

CREATE INDEX IF NOT EXISTS idx_data_migration_claimable
    ON data_migrations (phase, lease_until, updated_at)
    WHERE phase NOT IN ('COMPLETE','ROLLED_BACK') AND paused=FALSE;
