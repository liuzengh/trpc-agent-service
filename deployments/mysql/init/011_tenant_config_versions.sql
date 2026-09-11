-- =============================================================================
-- 011_tenant_config_versions.sql — Tenant configuration version history
--
-- Append-only configuration snapshot table. Every successful tenant update
-- (PUT /tenants) records the full post-update tenant as a JSON snapshot with
-- a per-tenant monotonically increasing version; POST /tenants/{id}/rollback
-- restores an older snapshot (which itself records a new version). This gives
-- tenants a rollback path for DataBackend / Quota / AuditPolicy changes
-- without touching the primary `tenants` row's shape.
-- =============================================================================

CREATE TABLE IF NOT EXISTS tenant_config_versions (
    id          BIGINT       NOT NULL AUTO_INCREMENT          COMMENT 'physical pk for pagination',
    tenant_id   VARCHAR(36)  NOT NULL                         COMMENT 'tenants.tenant_id (no FK: versions outlive row deletes)',
    version     INT          NOT NULL                         COMMENT 'per-tenant monotonic config version',
    config_json JSON         NOT NULL                         COMMENT 'full tenant snapshot {id,name,status,data_backend,quota,audit_policy}',
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_tenant_config_version (tenant_id, version),
    KEY idx_tenant_config_versions_tenant (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='tenant config version history for rollback';
