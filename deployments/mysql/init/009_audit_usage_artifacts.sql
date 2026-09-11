-- =============================================================================
-- 009_audit_usage_artifacts.sql — Governance, metering & artifacts
--
-- audit_logs and usage_records are append-heavy and get a BIGINT auto-increment
-- `id` for physical pagination; the UUID business key remains the external id.
-- =============================================================================

CREATE TABLE IF NOT EXISTS audit_logs (
    id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    audit_id    VARCHAR(36)  NOT NULL,
    tenant_id   VARCHAR(36)  NOT NULL,
    channel     VARCHAR(32)  NOT NULL,
    user_id     VARCHAR(64)  NOT NULL,
    session_id  VARCHAR(128) NOT NULL,
    agent_name  VARCHAR(128) NULL,
    tool_name   VARCHAR(128) NULL,
    decision    VARCHAR(32)  NULL                  COMMENT 'allow | deny | approve',
    latency_ms  INT          NULL,
    error_type  VARCHAR(64)  NULL,
    cost        DECIMAL(12,6) NULL,
    trace_id    VARCHAR(64)  NOT NULL              COMMENT 'links across IM callback -> Runner -> Tool -> reply',
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_audit_id (audit_id),
    KEY idx_audit_tenant_time (tenant_id, created_at),
    KEY idx_audit_trace (trace_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='audit log: governance decisions + per-request accounting';

CREATE TABLE IF NOT EXISTS usage_records (
    id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    record_id  VARCHAR(36)  NOT NULL,
    tenant_id  VARCHAR(36)  NOT NULL,
    agent_id   VARCHAR(36)  NULL,
    member_id  VARCHAR(64)  NULL                   COMMENT 'member that triggered the usage (platform member id, or the IM user id); NULL = tenant-attributed',
    dimension  VARCHAR(32)  NOT NULL               COMMENT 'token | tool | sandbox | artifact | skill',
    amount     DECIMAL(20,6) NOT NULL,
    meta       JSON         NULL,
    created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_usage_record (record_id),
    KEY idx_usage_tenant_dim (tenant_id, dimension, created_at),
    KEY idx_usage_member_dim (tenant_id, member_id, dimension, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='usage metering per tenant for cost attribution';

-- artifacts table was removed in stage 33: artifacts are persisted by the
-- framework artifact.Service (MinIO implementation, bytes + metadata handled
-- by the service) and this metadata table had no reader or writer.
-- Re-add metadata persistence as a new migration when an artifact index is needed.

