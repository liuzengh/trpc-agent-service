-- =============================================================================
-- 001_tenants.sql — Tenant & Membership (RBAC)
--
-- Global conventions for this schema (applies to all files):
--   * tenant_id is a VARCHAR(36) STRING and the universal isolation key:
--     it is the tRPC-Agent-Go session AppName, the Redis key prefix, and the
--     object-storage path prefix. It is the natural PRIMARY KEY of `tenants`.
--   * Child tables denormalize tenant_id VARCHAR(36) for index-only isolation.
--   * Entity tables use a VARCHAR(36) business key as PRIMARY KEY, matching the
--     Go domain models 1:1 (no store-layer id translation).
--   * Append-heavy fact tables (chat_messages / audit_logs / usage_records) add
--     a BIGINT auto-increment `id` for physical pagination, while a UUID business
--     key remains the external identity (anti-enumeration).
--   * Core tables carry `is_deleted` (soft delete). MVP tradeoff: soft-deleted
--     business keys cannot be re-created with the same unique value (MySQL 8 has
--     no partial index); production should add a tombstone timestamp.
-- =============================================================================

CREATE TABLE IF NOT EXISTS tenants (
    tenant_id    VARCHAR(36)  NOT NULL                COMMENT 'tenant business key (UUID); also session AppName + Redis prefix',
    name         VARCHAR(128) NOT NULL                COMMENT 'tenant display name',
    status       ENUM('active','disabled') NOT NULL DEFAULT 'active',
    data_backend JSON         NULL                    COMMENT 'per-domain backend selection; domains with a second implementation: session/memory/vector/artifact/audit (summary follows the session backend and has no entry of its own)',
    audit_policy JSON         NULL                    COMMENT 'governance policy as read+written by domain/tenant: {"redact":bool,"im_allow_users":[],"tool_whitelist":[],"force_approval_tools":[]}',
    quota        JSON         NULL                    COMMENT 'tenant quota as read+written by domain/tenant: {"token_quota":int}; absent or 0 = unlimited',
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_deleted   TINYINT      NOT NULL DEFAULT 0      COMMENT 'soft delete: 0=alive, 1=deleted',
    PRIMARY KEY (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='tenant root entity: the first isolation boundary';

-- -----------------------------------------------------------------------------
-- Membership and RBAC: a login member belongs to exactly one tenant. Migration
-- 013 adds the global user_id uniqueness constraint to existing databases.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tenant_members (
    tenant_id  VARCHAR(36) NOT NULL,
    user_id    VARCHAR(64) NOT NULL                COMMENT 'member external identity (IM/SSO user id)',
    role       ENUM('owner','admin','member') NOT NULL DEFAULT 'member',
    password_hash VARCHAR(255) NOT NULL DEFAULT '' COMMENT 'bcrypt hash; never expose to clients',
    created_at DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, user_id),
    KEY idx_tenant_members_tenant (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='tenant membership: one login member belongs to exactly one tenant';

-- roles / role_permissions / member_roles were removed: authorization is
-- enforced in code (app/web/permission.go derives permissions from
-- tenant_members.role + the row-level author rule), so these tables had no
-- reader or writer. Re-add a dynamic-RBAC schema when roles become data.
--
-- tenants.model_config was removed for the same reason: no reader or writer.
-- An agent version already names its endpoint (runtime_profile.endpoint_id), so
-- a tenant-level default model would have been a second, unused source of truth.
-- Existing volumes need:
--   ALTER TABLE tenants DROP COLUMN model_config;
