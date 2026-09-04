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
    model_config JSON         NULL                    COMMENT 'default model endpoint ref {endpoint_id, model_name}',
    data_backend JSON         NULL                    COMMENT 'per-domain backend selection: session/memory/summary/artifact/vector/audit',
    audit_policy JSON         NULL                    COMMENT 'audit policy: granularity / retention / masking rules',
    quota        JSON         NULL                    COMMENT 'tenant quota: token / tool-call / storage caps',
    created_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_deleted   TINYINT      NOT NULL DEFAULT 0      COMMENT 'soft delete: 0=alive, 1=deleted',
    PRIMARY KEY (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='tenant root entity: the first isolation boundary';

-- -----------------------------------------------------------------------------
-- Reserved (stage 33): tenant membership + RBAC tables below are PLANNED, not
-- wired. Domain model: tenant = a business team, member = a signed-in employee
-- (login user) of that team, roles = enterprise-internal authorization. They
-- stay reserved for the phase that introduces authentication/SSO (the Admin
-- API currently has no login); no production code reads or writes them yet.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tenant_members (
    tenant_id  VARCHAR(36) NOT NULL,
    user_id    VARCHAR(64) NOT NULL                COMMENT 'member external identity (IM/SSO user id)',
    role       ENUM('owner','admin','member') NOT NULL DEFAULT 'member',
    created_at DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, user_id),
    KEY idx_tenant_members_tenant (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='tenant membership: a member belongs to one or more tenants';

CREATE TABLE IF NOT EXISTS roles (
    role_id    VARCHAR(36)  NOT NULL,
    tenant_id  VARCHAR(36)  NOT NULL,
    name       VARCHAR(64)  NOT NULL,
    created_at DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (role_id),
    KEY idx_roles_tenant (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='tenant-scoped roles for RBAC';

CREATE TABLE IF NOT EXISTS role_permissions (
    role_id    VARCHAR(36)  NOT NULL,
    permission VARCHAR(128) NOT NULL                COMMENT 'e.g. agent:create / tool:grant / kb:read',
    PRIMARY KEY (role_id, permission)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='role -> permission grants';

CREATE TABLE IF NOT EXISTS member_roles (
    tenant_id VARCHAR(36) NOT NULL,
    user_id   VARCHAR(64) NOT NULL,
    role_id   VARCHAR(36) NOT NULL,
    PRIMARY KEY (tenant_id, user_id, role_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='member -> role assignments within a tenant';
