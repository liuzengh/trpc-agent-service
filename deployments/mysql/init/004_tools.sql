-- =============================================================================
-- 004_tools.sql — Tool registry & agent-level RBAC
--
-- risk_level drives governance (high-risk tools require a second approval via
-- Plugin/Guardrail). agent_tool_grants is the tenant-isolated tool whitelist:
-- tenant_id records which tenant the grant belongs to, so the grant can be
-- checked against the tenant that is about to use it (defense in depth behind
-- the API's own same-tenant validation). Existing volumes need:
--   ALTER TABLE agent_tool_grants
--     ADD COLUMN tenant_id VARCHAR(36) NOT NULL DEFAULT '',
--     ADD KEY idx_grant_tenant (tenant_id);
-- Rows with an empty tenant_id predate the column and stay tenant-agnostic
-- rather than being silently revoked.
-- =============================================================================

CREATE TABLE IF NOT EXISTS tools (
    tool_id     VARCHAR(36)  NOT NULL,
    scope       ENUM('global','tenant') NOT NULL DEFAULT 'tenant',
    tenant_id   VARCHAR(36)  NULL                   COMMENT 'NULL when scope=global',
    name        VARCHAR(128) NOT NULL,
    type        ENUM('function','mcp') NOT NULL DEFAULT 'function',
    definition  JSON         NOT NULL               COMMENT 'FunctionTool name/description/schema, or MCP server config',
    risk_level  ENUM('low','medium','high') NOT NULL DEFAULT 'low',
    created_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_deleted  TINYINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (tool_id),
    UNIQUE KEY uk_tool_scope_tenant_name (scope, tenant_id, name),
    KEY idx_tool_tenant (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='tool registry: global + tenant-scoped function/MCP tools';

CREATE TABLE IF NOT EXISTS agent_tool_grants (
    agent_id  VARCHAR(36) NOT NULL,
    tool_id   VARCHAR(36) NOT NULL,
    tenant_id VARCHAR(36) NOT NULL DEFAULT '' COMMENT 'tenant the grant belongs to; empty = legacy row (tenant-agnostic)',
    PRIMARY KEY (agent_id, tool_id),
    KEY idx_grant_tenant (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='agent-level tool whitelist (M:N) with its owning tenant';
