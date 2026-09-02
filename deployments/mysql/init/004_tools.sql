-- =============================================================================
-- 004_tools.sql — Tool registry & agent-level RBAC
--
-- risk_level drives governance (high-risk tools require a second approval via
-- Plugin/Guardrail). agent_tool_grants is the tenant-isolated tool whitelist.
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
    agent_id VARCHAR(36) NOT NULL,
    tool_id  VARCHAR(36) NOT NULL,
    PRIMARY KEY (agent_id, tool_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='agent-level tool whitelist (M:N)';
