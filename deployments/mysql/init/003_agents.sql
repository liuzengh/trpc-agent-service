-- =============================================================================
-- 003_agents.sql — Agent & immutable versions
--
-- Borrowed from the reference and adapted:
--   * org_id -> tenant_id (agent is a TEAM assistant at tenant scope, not a
--     per-person assistant; per-user differentiation is userId + session + agent).
--   * status is our three-state machine (draft/published/disabled), not the
--     reference's binary enabled/disabled.
--   * Immutable versioning: each publish freezes a new agent_versions row; the
--     runtime_profile JSON snapshot is the single atomic unit for rollback.
--
-- (The gray column was removed together with the asset-level canary release:
-- rolling out a new PLATFORM version is a deployment concern, handled by the
-- blue/green ingress in deployments/, not by an agent row. An agent's published
-- versions are immutable and move only through Publish / Rollback.)
--
-- The earlier agent_code / `group` columns were dropped: agent_code was always
-- written as the agent id itself (so it duplicated the primary key) and `group`
-- had no reader or writer. Existing volumes need:
--   ALTER TABLE agents DROP COLUMN agent_code, DROP COLUMN `group`,
--     DROP COLUMN gray;
-- =============================================================================

CREATE TABLE IF NOT EXISTS agents (
    agent_id        VARCHAR(36)  NOT NULL,
    tenant_id       VARCHAR(36)  NOT NULL,
    name            VARCHAR(128) NOT NULL               COMMENT 'agent display name',
    description     VARCHAR(512) NULL,
    status          ENUM('draft','published','disabled') NOT NULL DEFAULT 'draft',
    current_version INT          NOT NULL DEFAULT 0     COMMENT 'current effective version number (0 = never published)',
    created_by      VARCHAR(64)  NULL                   COMMENT 'authoring member id (weak ref); NULL = pre-authorship row, treated as tenant-shared',
    visibility      ENUM('private','shared') NOT NULL DEFAULT 'private'
                                                        COMMENT 'private = author + tenant managers only; shared = tenant-readable (others read-only)',
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_deleted      TINYINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (agent_id),
    KEY idx_agent_tenant (tenant_id),
    KEY idx_agent_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='agent main table: conversation subject + skill binding subject + future memory subject';

-- Immutable runtime snapshot; old versions are kept for rollback.
-- runtime_profile JSON shape (domain/agent.RuntimeProfile, the only writer):
-- {
--   "system_prompt":  "you are ...",
--   "endpoint_id":    "e-1",                  -- model endpoint of this version
--   "tool_ids":       ["echo"],               -- mounted tools (RBAC-checked at runtime)
--   "kb_ids":         ["kb-1"],               -- knowledge bases mounted as search tools
--   "skill_ids":      ["sk-1"],               -- skills whose SKILL.md is injected
--   "approval_tool_ids": ["code-exec"]        -- tool calls that need human approval
-- }
CREATE TABLE IF NOT EXISTS agent_versions (
    agent_id        VARCHAR(36) NOT NULL,
    version         INT         NOT NULL,
    runtime_profile JSON        NOT NULL               COMMENT 'frozen snapshot: prompts + endpoint + model + tools + skills + kb',
    status          ENUM('published','rolled_back') NOT NULL DEFAULT 'published',
    published_at    DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (agent_id, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='immutable agent runtime profiles; publish appends, rollback switches current_version';
