-- =============================================================================
-- 003_agents.sql — Agent & immutable versions
--
-- Borrowed from the reference and adapted:
--   * org_id -> tenant_id (agent is a TEAM assistant at tenant scope, not a
--     per-person assistant; per-user differentiation is userId + session + agent).
--   * agent_code: stable code within tenant for runtime resolution.
--   * status is our three-state machine (draft/published/disabled), not the
--     reference's binary enabled/disabled.
--   * Immutable versioning: each publish freezes a new agent_versions row; the
--     runtime_profile JSON snapshot is the single atomic unit for rollback, and
--     it holds the prompt segments (identity/system + reserved soul/agents/user/
--     tool for the memory phase).
-- =============================================================================

CREATE TABLE IF NOT EXISTS agents (
    agent_id        VARCHAR(36)  NOT NULL,
    tenant_id       VARCHAR(36)  NOT NULL,
    agent_code      VARCHAR(64)  NOT NULL               COMMENT 'stable code within tenant (runtime resolution)',
    name            VARCHAR(128) NOT NULL               COMMENT 'agent display name',
    description     VARCHAR(512) NULL,
    `group`         VARCHAR(64)  NOT NULL DEFAULT ''    COMMENT 'lightweight grouping tag, not an entity',
    status          ENUM('draft','published','disabled') NOT NULL DEFAULT 'draft',
    current_version INT          NOT NULL DEFAULT 0     COMMENT 'current effective version number (0 = never published)',
    created_by      VARCHAR(64)  NULL                   COMMENT 'creating member id (weak ref)',
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_deleted      TINYINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (agent_id),
    UNIQUE KEY uk_agent_tenant_code (tenant_id, agent_code),
    KEY idx_agent_tenant (tenant_id),
    KEY idx_agent_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='agent main table: conversation subject + skill binding subject + future memory subject';

-- Immutable runtime snapshot; old versions are kept for rollback.
-- runtime_profile JSON shape:
-- {
--   "identity_prompt": "...", "system_prompt": "...",
--   "soul_prompt": null, "agents_prompt": null, "user_prompt_template": null,
--   "tool_prompt": null,                          -- reserved for memory phase
--   "endpoint_id": "...", "model_name": null, "temperature": null,
--   "tool_ids": ["..."], "skill_ids": [{"skill_id":"...","version":3}],
--   "kb_ids": ["..."]
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
