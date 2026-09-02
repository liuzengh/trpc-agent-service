-- =============================================================================
-- 005_skills.sql — Skill organizational assets (four-level model + three-state)
--
-- The four levels follow the user's clarified semantics (Skill / SkillVersion /
-- SkillScript / SkillReference):
--   * skills            : stable identity + metadata + current version pointer.
--   * skill_versions    : versioned snapshot; SKILL.md body + prompt + executor.
--   * skill_scripts     : executable scripts bundled with a version, cited by
--                         SKILL.md (run to do precise work instead of pure LLM).
--   * skill_references  : knowledge / reference files cited by a version.
--   * agent_skills      : M:N agent<->skill binding with version lock + sort.
--
-- A skill is stored in MySQL and loaded into the model context at runtime; the
-- storage form (files vs rows) is irrelevant to the model, which only reads text.
-- =============================================================================

CREATE TABLE IF NOT EXISTS skills (
    skill_id        VARCHAR(36)  NOT NULL,
    scope           ENUM('global','tenant') NOT NULL DEFAULT 'tenant',
    owner_tenant_id VARCHAR(36)  NULL                COMMENT 'NULL when scope=global',
    code            VARCHAR(64)  NOT NULL            COMMENT 'stable identity code for runtime resolution',
    name            VARCHAR(128) NOT NULL,
    description     VARCHAR(512) NULL,
    current_version INT          NOT NULL DEFAULT 0  COMMENT 'current effective version (0 = never published)',
    status          ENUM('draft','published','disabled') NOT NULL DEFAULT 'draft',
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_deleted      TINYINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (skill_id),
    UNIQUE KEY uk_skill_code (code),
    KEY idx_skill_scope_tenant (scope, owner_tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='skill main table: stable identity + current version pointer';

CREATE TABLE IF NOT EXISTS skill_versions (
    skill_id        VARCHAR(36)  NOT NULL,
    version         INT          NOT NULL,
    content_md      MEDIUMTEXT   NOT NULL            COMMENT 'SKILL.md body (front matter + body), loaded at runtime',
    checksum        VARCHAR(64)  NOT NULL            COMMENT 'sha256 of content_md, for atomic version switch',
    prompt_template MEDIUMTEXT   NULL                COMMENT 'optional prompt fragment injected into context',
    input_schema    JSON         NULL                COMMENT 'tool input schema',
    output_schema   JSON         NULL                COMMENT 'tool output schema',
    executor_type   VARCHAR(32)  NOT NULL DEFAULT 'inline'
                                                      COMMENT 'inline | python | http (extensible)',
    requirements    JSON         NULL                COMMENT 'dependency declaration for script executor',
    timeout_seconds INT          NOT NULL DEFAULT 30,
    network_enabled TINYINT      NOT NULL DEFAULT 0,
    status          ENUM('draft','published','disabled') NOT NULL DEFAULT 'draft',
    published_at    DATETIME     NULL,
    PRIMARY KEY (skill_id, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='skill version snapshot: content + prompt + executor config';

CREATE TABLE IF NOT EXISTS skill_scripts (
    script_id  VARCHAR(36)  NOT NULL,
    skill_id   VARCHAR(36)  NOT NULL,
    version    INT          NOT NULL,
    path       VARCHAR(256) NOT NULL                COMMENT 'relative path inside the skill bundle',
    lang       VARCHAR(32)  NOT NULL                COMMENT 'python | shell | js | ...',
    content    MEDIUMTEXT   NOT NULL,
    checksum   VARCHAR(64)  NOT NULL,
    PRIMARY KEY (script_id),
    KEY idx_skill_scripts_ver (skill_id, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='executable scripts bundled with a skill version, cited by SKILL.md';

CREATE TABLE IF NOT EXISTS skill_references (
    reference_id VARCHAR(36)  NOT NULL,
    skill_id     VARCHAR(36)  NOT NULL,
    version      INT          NOT NULL,
    path         VARCHAR(256) NOT NULL,
    content      MEDIUMTEXT   NOT NULL,
    checksum     VARCHAR(64)  NOT NULL,
    PRIMARY KEY (reference_id),
    KEY idx_skill_refs_ver (skill_id, version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='knowledge/reference files cited by a skill version';

CREATE TABLE IF NOT EXISTS agent_skills (
    agent_id   VARCHAR(36) NOT NULL,
    skill_id   VARCHAR(36) NOT NULL,
    version    INT         NOT NULL                COMMENT 'locked version (atomic switch independent of running sessions)',
    sort_order INT         NOT NULL DEFAULT 0,
    created_at DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (agent_id, skill_id),
    KEY idx_agent_skills_skill (skill_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='agent <-> skill binding (M:N) with version lock and sort order';
