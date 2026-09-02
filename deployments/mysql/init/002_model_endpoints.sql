-- =============================================================================
-- 002_model_endpoints.sql — LLM endpoint pool (normalized layer)
--
-- Borrowed from the reference DDL and adapted:
--   * org_id -> tenant_id + scope (GLOBAL/TENANT), matching our A.4 decision that
--     a model endpoint is tenant-level shared infrastructure (global is optional).
--   * provider selects the wire protocol: openai | openai-compatible | anthropic |
--     gemini (see trpcservice/llm provider dispatch).
--   * api_key is NEVER plaintext: it is a secret-store reference (the reference
--     kept plaintext as an MVP concession; we go one step further to api_key_ref).
-- =============================================================================

CREATE TABLE IF NOT EXISTS model_endpoints (
    endpoint_id VARCHAR(36)   NOT NULL,
    scope       ENUM('global','tenant') NOT NULL DEFAULT 'tenant',
    tenant_id   VARCHAR(36)   NULL                   COMMENT 'NULL when scope=global',
    name        VARCHAR(128)  NOT NULL               COMMENT 'endpoint name within scope (admin label)',
    provider    VARCHAR(32)   NOT NULL DEFAULT 'openai-compatible'
                                                        COMMENT 'openai | openai-compatible | anthropic | gemini',
    base_url    VARCHAR(512)  NOT NULL               COMMENT 'LLM HTTP endpoint baseUrl',
    model_name  VARCHAR(128)  NOT NULL               COMMENT 'default model; fallback when agent does not specify',
    model_list  JSON          NULL                   COMMENT 'optional model array; NULL means [model_name]',
    temperature DECIMAL(4,2)  NULL                   COMMENT 'default sampling temperature',
    api_key_ref VARCHAR(128)  NOT NULL               COMMENT 'secret-store reference, never plaintext',
    description VARCHAR(512)  NULL,
    status      ENUM('active','disabled') NOT NULL DEFAULT 'active',
    created_by  VARCHAR(64)   NULL                   COMMENT 'creating member id (weak ref)',
    created_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_deleted  TINYINT       NOT NULL DEFAULT 0,
    PRIMARY KEY (endpoint_id),
    UNIQUE KEY uk_endpoint_scope_tenant_name (scope, tenant_id, name),
    KEY idx_endpoint_tenant (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
  COMMENT='LLM endpoint pool: agent references endpoint_id; hot-swappable infra resource';
