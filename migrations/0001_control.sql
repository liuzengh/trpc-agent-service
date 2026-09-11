-- 0001_control.sql — second batch, P1: MySQL as the control-plane source of truth.
--
-- This migration covers the control plane only (approved plan, "表分组"
-- 控制面 row): identity, apps, immutable revisions, profiles, channel/tool/
-- knowledge bindings. Sessions, inbox, outbox, executions, and the rest of
-- the runtime tables land in 0002_runtime.sql.
--
-- Every tenant-owned row carries tenant_id, and every uniqueness or lookup
-- index an application-facing query uses starts with tenant_id. That is not a
-- performance note — MySQL has no row-level security, so the platform's
-- tenant-scoping guarantee is exactly "a query without a tenant predicate
-- cannot reach a row it should not see". Composite foreign keys below carry
-- tenant_id for the same reason: they make a cross-tenant reference (an app
-- pointing at another tenant's revision) impossible to write, not merely
-- discouraged.
--
-- Character set is utf8mb4 throughout: IM text is user content from WeCom and
-- WeChat customer service, and emoji is not optional there.

SET NAMES utf8mb4;

CREATE TABLE IF NOT EXISTS tenants (
    tenant_id     VARCHAR(64)  NOT NULL,
    name          VARCHAR(255) NOT NULL DEFAULT '',
    status        ENUM('active', 'suspended') NOT NULL DEFAULT 'active',
    created_at    TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at    TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- Principals are platform-wide identities; membership maps them into one or
-- more tenants with a role. Only the SHA-256 of an API token is stored —
-- `token_hash`, never a token, so a database dump cannot be replayed as a
-- credential.
CREATE TABLE IF NOT EXISTS principals (
    principal_id  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    subject       VARCHAR(255)    NOT NULL COMMENT 'IM identity or service account, e.g. wecom:corp/userid',
    token_hash    CHAR(64)        NULL COMMENT 'sha256 hex of an API token; NULL for subjects that only ever authenticate through an IM channel',
    created_at    TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (principal_id),
    UNIQUE KEY uk_principals_subject (subject),
    UNIQUE KEY uk_principals_token_hash (token_hash)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS tenant_users (
    tenant_id     VARCHAR(64)        NOT NULL,
    principal_id  BIGINT UNSIGNED    NOT NULL,
    role          ENUM('admin', 'user') NOT NULL DEFAULT 'user',
    -- A platform allowlist, not an app's user whitelist: a revoked member is
    -- out of every agent of the tenant at once.
    status        ENUM('active', 'revoked') NOT NULL DEFAULT 'active',
    created_at    TIMESTAMP(6)       NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at    TIMESTAMP(6)       NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, principal_id),
    KEY idx_tenant_users_principal (principal_id),
    CONSTRAINT fk_tenant_users_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- A model profile is the tenant's view of one upstream: endpoint, model name,
-- and a secret *reference* (an env var or mounted-file name), never a key.
CREATE TABLE IF NOT EXISTS model_profiles (
    profile_id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id       VARCHAR(64)     NOT NULL,
    public_id       VARCHAR(64)     NOT NULL,
    -- Immutable: bumping a profile means inserting a new version, not UPDATE.
    -- This is what lets a session's fixed version mean something after a
    -- rotation (approved plan, "不可变修订与 RuntimePlan").
    version         INT UNSIGNED    NOT NULL DEFAULT 1,
    model_name      VARCHAR(255)    NOT NULL,
    base_url        VARCHAR(512)    NOT NULL DEFAULT '',
    api_key_ref     VARCHAR(255)    NOT NULL DEFAULT '' COMMENT 'env:VAR or file:/abs/path, resolved at execution time only',
    extra           JSON            NULL,
    created_at      TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (profile_id),
    UNIQUE KEY uk_model_profiles_version (tenant_id, public_id, version),
    CONSTRAINT fk_model_profiles_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- A backend profile pins the session/embedding/retrieval backend choice and
-- its non-secret parameters, also versioned and immutable for the same reason.
CREATE TABLE IF NOT EXISTS backend_profiles (
    profile_id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id       VARCHAR(64)     NOT NULL,
    public_id       VARCHAR(64)     NOT NULL,
    version         INT UNSIGNED    NOT NULL DEFAULT 1,
    session_backend ENUM('memory', 'redis') NOT NULL DEFAULT 'redis',
    redis_key_prefix VARCHAR(255)   NOT NULL DEFAULT '',
    session_ttl     BIGINT          NOT NULL DEFAULT 0 COMMENT 'seconds; 0 = no expiry',
    embedding_model VARCHAR(255)    NOT NULL DEFAULT '',
    embedding_dim   INT UNSIGNED    NOT NULL DEFAULT 0,
    extra           JSON            NULL,
    created_at      TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (profile_id),
    UNIQUE KEY uk_backend_profiles_version (tenant_id, public_id, version),
    CONSTRAINT fk_backend_profiles_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS agent_apps (
    app_id                  BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id               VARCHAR(64)     NOT NULL,
    public_id               VARCHAR(64)     NOT NULL,
    name                    VARCHAR(255)    NOT NULL DEFAULT '',
    -- The mutable part of an app is exactly one pointer. Publishing and
    -- rolling back are both compare-and-swap updates of this column, which is
    -- why they can be concurrent without a table-wide lock (see publish CAS
    -- in controlplane/revision.go). NULL means "never published", which a
    -- foreign key permits: MySQL skips the check when any column of a
    -- composite key is NULL.
    current_revision_id     BIGINT UNSIGNED NULL,
    created_at              TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at              TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (app_id),
    -- (tenant_id, app_id) is what 0002_runtime.sql's sessions table
    -- references: a session's tenant and app have to move together, and a
    -- foreign key can only point at a unique index.
    UNIQUE KEY uk_agent_apps_tenant_id (tenant_id, app_id),
    UNIQUE KEY uk_agent_apps_public_id (tenant_id, public_id),
    CONSTRAINT fk_agent_apps_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- agent_revisions rows are never UPDATEd. The manifest hash lets a worker
-- confirm the plan it cached is byte-identical to the one the publisher
-- validated, not merely the same row id.
CREATE TABLE IF NOT EXISTS agent_revisions (
    revision_id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id           VARCHAR(64)     NOT NULL,
    app_id              BIGINT UNSIGNED NOT NULL,
    revision_no         INT UNSIGNED    NOT NULL COMMENT 'monotonic per app, for display and ordering; not the CAS key',
    instruction         TEXT            NOT NULL,
    model_profile_id    BIGINT UNSIGNED NOT NULL,
    backend_profile_id  BIGINT UNSIGNED NOT NULL,
    -- Everything that changes how an agent behaves is either its own column
    -- here or captured in manifest_hash. max_llm_calls lives on the revision
    -- rather than on a shared runtime setting because the plan pins call
    -- ceilings per revision (approved plan, "不可变修订与 RuntimePlan").
    max_llm_calls       INT             NOT NULL DEFAULT 8,
    message_timeout_ms  INT             NOT NULL DEFAULT 120000,
    guardrails          JSON            NULL,
    tools               JSON            NULL COMMENT 'resolved tool bindings at publish time',
    knowledge_bases     JSON            NULL COMMENT 'resolved KB bindings at publish time',
    manifest_hash       CHAR(64)        NOT NULL,
    published_at        TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    published_by        BIGINT UNSIGNED NULL,
    PRIMARY KEY (revision_id),
    UNIQUE KEY uk_agent_revisions_no (app_id, revision_no),
    -- Two deliberate composite uniques, not redundancy: a foreign key can
    -- only reference a unique index, and these are the tenant-scoped shapes
    -- the app pointer and the knowledge-binding join need. Carrying
    -- tenant_id/revision_id inside the referenced key is what makes a
    -- cross-tenant pointer impossible to store rather than merely unlikely.
    UNIQUE KEY uk_agent_revisions_tenant_revision (tenant_id, revision_id),
    UNIQUE KEY uk_agent_revisions_revision_app (revision_id, app_id),
    KEY idx_agent_revisions_tenant_app (tenant_id, app_id),
    CONSTRAINT fk_agent_revisions_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- Deliberately RESTRICT rather than SET NULL: a revision that is someone's
-- current pointer must not be deletable at all, and an app row whose
-- tenant_id does not match its revision's is unwritable because the
-- referenced key carries tenant_id. SET NULL was measured not to work here —
-- it would have to null app_id too, which is part of the same composite and
-- NOT NULL — but RESTRICT is also the correct rule regardless.
ALTER TABLE agent_apps
    ADD CONSTRAINT fk_agent_apps_current_revision
    FOREIGN KEY (current_revision_id, app_id) REFERENCES agent_revisions (revision_id, app_id);

CREATE TABLE IF NOT EXISTS channel_bindings (
    binding_id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id       VARCHAR(64)     NOT NULL,
    app_id          BIGINT UNSIGNED NOT NULL,
    channel_type    ENUM('webchat', 'wecom', 'wechat_kf') NOT NULL,
    public_id       VARCHAR(64)     NOT NULL COMMENT 'the {tenant_id} segment of the callback URL path is not enough to identify a binding once one tenant has several apps',
    -- Credentials stay as references for the same reason as model_profiles:
    -- a MySQL dump must not become an IM impersonation kit.
    credential_ref  VARCHAR(255)    NOT NULL DEFAULT '',
    -- Secrets or identifiers filled through the Admin API rather than env.
    config          JSON            NULL,
    status          ENUM('active', 'disabled') NOT NULL DEFAULT 'active',
    created_at      TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at      TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (binding_id),
    -- Not redundant with the primary key: an application-facing row is
    -- addressed by (tenant_id, binding_id), and every foreign key into this
    -- table must reference a unique index that carries tenant_id, the same
    -- reason agent_revisions grew a second composite. Declared here rather
    -- than by a later ALTER, because the foreign key on channel_identities
    -- below has to find this index already in place.
    UNIQUE KEY uk_channel_bindings_id (tenant_id, binding_id),
    UNIQUE KEY uk_channel_bindings_public_id (tenant_id, channel_type, public_id),
    KEY idx_channel_bindings_app (tenant_id, app_id),
    CONSTRAINT fk_channel_bindings_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- The IM-user allowlist the approved plan's "权限" row asks for: a binding
-- only answers to people named here. external_user_id is the channel's own
-- id space (WeCom userid, WeChat external_userid), so it is only unique
-- within a binding, and the primary key says so.
CREATE TABLE IF NOT EXISTS channel_identities (
    identity_id      BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id        VARCHAR(64)     NOT NULL,
    binding_id       BIGINT UNSIGNED NOT NULL,
    external_user_id VARCHAR(255)    NOT NULL,
    display_name     VARCHAR(255)    NOT NULL DEFAULT '',
    status           ENUM('active', 'revoked') NOT NULL DEFAULT 'active',
    created_at       TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at       TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (identity_id),
    UNIQUE KEY uk_channel_identities_user (binding_id, external_user_id),
    KEY idx_channel_identities_tenant (tenant_id, binding_id),
    CONSTRAINT fk_channel_identities_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE,
    CONSTRAINT fk_channel_identities_binding FOREIGN KEY (tenant_id, binding_id) REFERENCES channel_bindings (tenant_id, binding_id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS tool_bindings (
    tool_id         BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id       VARCHAR(64)     NOT NULL,
    app_id          BIGINT UNSIGNED NOT NULL,
    name            VARCHAR(64)     NOT NULL,
    -- kind=go names a platform-registered builtin; kind=http is an
    -- admin-authored template whose URL, method and headers the model can
    -- never choose (approved plan, "受控工具").
    kind            ENUM('go', 'http') NOT NULL,
    version         INT UNSIGNED    NOT NULL DEFAULT 1,
    risk_level      ENUM('low', 'high') NOT NULL DEFAULT 'low',
    side_effect     ENUM('none', 'read', 'write') NOT NULL DEFAULT 'none',
    idempotent      TINYINT(1)      NOT NULL DEFAULT 0,
    spec            JSON            NOT NULL COMMENT 'HTTP template: method, url, allowed_headers, param map, timeouts',
    input_schema    JSON            NOT NULL,
    output_schema   JSON            NULL,
    timeout_ms      INT             NOT NULL DEFAULT 10000,
    secret_refs     JSON            NULL,
    status          ENUM('active', 'revoked') NOT NULL DEFAULT 'active',
    created_at      TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at      TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tool_id),
    UNIQUE KEY uk_tool_bindings_version (tenant_id, app_id, name, version),
    CONSTRAINT fk_tool_bindings_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- Knowledge bases belong to a tenant+app pair in this milestone; there is no
-- cross-app sharing, which is why nothing here references more than one app.
CREATE TABLE IF NOT EXISTS knowledge_bases (
    kb_id           BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    tenant_id       VARCHAR(64)     NOT NULL,
    app_id          BIGINT UNSIGNED NOT NULL,
    public_id       VARCHAR(64)     NOT NULL,
    name            VARCHAR(255)    NOT NULL DEFAULT '',
    status          ENUM('active', 'disabled') NOT NULL DEFAULT 'active',
    created_at      TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at      TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (kb_id),
    UNIQUE KEY uk_knowledge_bases_public_id (tenant_id, app_id, public_id),
    CONSTRAINT fk_knowledge_bases_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE IF NOT EXISTS knowledge_bindings (
    tenant_id       VARCHAR(64)     NOT NULL,
    revision_id     BIGINT UNSIGNED NOT NULL,
    kb_id           BIGINT UNSIGNED NOT NULL,
    created_at      TIMESTAMP(6)    NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, revision_id, kb_id),
    KEY idx_knowledge_bindings_kb (tenant_id, kb_id),
    CONSTRAINT fk_knowledge_bindings_revision FOREIGN KEY (tenant_id, revision_id) REFERENCES agent_revisions (tenant_id, revision_id) ON DELETE CASCADE,
    CONSTRAINT fk_knowledge_bindings_kb FOREIGN KEY (kb_id) REFERENCES knowledge_bases (kb_id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
