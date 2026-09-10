-- MySQL 8 control-plane schema.  Every statement is intentionally
-- idempotent: MySQL DDL implicitly commits, so the migration runner records a
-- statement checkpoint and resumes forward after a partial failure.

CREATE TABLE IF NOT EXISTS tenant (
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    tenant_key VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    display_name VARCHAR(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT 'active',
    rate_limit_rpm BIGINT NULL,
    max_concurrent_executions BIGINT NULL,
    monthly_token_budget BIGINT NULL,
    monthly_spend_limit_minor BIGINT NULL,
    billing_currency VARCHAR(3) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
    audit_retention_days INT NOT NULL DEFAULT 90,
    log_masking_level VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT 'basic',
    trace_sampling_rate DOUBLE NOT NULL DEFAULT 1.0,
    default_agent_app_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
    default_backend_profile_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
    version BIGINT NOT NULL DEFAULT 1,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (tenant_id),
    UNIQUE KEY tenant_key_idx (tenant_key),
    CONSTRAINT tenant_status_ck CHECK (status IN ('active', 'suspended', 'disabled')),
    CONSTRAINT tenant_rate_limit_ck CHECK (rate_limit_rpm IS NULL OR rate_limit_rpm >= 0),
    CONSTRAINT tenant_concurrency_ck CHECK (max_concurrent_executions IS NULL OR max_concurrent_executions > 0),
    CONSTRAINT tenant_token_budget_ck CHECK (monthly_token_budget IS NULL OR monthly_token_budget >= 0),
    CONSTRAINT tenant_spend_limit_ck CHECK (monthly_spend_limit_minor IS NULL OR monthly_spend_limit_minor >= 0),
    CONSTRAINT tenant_currency_ck CHECK (monthly_spend_limit_minor IS NULL OR billing_currency IS NOT NULL),
    CONSTRAINT tenant_retention_ck CHECK (audit_retention_days > 0),
    CONSTRAINT tenant_masking_ck CHECK (log_masking_level IN ('none', 'basic', 'strict')),
    CONSTRAINT tenant_sampling_ck CHECK (trace_sampling_rate BETWEEN 0 AND 1),
    CONSTRAINT tenant_version_ck CHECK (version >= 1)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS model_profile (
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    profile_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    profile_key VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    display_name VARCHAR(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    description VARCHAR(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    schema_version INT NOT NULL,
    provider VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    model VARCHAR(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    endpoint VARCHAR(2048) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    options JSON NOT NULL,
    secret_ref VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    generation JSON NOT NULL,
    content_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    version BIGINT NOT NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (tenant_id, profile_id),
    UNIQUE KEY model_profile_key_idx (tenant_id, profile_key),
    CONSTRAINT model_profile_tenant_fk FOREIGN KEY (tenant_id) REFERENCES tenant (tenant_id),
    CONSTRAINT model_profile_status_ck CHECK (status IN ('active', 'suspended', 'disabled')),
    CONSTRAINT model_profile_version_ck CHECK (version >= 1),
    CONSTRAINT model_profile_schema_ck CHECK (schema_version = 1)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS agent_app (
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    app_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    app_key VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    display_name VARCHAR(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    description VARCHAR(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    current_revision BIGINT NULL,
    version BIGINT NOT NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (tenant_id, app_id),
    UNIQUE KEY agent_app_key_idx (tenant_id, app_key),
    CONSTRAINT agent_app_tenant_fk FOREIGN KEY (tenant_id) REFERENCES tenant (tenant_id),
    CONSTRAINT agent_app_status_ck CHECK (status IN ('draft', 'active', 'suspended', 'disabled')),
    CONSTRAINT agent_app_status_revision_ck CHECK ((status = 'draft' AND current_revision IS NULL) OR (status IN ('active', 'suspended') AND current_revision IS NOT NULL) OR status = 'disabled'),
    CONSTRAINT agent_app_version_ck CHECK (version >= 1)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS agent_app_revision (
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    app_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    revision BIGINT NOT NULL,
    state VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    draft_version BIGINT NOT NULL,
    agent_kind VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    schema_version INT NOT NULL,
    description VARCHAR(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    instruction TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    global_instruction TEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    model_profile_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    generation_config JSON NOT NULL,
    runtime_policy JSON NOT NULL,
    content_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    published_at DATETIME(6) NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (tenant_id, app_id, revision),
    CONSTRAINT agent_revision_app_fk FOREIGN KEY (tenant_id, app_id) REFERENCES agent_app (tenant_id, app_id),
    CONSTRAINT agent_revision_model_fk FOREIGN KEY (tenant_id, model_profile_id) REFERENCES model_profile (tenant_id, profile_id),
    CONSTRAINT agent_revision_state_ck CHECK (state IN ('draft', 'published')),
    CONSTRAINT agent_revision_kind_ck CHECK (agent_kind = 'llm'),
    CONSTRAINT agent_revision_schema_ck CHECK (schema_version = 1),
    CONSTRAINT agent_revision_version_ck CHECK (draft_version >= 1),
    CONSTRAINT agent_revision_digest_ck CHECK ((state = 'draft' AND content_digest IS NULL AND published_at IS NULL) OR (state = 'published' AND content_digest IS NOT NULL AND published_at IS NOT NULL))
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS agent_app_revision_tool (
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    app_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    revision BIGINT NOT NULL,
    tool_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    required BOOLEAN NOT NULL DEFAULT FALSE,
    PRIMARY KEY (tenant_id, app_id, revision, tool_id),
    CONSTRAINT agent_revision_tool_fk FOREIGN KEY (tenant_id, app_id, revision) REFERENCES agent_app_revision (tenant_id, app_id, revision) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS backend_profile (
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    profile_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    profile_key VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    display_name VARCHAR(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    description VARCHAR(2000) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    schema_version INT NOT NULL,
    content_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    version BIGINT NOT NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    PRIMARY KEY (tenant_id, profile_id),
    UNIQUE KEY backend_profile_key_idx (tenant_id, profile_key),
    CONSTRAINT backend_profile_tenant_fk FOREIGN KEY (tenant_id) REFERENCES tenant (tenant_id),
    CONSTRAINT backend_profile_status_ck CHECK (status IN ('active', 'suspended', 'disabled')),
    CONSTRAINT backend_profile_schema_ck CHECK (schema_version = 1),
    CONSTRAINT backend_profile_version_ck CHECK (version >= 1)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS backend_profile_binding (
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    profile_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    capability VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    provider VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    endpoint VARCHAR(2048) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    options JSON NOT NULL,
    secret_ref VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    PRIMARY KEY (tenant_id, profile_id, capability),
    CONSTRAINT backend_binding_profile_fk FOREIGN KEY (tenant_id, profile_id) REFERENCES backend_profile (tenant_id, profile_id) ON DELETE RESTRICT,
    CONSTRAINT backend_binding_capability_ck CHECK (capability IN ('session', 'memory', 'knowledge', 'artifact', 'audit'))
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS channel_binding (
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    binding_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    binding_key VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    channel VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    provider_account_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    public_route_key_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    app_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    secret_ref VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    protocol_config JSON NOT NULL,
    schema_version INT NOT NULL,
    status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    version BIGINT NOT NULL,
    config_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_at DATETIME(6) NOT NULL,
    updated_at DATETIME(6) NOT NULL,
    active_provider_account_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin GENERATED ALWAYS AS (CASE WHEN status = 'active' THEN provider_account_id ELSE NULL END) STORED,
    PRIMARY KEY (tenant_id, binding_id),
    UNIQUE KEY channel_binding_key_idx (tenant_id, binding_key),
    UNIQUE KEY channel_binding_active_account_idx (channel, active_provider_account_id),
    KEY channel_binding_candidate_idx (channel, public_route_key_digest, status),
    CONSTRAINT channel_binding_tenant_fk FOREIGN KEY (tenant_id) REFERENCES tenant (tenant_id),
    CONSTRAINT channel_binding_app_fk FOREIGN KEY (tenant_id, app_id) REFERENCES agent_app (tenant_id, app_id),
    CONSTRAINT channel_binding_channel_ck CHECK (channel IN ('wecom', 'telegram')),
    CONSTRAINT channel_binding_status_ck CHECK (status IN ('draft', 'active', 'suspended', 'disabled')),
    CONSTRAINT channel_binding_schema_ck CHECK (schema_version = 1),
    CONSTRAINT channel_binding_version_ck CHECK (version >= 1)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

-- The following optional identity links are added after both sides exist.
-- The migration runner checkpoints each statement and treats duplicate
-- constraint errors as already-applied on recovery.
ALTER TABLE agent_app
    ADD CONSTRAINT agent_app_current_revision_fk
    FOREIGN KEY (tenant_id, app_id, current_revision)
    REFERENCES agent_app_revision (tenant_id, app_id, revision);

ALTER TABLE tenant
    ADD CONSTRAINT tenant_default_agent_fk
    FOREIGN KEY (tenant_id, default_agent_app_id)
    REFERENCES agent_app (tenant_id, app_id);

ALTER TABLE tenant
    ADD CONSTRAINT tenant_default_backend_fk
    FOREIGN KEY (tenant_id, default_backend_profile_id)
    REFERENCES backend_profile (tenant_id, profile_id);

CREATE TABLE IF NOT EXISTS tenant_status_change_outbox (
    event_id BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    next_status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    actor_type VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    actor_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    reason VARCHAR(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_version BIGINT NOT NULL,
    next_version BIGINT NOT NULL,
    correlation_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    occurred_at DATETIME(6) NOT NULL,
    PRIMARY KEY (event_id),
    KEY tenant_status_event_idx (tenant_id, event_id),
    CONSTRAINT tenant_status_event_fk FOREIGN KEY (tenant_id) REFERENCES tenant (tenant_id),
    CONSTRAINT tenant_status_event_version_ck CHECK (next_version = previous_version + 1)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS model_profile_change_outbox (
    event_id BIGINT NOT NULL AUTO_INCREMENT,
    event_type VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    profile_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
    current_status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    current_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    actor_type VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    actor_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    reason VARCHAR(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    correlation_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_version BIGINT NOT NULL,
    next_version BIGINT NOT NULL,
    occurred_at DATETIME(6) NOT NULL,
    PRIMARY KEY (event_id),
    KEY model_profile_event_idx (tenant_id, profile_id, event_id),
    CONSTRAINT model_profile_event_fk FOREIGN KEY (tenant_id, profile_id) REFERENCES model_profile (tenant_id, profile_id),
    CONSTRAINT model_profile_event_version_ck CHECK (next_version = previous_version + 1)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS backend_profile_change_outbox (
    event_id BIGINT NOT NULL AUTO_INCREMENT,
    event_type VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    profile_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
    current_status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    current_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    actor_type VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    actor_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    reason VARCHAR(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    correlation_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_version BIGINT NOT NULL,
    next_version BIGINT NOT NULL,
    occurred_at DATETIME(6) NOT NULL,
    PRIMARY KEY (event_id),
    KEY backend_profile_event_idx (tenant_id, profile_id, event_id),
    CONSTRAINT backend_profile_event_fk FOREIGN KEY (tenant_id, profile_id) REFERENCES backend_profile (tenant_id, profile_id),
    CONSTRAINT backend_profile_event_version_ck CHECK (next_version = previous_version + 1)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS agent_app_change_outbox (
    event_id BIGINT NOT NULL AUTO_INCREMENT,
    event_type VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    app_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
    current_status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_revision BIGINT NULL,
    current_revision BIGINT NULL,
    content_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    actor_type VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    actor_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    reason VARCHAR(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    correlation_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_version BIGINT NOT NULL,
    next_version BIGINT NOT NULL,
    occurred_at DATETIME(6) NOT NULL,
    PRIMARY KEY (event_id),
    KEY agent_app_event_idx (tenant_id, app_id, event_id),
    CONSTRAINT agent_app_event_fk FOREIGN KEY (tenant_id, app_id) REFERENCES agent_app (tenant_id, app_id),
    CONSTRAINT agent_app_event_version_ck CHECK (next_version = previous_version + 1)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS channel_binding_change_outbox (
    event_id BIGINT NOT NULL AUTO_INCREMENT,
    event_type VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    binding_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL,
    current_status VARCHAR(16) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
    current_digest CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    actor_type VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    actor_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    reason VARCHAR(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    correlation_id VARCHAR(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_version BIGINT NOT NULL,
    next_version BIGINT NOT NULL,
    occurred_at DATETIME(6) NOT NULL,
    PRIMARY KEY (event_id),
    KEY channel_binding_event_idx (tenant_id, binding_id, event_id),
    CONSTRAINT channel_binding_event_fk FOREIGN KEY (tenant_id, binding_id) REFERENCES channel_binding (tenant_id, binding_id),
    CONSTRAINT channel_binding_event_version_ck CHECK (next_version = previous_version + 1)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

CREATE TABLE IF NOT EXISTS tenant_configuration_outbox (
    event_id BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
    previous_version BIGINT NOT NULL,
    next_version BIGINT NOT NULL,
    occurred_at DATETIME(6) NOT NULL,
    PRIMARY KEY (event_id),
    CONSTRAINT tenant_configuration_event_fk FOREIGN KEY (tenant_id) REFERENCES tenant (tenant_id),
    CONSTRAINT tenant_configuration_event_version_ck CHECK (next_version = previous_version + 1)
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

-- Cross-row lifecycle and identity guards mirror the PostgreSQL control-plane
-- contract. MySQL has no deferrable constraint triggers, so backend profiles are
-- created disabled, bindings are written, and the repository then moves the
-- profile to its requested status in the same transaction.

CREATE TRIGGER agent_app_revision_guard_ins
BEFORE INSERT ON agent_app
FOR EACH ROW
BEGIN
    DECLARE revision_state VARCHAR(16) DEFAULT NULL;
    IF NEW.current_revision IS NOT NULL THEN
        SELECT state INTO revision_state
        FROM agent_app_revision
        WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id AND revision = NEW.current_revision;
        IF revision_state IS NULL OR revision_state <> 'published' THEN
            SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'agent app current revision must be published';
        END IF;
    ELSEIF NEW.status IN ('active', 'suspended') THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'active or suspended agent app requires a current revision';
    END IF;
END;

CREATE TRIGGER agent_app_revision_guard_upd
BEFORE UPDATE ON agent_app
FOR EACH ROW
BEGIN
    DECLARE revision_state VARCHAR(16) DEFAULT NULL;
    IF NEW.current_revision IS NOT NULL THEN
        SELECT state INTO revision_state
        FROM agent_app_revision
        WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id AND revision = NEW.current_revision;
        IF revision_state IS NULL OR revision_state <> 'published' THEN
            SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'agent app current revision must be published';
        END IF;
    ELSEIF NEW.status IN ('active', 'suspended') THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'active or suspended agent app requires a current revision';
    END IF;
END;

CREATE TRIGGER agent_revision_immutable_upd
BEFORE UPDATE ON agent_app_revision
FOR EACH ROW
BEGIN
    IF NEW.tenant_id <> OLD.tenant_id OR NEW.app_id <> OLD.app_id OR NEW.revision <> OLD.revision THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'agent app revision identity is immutable';
    END IF;
    IF OLD.state = 'published' THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'published agent app revision is immutable';
    END IF;
END;

CREATE TRIGGER agent_revision_immutable_del
BEFORE DELETE ON agent_app_revision
FOR EACH ROW
BEGIN
    IF OLD.state = 'published' THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'published agent app revision is immutable';
    END IF;
END;

CREATE TRIGGER agent_revision_tool_guard_ins
BEFORE INSERT ON agent_app_revision_tool
FOR EACH ROW
BEGIN
    DECLARE revision_state VARCHAR(16) DEFAULT NULL;
    SELECT state INTO revision_state
    FROM agent_app_revision
    WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id AND revision = NEW.revision;
    IF revision_state = 'published' THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'published agent app tool authorization is immutable';
    END IF;
END;

CREATE TRIGGER agent_revision_tool_guard_upd
BEFORE UPDATE ON agent_app_revision_tool
FOR EACH ROW
BEGIN
    DECLARE revision_state VARCHAR(16) DEFAULT NULL;
    SELECT state INTO revision_state
    FROM agent_app_revision
    WHERE tenant_id = OLD.tenant_id AND app_id = OLD.app_id AND revision = OLD.revision;
    IF revision_state = 'published' THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'published agent app tool authorization is immutable';
    END IF;
END;

CREATE TRIGGER agent_revision_tool_guard_del
BEFORE DELETE ON agent_app_revision_tool
FOR EACH ROW
BEGIN
    DECLARE revision_state VARCHAR(16) DEFAULT NULL;
    SELECT state INTO revision_state
    FROM agent_app_revision
    WHERE tenant_id = OLD.tenant_id AND app_id = OLD.app_id AND revision = OLD.revision;
    IF revision_state = 'published' THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'published agent app tool authorization is immutable';
    END IF;
END;

CREATE TRIGGER tenant_identity_immutable_upd
BEFORE UPDATE ON tenant
FOR EACH ROW
BEGIN
    IF NEW.tenant_id <> OLD.tenant_id OR NEW.tenant_key <> OLD.tenant_key THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'tenant identity is immutable';
    END IF;
END;

CREATE TRIGGER model_profile_identity_immutable_upd
BEFORE UPDATE ON model_profile
FOR EACH ROW
BEGIN
    IF NEW.tenant_id <> OLD.tenant_id OR NEW.profile_id <> OLD.profile_id OR NEW.profile_key <> OLD.profile_key THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'model profile identity is immutable';
    END IF;
END;

CREATE TRIGGER agent_app_identity_immutable_upd
BEFORE UPDATE ON agent_app
FOR EACH ROW
BEGIN
    IF NEW.tenant_id <> OLD.tenant_id OR NEW.app_id <> OLD.app_id OR NEW.app_key <> OLD.app_key THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'agent app identity is immutable';
    END IF;
END;

CREATE TRIGGER backend_profile_insert_guard
BEFORE INSERT ON backend_profile
FOR EACH ROW
BEGIN
    IF NEW.status <> 'disabled' THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'backend profile must be created disabled before bindings';
    END IF;
END;

CREATE TRIGGER backend_profile_lifecycle_guard
BEFORE UPDATE ON backend_profile
FOR EACH ROW
BEGIN
    IF NEW.status <> 'disabled' AND NOT EXISTS (
        SELECT 1 FROM backend_profile_binding
        WHERE tenant_id = NEW.tenant_id AND profile_id = NEW.profile_id
    ) THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'non-disabled backend profile requires a binding';
    END IF;
    IF NEW.status = 'active' AND NOT EXISTS (
        SELECT 1 FROM backend_profile_binding
        WHERE tenant_id = NEW.tenant_id AND profile_id = NEW.profile_id AND capability = 'session'
    ) THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'active backend profile requires a session binding';
    END IF;
END;

CREATE TRIGGER backend_profile_identity_guard
BEFORE UPDATE ON backend_profile
FOR EACH ROW
BEGIN
    IF NEW.tenant_id <> OLD.tenant_id OR NEW.profile_id <> OLD.profile_id OR NEW.profile_key <> OLD.profile_key THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'backend profile identity is immutable';
    END IF;
END;

CREATE TRIGGER backend_binding_identity_guard
BEFORE UPDATE ON backend_profile_binding
FOR EACH ROW
BEGIN
    IF NEW.tenant_id <> OLD.tenant_id OR NEW.profile_id <> OLD.profile_id OR NEW.capability <> OLD.capability THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'backend profile binding identity is immutable';
    END IF;
END;

CREATE TRIGGER backend_binding_delete_guard
AFTER DELETE ON backend_profile_binding
FOR EACH ROW
BEGIN
    DECLARE profile_status VARCHAR(16) DEFAULT NULL;
    SELECT status INTO profile_status
    FROM backend_profile
    WHERE tenant_id = OLD.tenant_id AND profile_id = OLD.profile_id;
    IF profile_status IS NOT NULL AND profile_status <> 'disabled' AND NOT EXISTS (
        SELECT 1 FROM backend_profile_binding
        WHERE tenant_id = OLD.tenant_id AND profile_id = OLD.profile_id
    ) THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'non-disabled backend profile requires a binding';
    END IF;
    IF profile_status = 'active' AND NOT EXISTS (
        SELECT 1 FROM backend_profile_binding
        WHERE tenant_id = OLD.tenant_id AND profile_id = OLD.profile_id AND capability = 'session'
    ) THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'active backend profile requires a session binding';
    END IF;
END;

CREATE TRIGGER channel_binding_identity_guard
BEFORE UPDATE ON channel_binding
FOR EACH ROW
BEGIN
    IF NEW.tenant_id <> OLD.tenant_id OR NEW.binding_id <> OLD.binding_id OR NEW.binding_key <> OLD.binding_key OR NEW.channel <> OLD.channel THEN
        SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 3819, MESSAGE_TEXT = 'channel binding identity is immutable';
    END IF;
END;
