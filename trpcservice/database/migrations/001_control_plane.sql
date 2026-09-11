CREATE TABLE tenant (
    tenant_id VARCHAR(64) PRIMARY KEY,
    display_name VARCHAR(255) NOT NULL,
    status VARCHAR(32) NOT NULL CHECK (status IN ('active', 'suspended', 'disabled')),
    region VARCHAR(64) NOT NULL,
    quota_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    audit_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    secret_namespace VARCHAR(255) NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE agent_app (
    app_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    status VARCHAR(32) NOT NULL CHECK (status IN ('active', 'disabled')),
    stable_revision_id VARCHAR(64),
    rollout_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name),
    UNIQUE (tenant_id, app_id)
);

CREATE TABLE agent_revision (
    revision_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    app_id VARCHAR(64) NOT NULL,
    revision_no BIGINT NOT NULL,
    agent_type VARCHAR(64) NOT NULL,
    agent_config JSONB NOT NULL,
    model_config JSONB NOT NULL,
    tool_policy JSONB NOT NULL DEFAULT '{}'::jsonb,
    knowledge_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    memory_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    guardrail_config JSONB NOT NULL DEFAULT '{}'::jsonb,
    checksum VARCHAR(128) NOT NULL,
    created_by VARCHAR(128) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (app_id, revision_no),
    UNIQUE (tenant_id, revision_id),
    FOREIGN KEY (tenant_id, app_id) REFERENCES agent_app(tenant_id, app_id)
);

ALTER TABLE agent_app
    ADD CONSTRAINT fk_agent_app_stable_revision
    FOREIGN KEY (tenant_id, stable_revision_id)
    REFERENCES agent_revision(tenant_id, revision_id)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE backend_binding (
    binding_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    app_id VARCHAR(64),
    resource_type VARCHAR(32) NOT NULL,
    backend_type VARCHAR(32) NOT NULL,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    secret_ref VARCHAR(512),
    isolation_level VARCHAR(32) NOT NULL DEFAULT 'shared',
    migration_state VARCHAR(32) NOT NULL DEFAULT 'active',
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (tenant_id, app_id) REFERENCES agent_app(tenant_id, app_id)
);

CREATE UNIQUE INDEX idx_backend_binding_app
    ON backend_binding (tenant_id, app_id, resource_type)
    WHERE app_id IS NOT NULL;

CREATE UNIQUE INDEX idx_backend_binding_tenant_default
    ON backend_binding (tenant_id, resource_type)
    WHERE app_id IS NULL;

CREATE TABLE channel_binding (
    channel_binding_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    app_id VARCHAR(64) NOT NULL,
    channel_type VARCHAR(32) NOT NULL,
    account_id VARCHAR(255) NOT NULL,
    callback_key VARCHAR(128) NOT NULL UNIQUE,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    secret_ref VARCHAR(512) NOT NULL,
    status VARCHAR(32) NOT NULL CHECK (status IN ('active', 'disabled')),
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, channel_type, account_id),
    FOREIGN KEY (tenant_id, app_id) REFERENCES agent_app(tenant_id, app_id)
);

CREATE TABLE external_identity (
    identity_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    channel_binding_id VARCHAR(64) NOT NULL REFERENCES channel_binding(channel_binding_id),
    external_user_id VARCHAR(255) NOT NULL,
    canonical_user_id VARCHAR(128) NOT NULL,
    profile JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (channel_binding_id, external_user_id)
);

CREATE TABLE conversation (
    conversation_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    app_id VARCHAR(64) NOT NULL,
    channel_binding_id VARCHAR(64) REFERENCES channel_binding(channel_binding_id),
    session_id VARCHAR(255) NOT NULL,
    runtime_user_id VARCHAR(255) NOT NULL,
    chat_type VARCHAR(32) NOT NULL,
    external_chat_id VARCHAR(255),
    external_thread_id VARCHAR(255),
    pinned_revision_id VARCHAR(64),
    last_turn_seq BIGINT NOT NULL DEFAULT 0,
    last_event_seq BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, app_id, runtime_user_id, session_id),
    FOREIGN KEY (tenant_id, app_id) REFERENCES agent_app(tenant_id, app_id),
    FOREIGN KEY (tenant_id, pinned_revision_id) REFERENCES agent_revision(tenant_id, revision_id)
);

CREATE TABLE inbound_message (
    inbound_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    app_id VARCHAR(64) NOT NULL,
    channel_binding_id VARCHAR(64) REFERENCES channel_binding(channel_binding_id),
    external_message_id VARCHAR(512) NOT NULL,
    request_id VARCHAR(128) NOT NULL,
    conversation_id VARCHAR(64) NOT NULL REFERENCES conversation(conversation_id),
    actor_user_id VARCHAR(255) NOT NULL,
    message_type VARCHAR(32) NOT NULL,
    payload JSONB NOT NULL,
    payload_hash VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at TIMESTAMPTZ,
    UNIQUE (channel_binding_id, external_message_id),
    UNIQUE (tenant_id, request_id),
    FOREIGN KEY (tenant_id, app_id) REFERENCES agent_app(tenant_id, app_id)
);

CREATE TABLE agent_run (
    request_id VARCHAR(128) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    app_id VARCHAR(64) NOT NULL,
    revision_id VARCHAR(64) NOT NULL,
    conversation_id VARCHAR(64) NOT NULL REFERENCES conversation(conversation_id),
    turn_seq BIGINT NOT NULL,
    fencing_token BIGINT NOT NULL DEFAULT 0,
    status VARCHAR(32) NOT NULL,
    worker_id VARCHAR(128),
    model_name VARCHAR(255),
    prompt_tokens BIGINT NOT NULL DEFAULT 0,
    completion_tokens BIGINT NOT NULL DEFAULT 0,
    cost NUMERIC(20, 8) NOT NULL DEFAULT 0,
    error_type VARCHAR(128),
    error_message TEXT,
    trace_id VARCHAR(64),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    cancel_requested_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (conversation_id, turn_seq),
    FOREIGN KEY (tenant_id, app_id) REFERENCES agent_app(tenant_id, app_id),
    FOREIGN KEY (tenant_id, revision_id) REFERENCES agent_revision(tenant_id, revision_id)
);

CREATE TABLE outbound_message (
    outbound_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    app_id VARCHAR(64) NOT NULL,
    channel_binding_id VARCHAR(64) REFERENCES channel_binding(channel_binding_id),
    request_id VARCHAR(128) NOT NULL REFERENCES agent_run(request_id),
    conversation_id VARCHAR(64) NOT NULL REFERENCES conversation(conversation_id),
    payload JSONB NOT NULL,
    status VARCHAR(32) NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ,
    provider_message_id VARCHAR(512),
    last_error_type VARCHAR(128),
    last_error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at TIMESTAMPTZ,
    UNIQUE (request_id)
);

CREATE TABLE queue_outbox (
    outbox_id VARCHAR(64) PRIMARY KEY,
    topic VARCHAR(128) NOT NULL,
    partition_key VARCHAR(512) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX idx_queue_outbox_pending
    ON queue_outbox (next_attempt_at, created_at)
    WHERE status = 'pending';

CREATE TABLE audit_log (
    audit_id VARCHAR(64) PRIMARY KEY,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    tenant_id VARCHAR(64) NOT NULL,
    channel VARCHAR(32),
    channel_binding_id VARCHAR(64),
    user_id VARCHAR(255),
    session_id VARCHAR(255),
    message_id VARCHAR(512),
    request_id VARCHAR(128),
    trace_id VARCHAR(64),
    agent_name VARCHAR(255),
    revision_id VARCHAR(64),
    tool_name VARCHAR(255),
    decision VARCHAR(64) NOT NULL,
    latency_ms BIGINT NOT NULL DEFAULT 0,
    error_type VARCHAR(128),
    cost NUMERIC(20, 8) NOT NULL DEFAULT 0,
    details JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX idx_audit_log_tenant_time
    ON audit_log (tenant_id, occurred_at DESC);

CREATE INDEX idx_audit_log_request
    ON audit_log (tenant_id, request_id)
    WHERE request_id IS NOT NULL;
