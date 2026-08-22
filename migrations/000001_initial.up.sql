CREATE TABLE tenant (
    tenant_id text PRIMARY KEY,
    name text NOT NULL,
    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended', 'deleted')),
    config_version bigint NOT NULL DEFAULT 0 CHECK (config_version >= 0),
    default_agent_app_id text,
    backend_config jsonb NOT NULL DEFAULT '{}'::jsonb,
    policy_config jsonb NOT NULL DEFAULT '{}'::jsonb,
    budget_cents bigint NOT NULL DEFAULT 0 CHECK (budget_cents >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE agent_app (
    tenant_id text NOT NULL,
    agent_app_id text NOT NULL,
    name text NOT NULL,
    status text NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'disabled', 'archived')),
    model_config_ref text,
    system_prompt text,
    system_prompt_ref text,
    tool_policy jsonb NOT NULL DEFAULT '{}'::jsonb,
    guardrail_ref text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, agent_app_id),
    FOREIGN KEY (tenant_id) REFERENCES tenant (tenant_id) ON DELETE CASCADE
);

CREATE TABLE channel_binding (
    tenant_id text NOT NULL,
    channel text NOT NULL,
    binding_id text NOT NULL,
    external_app_id text NOT NULL,
    secret_ref text,
    enabled boolean NOT NULL DEFAULT true,
    config jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, channel, binding_id),
    UNIQUE (tenant_id, channel, external_app_id),
    FOREIGN KEY (tenant_id) REFERENCES tenant (tenant_id) ON DELETE CASCADE
);

CREATE TABLE user_identity (
    tenant_id text NOT NULL,
    identity_id text NOT NULL,
    channel text NOT NULL,
    binding_id text NOT NULL,
    external_user_id text NOT NULL,
    display_name text,
    attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
    first_seen_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, identity_id),
    UNIQUE (tenant_id, channel, binding_id, external_user_id),
    FOREIGN KEY (tenant_id, channel, binding_id)
        REFERENCES channel_binding (tenant_id, channel, binding_id) ON DELETE RESTRICT
);

CREATE TABLE session (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    agent_app_id text NOT NULL,
    agent_version bigint NOT NULL CHECK (agent_version >= 1),
    channel text NOT NULL,
    binding_id text NOT NULL,
    external_chat text NOT NULL,
    external_user text NOT NULL,
    state text NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'paused', 'completed', 'archived')),
    state_version bigint NOT NULL DEFAULT 1 CHECK (state_version >= 1),
    summary_version bigint NOT NULL DEFAULT 0 CHECK (summary_version >= 0),
    last_event_seq bigint NOT NULL DEFAULT 0 CHECK (last_event_seq >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, session_id),
    FOREIGN KEY (tenant_id, agent_app_id) REFERENCES agent_app (tenant_id, agent_app_id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, channel, binding_id)
        REFERENCES channel_binding (tenant_id, channel, binding_id) ON DELETE RESTRICT
);

CREATE TABLE session_event (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    event_id text NOT NULL,
    sequence bigint NOT NULL CHECK (sequence >= 1),
    event_type text NOT NULL,
    role text,
    message_id text,
    execution_id text,
    parent_event_id text,
    attempt integer NOT NULL DEFAULT 1 CHECK (attempt >= 1),
    trace_id text,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, event_id),
    UNIQUE (tenant_id, session_id, sequence),
    FOREIGN KEY (tenant_id, session_id) REFERENCES session (tenant_id, session_id) ON DELETE CASCADE
);

CREATE TABLE message_dedup (
    tenant_id text NOT NULL,
    channel text NOT NULL,
    binding_id text NOT NULL,
    external_message_id text NOT NULL,
    status text NOT NULL DEFAULT 'acquired' CHECK (status IN ('acquired', 'in_flight', 'completed')),
    owner_id text,
    attempt integer NOT NULL DEFAULT 1 CHECK (attempt >= 1),
    fence_token bigint NOT NULL DEFAULT 0 CHECK (fence_token >= 0),
    response_ref text,
    claimed_at timestamptz,
    expires_at timestamptz,
    error_message text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, channel, binding_id, external_message_id),
    FOREIGN KEY (tenant_id, channel, binding_id)
        REFERENCES channel_binding (tenant_id, channel, binding_id) ON DELETE CASCADE
);

CREATE TABLE memory (
    tenant_id text NOT NULL,
    memory_id text NOT NULL,
    scope text NOT NULL CHECK (scope IN ('session', 'user', 'tenant')),
    scope_id text NOT NULL,
    session_id text,
    kind text NOT NULL,
    content text NOT NULL DEFAULT '',
    vector_ref text,
    version bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
    source_seq bigint NOT NULL DEFAULT 0 CHECK (source_seq >= 0),
    deleted boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, memory_id),
    FOREIGN KEY (tenant_id, session_id) REFERENCES session (tenant_id, session_id) ON DELETE RESTRICT,
    CHECK (scope <> 'session' OR session_id IS NOT NULL),
    CHECK (scope <> 'session' OR scope_id = session_id),
    CHECK (deleted OR length(btrim(content)) > 0)
);

CREATE TABLE summary (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    version bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
    covered_seq bigint NOT NULL DEFAULT 0 CHECK (covered_seq >= 0),
    content text NOT NULL,
    token_estimate bigint NOT NULL DEFAULT 0 CHECK (token_estimate >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, session_id),
    FOREIGN KEY (tenant_id, session_id) REFERENCES session (tenant_id, session_id) ON DELETE CASCADE
);

CREATE TABLE artifact (
    tenant_id text NOT NULL,
    artifact_id text NOT NULL,
    session_id text NOT NULL,
    message_id text NOT NULL,
    object_key text NOT NULL,
    mime_type text NOT NULL,
    size_bytes bigint NOT NULL DEFAULT 0 CHECK (size_bytes >= 0),
    sha256 text,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'ready', 'failed', 'expired')),
    expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, artifact_id),
    FOREIGN KEY (tenant_id, session_id) REFERENCES session (tenant_id, session_id) ON DELETE CASCADE,
    CHECK (object_key LIKE 'tenants/' || tenant_id || '/%'),
    CHECK (status <> 'ready' OR sha256 IS NOT NULL)
);

CREATE TABLE audit_log (
    tenant_id text NOT NULL,
    audit_id text NOT NULL,
    trace_id text NOT NULL,
    request_id text NOT NULL,
    execution_id text NOT NULL,
    channel text NOT NULL,
    external_user text NOT NULL,
    session_id text,
    agent_app_id text,
    tool_name text,
    decision text NOT NULL,
    latency_micros bigint NOT NULL DEFAULT 0 CHECK (latency_micros >= 0),
    cost_cents bigint NOT NULL DEFAULT 0 CHECK (cost_cents >= 0),
    error_type text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, audit_id),
    FOREIGN KEY (tenant_id, session_id) REFERENCES session (tenant_id, session_id) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, agent_app_id) REFERENCES agent_app (tenant_id, agent_app_id) ON DELETE RESTRICT
);

CREATE TABLE outbox_message (
    tenant_id text NOT NULL,
    outbox_id text NOT NULL,
    kind text NOT NULL,
    aggregate_id text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'retry', 'dead')),
    attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    locked_by text,
    locked_until timestamptz,
    last_error text,
    dedup_key text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, outbox_id)
);

CREATE TABLE dead_letter (
    tenant_id text NOT NULL,
    dead_letter_id text NOT NULL,
    outbox_id text NOT NULL,
    kind text NOT NULL,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    reason text NOT NULL,
    last_error text,
    failed_at timestamptz NOT NULL DEFAULT now(),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, dead_letter_id),
    UNIQUE (tenant_id, outbox_id),
    FOREIGN KEY (tenant_id, outbox_id) REFERENCES outbox_message (tenant_id, outbox_id) ON DELETE RESTRICT
);

CREATE TABLE agent_release (
    tenant_id text NOT NULL,
    agent_app_id text NOT NULL,
    agent_version bigint NOT NULL CHECK (agent_version >= 1),
    release_id text,
    status text NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'released', 'recalled')),
    model_config_ref text,
    tool_policy jsonb NOT NULL DEFAULT '{}'::jsonb,
    artifact_ref text,
    config_ref text,
    released_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, agent_app_id, agent_version),
    UNIQUE (tenant_id, release_id),
    FOREIGN KEY (tenant_id, agent_app_id) REFERENCES agent_app (tenant_id, agent_app_id) ON DELETE CASCADE
);

CREATE TABLE tenant_config_version (
    tenant_id text NOT NULL,
    config_version bigint NOT NULL CHECK (config_version >= 1),
    status text NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'published', 'recalled')),
    config jsonb NOT NULL DEFAULT '{}'::jsonb,
    checksum text NOT NULL,
    created_by text,
    published_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, config_version),
    UNIQUE (tenant_id, checksum),
    FOREIGN KEY (tenant_id) REFERENCES tenant (tenant_id) ON DELETE CASCADE
);

CREATE UNIQUE INDEX ux_session_event_message_type
    ON session_event (tenant_id, session_id, message_id, event_type)
    WHERE message_id IS NOT NULL;
CREATE UNIQUE INDEX ux_outbox_message_dedup
    ON outbox_message (tenant_id, dedup_key)
    WHERE dedup_key IS NOT NULL;
CREATE INDEX ix_session_channel_chat
    ON session (tenant_id, channel, external_chat);
CREATE INDEX ix_session_updated
    ON session (tenant_id, updated_at);
CREATE INDEX ix_session_event_created
    ON session_event (tenant_id, session_id, created_at);
CREATE INDEX ix_message_dedup_status_expiry
    ON message_dedup (tenant_id, status, expires_at);
CREATE INDEX ix_memory_scope_updated
    ON memory (tenant_id, scope, scope_id, updated_at);
CREATE INDEX ix_memory_session_source
    ON memory (tenant_id, session_id, source_seq);
CREATE INDEX ix_artifact_session_created
    ON artifact (tenant_id, session_id, created_at);
CREATE INDEX ix_artifact_status_expiry
    ON artifact (tenant_id, status, expires_at);
CREATE INDEX ix_audit_created
    ON audit_log (tenant_id, created_at);
CREATE INDEX ix_audit_session_created
    ON audit_log (tenant_id, session_id, created_at);
CREATE INDEX ix_outbox_status_next_attempt
    ON outbox_message (tenant_id, status, next_attempt_at);
CREATE INDEX ix_dead_letter_failed
    ON dead_letter (tenant_id, failed_at);