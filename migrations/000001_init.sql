-- Current development schema. Historical development migrations are intentionally
-- squashed: this repository has not released a compatibility boundary yet.

CREATE TABLE tenants (
    id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL DEFAULT '',
	status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE platform_users (
    platform_user_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL DEFAULT '',
    email TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended')),
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_platform_users_display_name
    ON platform_users (display_name, platform_user_id);

CREATE TABLE login_providers (
    provider_id TEXT PRIMARY KEY,
    provider_type TEXT NOT NULL CHECK (provider_type IN ('local', 'wecom', 'feishu', 'oidc', 'mock')),
    display_name TEXT NOT NULL,
    enterprise_id TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (provider_type, enterprise_id)
);

CREATE TABLE login_identities (
    provider_id TEXT NOT NULL REFERENCES login_providers(provider_id),
    subject_id TEXT NOT NULL,
    platform_user_id TEXT NOT NULL REFERENCES platform_users(platform_user_id),
    display_name TEXT NOT NULL DEFAULT '',
    email TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (provider_id, subject_id)
);

CREATE INDEX idx_login_identities_platform_user
    ON login_identities (platform_user_id, provider_id);

CREATE TABLE local_credentials (
    platform_user_id TEXT PRIMARY KEY REFERENCES platform_users(platform_user_id) ON DELETE CASCADE,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    must_change_password BOOLEAN NOT NULL DEFAULT TRUE,
    credential_version BIGINT NOT NULL DEFAULT 1 CHECK (credential_version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE system_admins (
    platform_user_id TEXT PRIMARY KEY REFERENCES platform_users(platform_user_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE tenant_members (
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    platform_user_id TEXT NOT NULL REFERENCES platform_users(platform_user_id),
    role TEXT NOT NULL CHECK (role IN ('admin', 'member')),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'suspended')),
    conversation_content_audit BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, platform_user_id)
);

CREATE INDEX idx_tenant_members_user
    ON tenant_members (platform_user_id, status, tenant_id);

CREATE TABLE tenant_model_allowlist (
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    provider_id TEXT NOT NULL,
    model_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, provider_id, model_name)
);

CREATE TABLE tenant_tool_allowlist (
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    tool_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, tool_name)
);

CREATE TABLE backend_profiles (
    profile_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    driver TEXT NOT NULL CHECK (driver IN ('inmemory','postgres','redis','mysql','sqlite','mongodb','clickhouse','pgvector','qdrant','elasticsearch','s3','cos','mem0','chromadb','tencentdb')),
    connection_ref TEXT NOT NULL DEFAULT '',
    domains TEXT[] NOT NULL CHECK (
        cardinality(domains) > 0
        AND domains <@ CASE driver
            WHEN 'inmemory' THEN ARRAY['session','memory','artifact']::TEXT[]
            WHEN 'postgres' THEN ARRAY['session','memory','artifact']::TEXT[]
            WHEN 'redis' THEN ARRAY['session','memory']::TEXT[]
            WHEN 'mysql' THEN ARRAY['session','memory']::TEXT[]
            WHEN 'sqlite' THEN ARRAY['session']::TEXT[]
            WHEN 'mongodb' THEN ARRAY['session']::TEXT[]
            WHEN 'clickhouse' THEN ARRAY['session']::TEXT[]
            WHEN 'pgvector' THEN ARRAY['knowledge']::TEXT[]
            WHEN 'qdrant' THEN ARRAY['knowledge']::TEXT[]
            WHEN 'elasticsearch' THEN ARRAY['knowledge']::TEXT[]
            WHEN 's3' THEN ARRAY['artifact']::TEXT[]
            WHEN 'cos' THEN ARRAY['artifact']::TEXT[]
            WHEN 'mem0' THEN ARRAY['memory']::TEXT[]
            WHEN 'chromadb' THEN ARRAY['memory']::TEXT[]
            WHEN 'tencentdb' THEN ARRAY['memory']::TEXT[]
        END
    ),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active','disabled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO backend_profiles (profile_id, display_name, driver, connection_ref, domains, status) VALUES
    ('platform-postgres', '平台 PostgreSQL', 'postgres', '', ARRAY['session','memory','artifact'], 'active'),
    ('platform-pgvector', '平台 pgvector', 'pgvector', '', ARRAY['knowledge'], 'active');

CREATE TABLE tenant_backend_profiles (
    tenant_id TEXT NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    profile_id TEXT NOT NULL REFERENCES backend_profiles(profile_id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, profile_id)
);

CREATE INDEX idx_tenant_backend_profiles_profile
    ON tenant_backend_profiles (profile_id);

CREATE TABLE service_nodes (
    node_id TEXT PRIMARY KEY,
    boot_id TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('gateway', 'channel', 'worker', 'all')),
    state TEXT NOT NULL CHECK (state IN ('ready', 'draining')),
    build_version TEXT NOT NULL,
    inflight INTEGER NOT NULL DEFAULT 0 CHECK (inflight >= 0),
    started_at TIMESTAMPTZ NOT NULL,
    last_heartbeat TIMESTAMPTZ NOT NULL,
    lease_until TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_service_nodes_lease
    ON service_nodes (lease_until);

CREATE TABLE applications (
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    app_code TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('draft', 'active', 'disabled')),
    active_config_version BIGINT,
    candidate_config_version BIGINT,
    rollout_generation BIGINT NOT NULL DEFAULT 0 CHECK (rollout_generation >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, app_code),
    CHECK (active_config_version IS NULL OR active_config_version > 0),
    CHECK (candidate_config_version IS NULL OR candidate_config_version > 0),
    CHECK (candidate_config_version IS NULL OR candidate_config_version <> active_config_version)
);

CREATE TABLE application_configs (
    tenant_id TEXT NOT NULL,
    app_code TEXT NOT NULL,
    version BIGINT NOT NULL CHECK (version > 0),
    config_json JSONB NOT NULL,
    checksum CHAR(64) NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, app_code, version),
    FOREIGN KEY (tenant_id, app_code) REFERENCES applications(tenant_id, app_code),
    UNIQUE (tenant_id, app_code, checksum)
);

CREATE TABLE application_rollouts (
    tenant_id TEXT NOT NULL,
    app_code TEXT NOT NULL,
    generation BIGINT NOT NULL CHECK (generation > 0),
    stable_version BIGINT NOT NULL CHECK (stable_version > 0),
    candidate_version BIGINT NOT NULL CHECK (candidate_version > 0),
    basis_points INTEGER NOT NULL CHECK (basis_points BETWEEN 0 AND 10000),
    test_user_ids JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(test_user_ids) = 'array'),
    ingresses JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(ingresses) = 'array'),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, app_code),
    FOREIGN KEY (tenant_id, app_code) REFERENCES applications(tenant_id, app_code),
    FOREIGN KEY (tenant_id, app_code, stable_version) REFERENCES application_configs(tenant_id, app_code, version),
    FOREIGN KEY (tenant_id, app_code, candidate_version) REFERENCES application_configs(tenant_id, app_code, version),
    CHECK (stable_version <> candidate_version)
);

CREATE TABLE channel_bindings (
    channel_type TEXT NOT NULL CHECK (channel_type IN ('telegram', 'wecom', 'feishu')),
    external_binding_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    app_code TEXT NOT NULL,
    trusted_enterprise_id TEXT NOT NULL DEFAULT '',
	access_policy TEXT NOT NULL DEFAULT 'member_only' CHECK (access_policy IN ('member_only', 'allowlist', 'public')),
	allowlist JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(allowlist) = 'array'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (channel_type, external_binding_id),
    FOREIGN KEY (tenant_id, app_code) REFERENCES applications(tenant_id, app_code)
);

CREATE INDEX idx_channel_bindings_application
    ON channel_bindings (tenant_id, app_code);

CREATE TABLE channel_identities (
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    channel_type TEXT NOT NULL CHECK (channel_type IN ('telegram', 'wecom', 'feishu')),
    binding_id TEXT NOT NULL,
    external_user_id TEXT NOT NULL,
	platform_user_id TEXT REFERENCES platform_users(platform_user_id),
	created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
	linked_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, channel_type, binding_id, external_user_id),
    FOREIGN KEY (channel_type, binding_id) REFERENCES channel_bindings(channel_type, external_binding_id)
);

CREATE INDEX idx_channel_identities_user
    ON channel_identities (tenant_id, platform_user_id, channel_type, binding_id);
CREATE INDEX idx_channel_identities_lookup
    ON channel_identities (tenant_id, channel_type, external_user_id);

-- trpc-agent-go PostgreSQL Memory schema. Runtime Memory services use
-- WithSkipDBInit(true); schema ownership and DDL stay in this baseline.
CREATE TABLE memories (
    memory_id TEXT PRIMARY KEY,
    app_name TEXT NOT NULL,
    user_id TEXT NOT NULL,
    memory_data JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMP NULL DEFAULT NULL
);

CREATE INDEX idx_memories_app_user
    ON memories (app_name, user_id);
CREATE INDEX idx_memories_updated_at
    ON memories (updated_at DESC);
CREATE INDEX idx_memories_deleted_at
    ON memories (deleted_at);

ALTER TABLE memories ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope_memories ON memories
    USING (app_name = current_setting('app.app_name', true))
    WITH CHECK (app_name = current_setting('app.app_name', true));
ALTER TABLE memories FORCE ROW LEVEL SECURITY;

-- trpc-agent-go PostgreSQL Session schema. Runtime Session services use
-- WithSkipDBInit(true); schema ownership and DDL stay in this baseline.
CREATE TABLE session_states (
    id BIGSERIAL PRIMARY KEY,
    app_name VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    session_id VARCHAR(255) NOT NULL,
    state JSONB DEFAULT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ DEFAULT NULL,
    deleted_at TIMESTAMPTZ DEFAULT NULL
);

CREATE UNIQUE INDEX idx_session_states_unique_active
    ON session_states (app_name, user_id, session_id)
    WHERE deleted_at IS NULL;
CREATE INDEX idx_session_states_expires
    ON session_states (expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE session_events (
    id BIGSERIAL PRIMARY KEY,
    app_name VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    session_id VARCHAR(255) NOT NULL,
    event JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ DEFAULT NULL,
    deleted_at TIMESTAMPTZ DEFAULT NULL
);

CREATE INDEX idx_session_events_lookup
    ON session_events (app_name, user_id, session_id, created_at);
CREATE INDEX idx_session_events_expires
    ON session_events (expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE session_track_events (
    id BIGSERIAL PRIMARY KEY,
    app_name VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    session_id VARCHAR(255) NOT NULL,
    track VARCHAR(255) NOT NULL,
    event JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ DEFAULT NULL,
    deleted_at TIMESTAMPTZ DEFAULT NULL
);

CREATE INDEX idx_session_track_events_lookup
    ON session_track_events (app_name, user_id, session_id, track, created_at);
CREATE INDEX idx_session_track_events_expires
    ON session_track_events (expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE session_summaries (
    id BIGSERIAL PRIMARY KEY,
    app_name VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    session_id VARCHAR(255) NOT NULL,
    filter_key VARCHAR(255) NOT NULL DEFAULT '',
    summary JSONB DEFAULT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ DEFAULT NULL,
    deleted_at TIMESTAMPTZ DEFAULT NULL
);

CREATE UNIQUE INDEX idx_session_summaries_unique_active
    ON session_summaries (app_name, user_id, session_id, filter_key)
    WHERE deleted_at IS NULL;
CREATE INDEX idx_session_summaries_expires
    ON session_summaries (expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE app_states (
    id BIGSERIAL PRIMARY KEY,
    app_name VARCHAR(255) NOT NULL,
    key VARCHAR(255) NOT NULL,
    value TEXT DEFAULT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ DEFAULT NULL,
    deleted_at TIMESTAMPTZ DEFAULT NULL
);

CREATE UNIQUE INDEX idx_app_states_unique_active
    ON app_states (app_name, key)
    WHERE deleted_at IS NULL;
CREATE INDEX idx_app_states_expires
    ON app_states (expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE user_states (
    id BIGSERIAL PRIMARY KEY,
    app_name VARCHAR(255) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    key VARCHAR(255) NOT NULL,
    value TEXT DEFAULT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMPTZ DEFAULT NULL,
    deleted_at TIMESTAMPTZ DEFAULT NULL
);

CREATE UNIQUE INDEX idx_user_states_unique_active
    ON user_states (app_name, user_id, key)
    WHERE deleted_at IS NULL;
CREATE INDEX idx_user_states_expires
    ON user_states (expires_at) WHERE expires_at IS NOT NULL;

ALTER TABLE session_states ENABLE ROW LEVEL SECURITY;
ALTER TABLE session_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE session_track_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE session_summaries ENABLE ROW LEVEL SECURITY;
ALTER TABLE app_states ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_states ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope_session_states ON session_states
    USING (app_name = current_setting('app.app_name', true))
    WITH CHECK (app_name = current_setting('app.app_name', true));
CREATE POLICY tenant_scope_session_events ON session_events
    USING (app_name = current_setting('app.app_name', true))
    WITH CHECK (app_name = current_setting('app.app_name', true));
CREATE POLICY tenant_scope_session_track_events ON session_track_events
    USING (app_name = current_setting('app.app_name', true))
    WITH CHECK (app_name = current_setting('app.app_name', true));
CREATE POLICY tenant_scope_session_summaries ON session_summaries
    USING (app_name = current_setting('app.app_name', true))
    WITH CHECK (app_name = current_setting('app.app_name', true));
CREATE POLICY tenant_scope_app_states ON app_states
    USING (app_name = current_setting('app.app_name', true))
    WITH CHECK (app_name = current_setting('app.app_name', true));
CREATE POLICY tenant_scope_user_states ON user_states
    USING (app_name = current_setting('app.app_name', true))
    WITH CHECK (app_name = current_setting('app.app_name', true));
ALTER TABLE session_states FORCE ROW LEVEL SECURITY;
ALTER TABLE session_events FORCE ROW LEVEL SECURITY;
ALTER TABLE session_track_events FORCE ROW LEVEL SECURITY;
ALTER TABLE session_summaries FORCE ROW LEVEL SECURITY;
ALTER TABLE app_states FORCE ROW LEVEL SECURITY;
ALTER TABLE user_states FORCE ROW LEVEL SECURITY;

CREATE TABLE sessions (
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    app_code TEXT NOT NULL,
    session_key TEXT NOT NULL,
    last_message_id TEXT NOT NULL,
    revision BIGINT NOT NULL DEFAULT 0 CHECK (revision >= 0),
    subject_id TEXT NOT NULL DEFAULT '',
    owner_platform_user_id TEXT REFERENCES platform_users(platform_user_id),
    status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'archived')),
    archived_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, session_key),
    FOREIGN KEY (tenant_id, app_code) REFERENCES applications(tenant_id, app_code)
);

CREATE INDEX idx_sessions_idle_active
    ON sessions (updated_at) WHERE status = 'active';

CREATE TABLE session_execution_leases (
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    session_key TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    fencing_token BIGINT NOT NULL CHECK (fencing_token > 0),
    lease_until TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, session_key)
);

CREATE INDEX idx_session_execution_leases_expiry
    ON session_execution_leases (lease_until);

CREATE TABLE session_backend_migrations (
    migration_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    app_code TEXT NOT NULL,
    generation BIGINT NOT NULL CHECK (generation > 0),
    source_profile_id TEXT NOT NULL,
    target_profile_id TEXT NOT NULL,
    source_driver TEXT NOT NULL CHECK (source_driver IN ('postgres','redis','inmemory','mysql','sqlite','mongodb','clickhouse')),
    source_connection_ref TEXT NOT NULL DEFAULT '',
    target_driver TEXT NOT NULL CHECK (target_driver IN ('postgres','redis','inmemory','mysql','sqlite','mongodb','clickhouse')),
    target_connection_ref TEXT NOT NULL DEFAULT '',
    phase TEXT NOT NULL CHECK (phase IN ('prepared','dual_write','backfill','verify','cut_read','stop_old_write','done','rolled_back')),
    backfilled_sessions INTEGER NOT NULL DEFAULT 0 CHECK (backfilled_sessions >= 0),
    verified_sessions INTEGER NOT NULL DEFAULT 0 CHECK (verified_sessions >= 0),
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, app_code) REFERENCES applications(tenant_id, app_code)
);

CREATE UNIQUE INDEX uq_session_backend_migration_active
    ON session_backend_migrations (tenant_id, app_code)
    WHERE phase NOT IN ('done','rolled_back');

CREATE INDEX idx_session_backend_migrations_app_updated
    ON session_backend_migrations (tenant_id, app_code, updated_at DESC);

CREATE TABLE session_migration_repairs (
    migration_id TEXT NOT NULL REFERENCES session_backend_migrations(migration_id) ON DELETE CASCADE,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    app_code TEXT NOT NULL,
    scope TEXT NOT NULL CHECK (scope IN ('session','user','application')),
    scope_key TEXT NOT NULL CHECK (btrim(scope_key) <> ''),
    subject_id TEXT NOT NULL DEFAULT '',
    route_generation BIGINT NOT NULL CHECK (route_generation > 0),
    primary_profile_id TEXT NOT NULL,
    replica_profile_id TEXT NOT NULL,
    revision BIGINT NOT NULL CHECK (revision > 0),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_owner TEXT NOT NULL DEFAULT '',
    lease_until TIMESTAMPTZ,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (migration_id, scope, scope_key, subject_id),
    FOREIGN KEY (tenant_id, app_code) REFERENCES applications(tenant_id, app_code)
);

CREATE INDEX idx_session_migration_repairs_claim
    ON session_migration_repairs (tenant_id, migration_id, next_attempt_at, lease_until, updated_at);

CREATE TABLE channel_conversations (
    tenant_id TEXT NOT NULL,
    app_code TEXT NOT NULL,
    channel_type TEXT NOT NULL CHECK (channel_type IN ('telegram', 'wecom', 'feishu', 'web')),
    binding_id TEXT NOT NULL CHECK (btrim(binding_id) <> ''),
    external_conversation_id TEXT NOT NULL,
    external_user_id TEXT NOT NULL DEFAULT '',
    session_key TEXT NOT NULL,
    scope TEXT NOT NULL CHECK (scope IN ('direct', 'group')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, app_code, channel_type, binding_id, external_conversation_id, session_key),
    FOREIGN KEY (tenant_id, app_code) REFERENCES applications(tenant_id, app_code),
    FOREIGN KEY (tenant_id, session_key) REFERENCES sessions(tenant_id, session_key)
);

CREATE UNIQUE INDEX uq_channel_conversations_active
    ON channel_conversations (tenant_id, app_code, channel_type, binding_id, external_conversation_id)
    WHERE ended_at IS NULL;

CREATE INDEX idx_channel_conversations_session
    ON channel_conversations (tenant_id, session_key, updated_at DESC);

CREATE TABLE inbound_message_routes (
    tenant_id TEXT NOT NULL,
    app_code TEXT NOT NULL,
    channel_type TEXT NOT NULL CHECK (channel_type IN ('telegram', 'wecom', 'feishu', 'web')),
    message_id TEXT NOT NULL,
    session_key TEXT NOT NULL,
    binding_id TEXT NOT NULL CHECK (btrim(binding_id) <> ''),
    external_conversation_id TEXT NOT NULL,
    scope TEXT NOT NULL CHECK (scope IN ('direct', 'group')),
    actor_external_user_id TEXT NOT NULL,
    actor_platform_user_id TEXT,
    trigger_type TEXT NOT NULL CHECK (trigger_type IN ('direct', 'mention', 'command', 'action')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, channel_type, binding_id, message_id),
    FOREIGN KEY (tenant_id, app_code) REFERENCES applications(tenant_id, app_code),
    FOREIGN KEY (tenant_id, session_key) REFERENCES sessions(tenant_id, session_key),
    FOREIGN KEY (actor_platform_user_id) REFERENCES platform_users(platform_user_id)
);

CREATE INDEX idx_inbound_message_routes_session
    ON inbound_message_routes (tenant_id, session_key, created_at);

CREATE INDEX idx_inbound_message_routes_session_message
    ON inbound_message_routes (tenant_id, session_key, message_id);

CREATE TABLE messages (
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    app_code TEXT NOT NULL,
    channel_type TEXT NOT NULL CHECK (channel_type IN ('telegram', 'wecom', 'feishu', 'web')),
    binding_id TEXT NOT NULL CHECK (btrim(binding_id) <> ''),
    message_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('processing', 'completed', 'failed')),
    trace_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, channel_type, binding_id, message_id),
    FOREIGN KEY (tenant_id, app_code) REFERENCES applications(tenant_id, app_code)
);

CREATE INDEX idx_messages_tenant_updated
    ON messages (tenant_id, updated_at DESC, message_id);

CREATE TABLE message_retry_attempts (
    tenant_id TEXT NOT NULL,
    session_key TEXT NOT NULL,
    event_id TEXT NOT NULL,
    attempts INTEGER NOT NULL CHECK (attempts >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, session_key, event_id)
);

CREATE TABLE execution_traces (
    tenant_id TEXT NOT NULL,
    app_code TEXT NOT NULL,
    channel_type TEXT NOT NULL CHECK (channel_type IN ('telegram', 'wecom', 'feishu', 'web')),
    binding_id TEXT NOT NULL CHECK (btrim(binding_id) <> ''),
    message_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    projection JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, channel_type, binding_id, message_id),
    FOREIGN KEY (tenant_id, app_code) REFERENCES applications(tenant_id, app_code)
);

CREATE INDEX idx_execution_traces_trace
    ON execution_traces (tenant_id, trace_id);

CREATE TABLE audit_events (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    trace_id TEXT NOT NULL,
    request_id TEXT NOT NULL DEFAULT '',
    channel TEXT NOT NULL DEFAULT '',
    user_id TEXT NOT NULL DEFAULT '',
    session_id TEXT NOT NULL DEFAULT '',
    agent_name TEXT NOT NULL DEFAULT '',
    tool_name TEXT NOT NULL DEFAULT '',
    action TEXT NOT NULL,
    result TEXT NOT NULL,
    decision TEXT NOT NULL DEFAULT '',
    latency_ms BIGINT NOT NULL DEFAULT 0,
    error_type TEXT NOT NULL DEFAULT '',
    cost_micros BIGINT NOT NULL DEFAULT 0,
    redacted_detail TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_events_tenant_trace
    ON audit_events (tenant_id, trace_id, created_at);
CREATE INDEX idx_audit_events_purge
    ON audit_events (tenant_id, created_at);

CREATE TABLE outbox_events (
    id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL DEFAULT '',
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    aggregate_key TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at TIMESTAMPTZ,
    delivery_owner TEXT,
    lease_expires_at TIMESTAMPTZ,
    delivery_receipt TEXT NOT NULL DEFAULT '',
    delivery_attempts INTEGER NOT NULL DEFAULT 0 CHECK (delivery_attempts >= 0),
    last_delivery_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_outbox_events_tenant_request
    ON outbox_events (tenant_id, request_id) WHERE request_id <> '';
CREATE INDEX idx_outbox_events_dispatch_lease
    ON outbox_events (tenant_id, available_at, lease_expires_at, created_at)
    WHERE delivered_at IS NULL;
CREATE INDEX idx_outbox_events_delivered_retention
    ON outbox_events (tenant_id, delivered_at)
    WHERE delivered_at IS NOT NULL;

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE knowledge_documents (
    tenant_id TEXT NOT NULL,
    app_code TEXT NOT NULL,
    document_id TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'ready' CHECK (status IN ('ready', 'indexing', 'failed')),
    total_chunks INTEGER NOT NULL DEFAULT 0,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, app_code, document_id),
    FOREIGN KEY (tenant_id, app_code) REFERENCES applications(tenant_id, app_code)
);

-- Canonical reindex source. Vector backends are derived indexes and may be
-- replaced without losing the original tenant document needed to rebuild
-- embeddings in another framework VectorStore.
CREATE TABLE knowledge_document_sources (
    tenant_id TEXT NOT NULL,
    app_code TEXT NOT NULL,
    document_id TEXT NOT NULL,
    name TEXT NOT NULL,
    filename TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT '',
    source_data BYTEA NOT NULL,
    chunk_size INTEGER NOT NULL DEFAULT 0 CHECK (chunk_size >= 0),
    overlap INTEGER NOT NULL DEFAULT 0 CHECK (overlap >= 0),
    metadata JSONB NOT NULL DEFAULT '{}',
    canonical_documents JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, app_code, document_id),
    FOREIGN KEY (tenant_id, app_code, document_id)
        REFERENCES knowledge_documents(tenant_id, app_code, document_id) ON DELETE CASCADE
);

-- Matches trpc-agent-go's pgvector schema exactly. Tenant/application scope
-- remains framework metadata so SELECT * continues to match the framework
-- scanner while PostgreSQL RLS can still enforce the boundary.
CREATE TABLE knowledge_vectors (
    id TEXT PRIMARY KEY,
    name VARCHAR(255),
    content TEXT,
    embedding vector(1536),
    metadata JSONB,
    created_at BIGINT,
    updated_at BIGINT
);

CREATE INDEX knowledge_vectors_embedding_idx
    ON knowledge_vectors USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64);
CREATE INDEX knowledge_vectors_content_fts_idx
    ON knowledge_vectors USING gin (to_tsvector('simple', content));
CREATE INDEX knowledge_vectors_metadata_idx
    ON knowledge_vectors USING gin (metadata);

CREATE TABLE knowledge_ingest_jobs (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    app_code TEXT NOT NULL,
    document_id TEXT NOT NULL,
    name TEXT NOT NULL,
    filename TEXT NOT NULL,
    content_type TEXT NOT NULL DEFAULT '',
    source_data BYTEA NOT NULL,
    chunk_size INTEGER NOT NULL DEFAULT 0 CHECK (chunk_size >= 0),
    overlap INTEGER NOT NULL DEFAULT 0 CHECK (overlap >= 0),
    metadata JSONB NOT NULL DEFAULT '{}',
    backend_profile_id TEXT NOT NULL DEFAULT '',
    backend_driver TEXT NOT NULL CHECK (backend_driver IN ('pgvector','qdrant','elasticsearch')),
    backend_connection_ref TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    lease_owner TEXT NOT NULL DEFAULT '',
    lease_until TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, app_code, document_id),
    FOREIGN KEY (tenant_id, app_code) REFERENCES applications(tenant_id, app_code)
);

CREATE TABLE knowledge_backend_migrations (
    migration_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    app_code TEXT NOT NULL,
    generation BIGINT NOT NULL DEFAULT 1 CHECK (generation > 0),
    source_profile_id TEXT NOT NULL,
    target_profile_id TEXT NOT NULL,
    source_driver TEXT NOT NULL CHECK (source_driver IN ('pgvector','qdrant')),
    source_connection_ref TEXT NOT NULL DEFAULT '',
    target_driver TEXT NOT NULL CHECK (target_driver IN ('pgvector','qdrant')),
    target_connection_ref TEXT NOT NULL DEFAULT '',
    phase TEXT NOT NULL CHECK (phase IN ('prepared','reindexed','verified','done','rolled_back')),
    reindexed_documents INTEGER NOT NULL DEFAULT 0 CHECK (reindexed_documents >= 0),
    verified_documents INTEGER NOT NULL DEFAULT 0 CHECK (verified_documents >= 0),
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, app_code) REFERENCES applications(tenant_id, app_code)
);

CREATE UNIQUE INDEX uq_knowledge_backend_migrations_active
    ON knowledge_backend_migrations (tenant_id, app_code)
    WHERE phase NOT IN ('done','rolled_back');

CREATE INDEX idx_knowledge_ingest_jobs_claim
    ON knowledge_ingest_jobs (status, available_at, lease_until, created_at);

CREATE TABLE artifacts (
    tenant_id TEXT NOT NULL,
    app_code TEXT NOT NULL,
    user_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    filename TEXT NOT NULL,
    version INTEGER NOT NULL CHECK (version >= 0),
    mime_type TEXT NOT NULL,
    content BYTEA NOT NULL,
    artifact_url TEXT NOT NULL DEFAULT '',
    display_name TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, app_code, user_id, session_id, filename, version),
    FOREIGN KEY (tenant_id, app_code) REFERENCES applications(tenant_id, app_code)
);

CREATE TABLE model_usage_ledger (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    app_code TEXT NOT NULL,
    channel_type TEXT NOT NULL,
    binding_id TEXT NOT NULL DEFAULT '',
    message_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    provider_id TEXT NOT NULL,
    model_name TEXT NOT NULL,
    usage_known BOOLEAN NOT NULL,
    prompt_tokens BIGINT CHECK (prompt_tokens >= 0),
    cached_prompt_tokens BIGINT CHECK (cached_prompt_tokens >= 0 AND cached_prompt_tokens <= prompt_tokens),
    completion_tokens BIGINT CHECK (completion_tokens >= 0),
    total_tokens BIGINT CHECK (total_tokens >= 0),
    cost_micros BIGINT CHECK (cost_micros >= 0),
    usage_breakdown JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(usage_breakdown) = 'array'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, channel_type, binding_id, message_id),
    FOREIGN KEY (tenant_id, app_code) REFERENCES applications(tenant_id, app_code),
    CHECK (
        (usage_known AND prompt_tokens IS NOT NULL AND cached_prompt_tokens IS NOT NULL AND completion_tokens IS NOT NULL AND total_tokens IS NOT NULL AND cost_micros IS NOT NULL)
        OR
        (NOT usage_known AND prompt_tokens IS NULL AND cached_prompt_tokens IS NULL AND completion_tokens IS NULL AND total_tokens IS NULL AND cost_micros IS NULL)
    )
);

CREATE INDEX idx_model_usage_ledger_tenant_created
    ON model_usage_ledger (tenant_id, created_at DESC);

CREATE TABLE model_usage_reservations (
    reservation_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    app_code TEXT NOT NULL,
    channel_type TEXT NOT NULL,
    binding_id TEXT NOT NULL DEFAULT '',
    message_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    period_start TIMESTAMPTZ NOT NULL,
    reserved_tokens BIGINT NOT NULL CHECK (reserved_tokens >= 0),
    status TEXT NOT NULL CHECK (status IN ('pending', 'settled_known', 'settled_unknown')),
    prompt_tokens BIGINT CHECK (prompt_tokens >= 0),
    completion_tokens BIGINT CHECK (completion_tokens >= 0),
    total_tokens BIGINT CHECK (total_tokens >= 0),
    cost_micros BIGINT CHECK (cost_micros >= 0),
    lease_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, app_code) REFERENCES applications(tenant_id, app_code),
    CHECK (
        (status = 'settled_known' AND prompt_tokens IS NOT NULL AND completion_tokens IS NOT NULL AND total_tokens IS NOT NULL AND cost_micros IS NOT NULL AND lease_until IS NULL)
        OR (status = 'settled_unknown' AND prompt_tokens IS NULL AND completion_tokens IS NULL AND total_tokens IS NULL AND cost_micros IS NULL AND lease_until IS NULL)
        OR (status = 'pending' AND prompt_tokens IS NULL AND completion_tokens IS NULL AND total_tokens IS NULL AND cost_micros IS NULL AND lease_until IS NOT NULL)
    )
);

CREATE INDEX idx_model_usage_reservations_window
    ON model_usage_reservations (tenant_id, app_code, period_start, status);

CREATE INDEX idx_model_usage_reservations_active
    ON model_usage_reservations (tenant_id, app_code, lease_until)
    WHERE status = 'pending';

CREATE TABLE tool_executions (
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    request_id TEXT NOT NULL,
    tool_call_id TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    arguments_hash CHAR(64) NOT NULL,
    idempotency_key CHAR(64) NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'outcome_unknown')),
    result_hash CHAR(64),
    result_ciphertext BYTEA,
    error_type TEXT NOT NULL DEFAULT '',
    trace_id TEXT NOT NULL,
    lease_until TIMESTAMPTZ,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, request_id, tool_call_id),
    UNIQUE (tenant_id, idempotency_key),
    CHECK (
        (status = 'completed' AND result_hash IS NOT NULL AND result_ciphertext IS NOT NULL AND completed_at IS NOT NULL)
        OR (status <> 'completed')
    )
);

CREATE INDEX idx_tool_executions_status
    ON tool_executions (tenant_id, status, updated_at);

CREATE TABLE tool_approvals (
    approval_token TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    app_code TEXT NOT NULL,
    config_version BIGINT NOT NULL CHECK (config_version > 0),
    request_id TEXT NOT NULL DEFAULT '',
    trace_id TEXT NOT NULL DEFAULT '',
    channel_type TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    conversation_scope TEXT NOT NULL DEFAULT '',
    external_user_id TEXT NOT NULL,
    requester_user_id TEXT NOT NULL DEFAULT '',
    progress_message_id TEXT NOT NULL DEFAULT '',
    tool_name TEXT NOT NULL,
    tool_description TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'rejected', 'expired', 'canceled')),
    notification_id TEXT NOT NULL DEFAULT '',
    resolved_by TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    notified_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (tenant_id, app_code) REFERENCES applications(tenant_id, app_code) ON DELETE CASCADE,
    CHECK (expires_at > created_at),
    CHECK (
        (status = 'pending' AND resolved_at IS NULL)
        OR (status <> 'pending' AND resolved_at IS NOT NULL)
    )
);

CREATE INDEX idx_tool_approvals_tenant_status
    ON tool_approvals (tenant_id, status, created_at DESC);

ALTER TABLE tenants ENABLE ROW LEVEL SECURITY;
ALTER TABLE applications ENABLE ROW LEVEL SECURITY;
ALTER TABLE application_configs ENABLE ROW LEVEL SECURITY;
ALTER TABLE application_rollouts ENABLE ROW LEVEL SECURITY;
ALTER TABLE channel_bindings ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_members ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_model_allowlist ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_tool_allowlist ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_backend_profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE channel_identities ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope_tenants ON tenants
    USING (id = current_setting('app.tenant_id', true))
    WITH CHECK (id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_scope_applications ON applications
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_scope_application_configs ON application_configs
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_scope_application_rollouts ON application_rollouts
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_scope_channel_bindings ON channel_bindings
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_scope_tenant_members ON tenant_members
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_scope_tenant_model_allowlist ON tenant_model_allowlist
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_scope_tenant_tool_allowlist ON tenant_tool_allowlist
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_scope_tenant_backend_profiles ON tenant_backend_profiles
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_scope_channel_identities ON channel_identities
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE tenants FORCE ROW LEVEL SECURITY;
ALTER TABLE applications FORCE ROW LEVEL SECURITY;
ALTER TABLE application_configs FORCE ROW LEVEL SECURITY;
ALTER TABLE application_rollouts FORCE ROW LEVEL SECURITY;
ALTER TABLE channel_bindings FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant_members FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant_model_allowlist FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant_tool_allowlist FORCE ROW LEVEL SECURITY;
ALTER TABLE tenant_backend_profiles FORCE ROW LEVEL SECURITY;
ALTER TABLE channel_identities FORCE ROW LEVEL SECURITY;

ALTER TABLE knowledge_documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE knowledge_document_sources ENABLE ROW LEVEL SECURITY;
ALTER TABLE knowledge_vectors ENABLE ROW LEVEL SECURITY;
ALTER TABLE knowledge_ingest_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE knowledge_backend_migrations ENABLE ROW LEVEL SECURITY;
ALTER TABLE artifacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE execution_traces ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope_knowledge_documents ON knowledge_documents
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_scope_knowledge_document_sources ON knowledge_document_sources
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
-- Retrieval must never expose a partially indexed document. Normal readers
-- see vectors only after the platform projection is ready; the currently
-- leased ingest worker can see its in-progress rows so framework Count/Delete
-- continue to work before completion.
CREATE POLICY tenant_scope_knowledge_vectors_select ON knowledge_vectors
    FOR SELECT
    USING (
        metadata @> jsonb_build_object(
            'tenant_id', current_setting('app.tenant_id', true),
            'app_code', current_setting('app.app_code', true)
        )
        AND (
            EXISTS (
                SELECT 1
                FROM knowledge_ingest_jobs AS job
                WHERE job.id = current_setting('app.knowledge_ingest_job', true)
                  AND job.tenant_id = current_setting('app.tenant_id', true)
                  AND job.app_code = current_setting('app.app_code', true)
                  AND job.status = 'running'
                  AND job.lease_owner = current_setting('app.knowledge_ingest_owner', true)
                  AND job.lease_until >= NOW()
            )
            OR EXISTS (
                SELECT 1
                FROM knowledge_documents AS document
                WHERE document.tenant_id = current_setting('app.tenant_id', true)
                  AND document.app_code = current_setting('app.app_code', true)
                  AND document.document_id = knowledge_vectors.metadata->>'parent_document_id'
                  AND document.status = 'ready'
            )
        )
    );
CREATE POLICY tenant_scope_knowledge_vectors_insert ON knowledge_vectors
    FOR INSERT
    WITH CHECK (
        metadata @> jsonb_build_object(
            'tenant_id', current_setting('app.tenant_id', true),
            'app_code', current_setting('app.app_code', true)
        )
        AND EXISTS (
            SELECT 1
            FROM knowledge_ingest_jobs AS job
            WHERE job.id = current_setting('app.knowledge_ingest_job', true)
              AND job.tenant_id = current_setting('app.tenant_id', true)
              AND job.app_code = current_setting('app.app_code', true)
              AND job.status = 'running'
              AND job.lease_owner = current_setting('app.knowledge_ingest_owner', true)
              AND job.lease_until >= NOW()
        )
    );
CREATE POLICY tenant_scope_knowledge_vectors_update ON knowledge_vectors
    FOR UPDATE
    USING (
        metadata @> jsonb_build_object(
            'tenant_id', current_setting('app.tenant_id', true),
            'app_code', current_setting('app.app_code', true)
        )
        AND EXISTS (
            SELECT 1 FROM knowledge_ingest_jobs AS job
            WHERE job.id = current_setting('app.knowledge_ingest_job', true)
              AND job.tenant_id = current_setting('app.tenant_id', true)
              AND job.app_code = current_setting('app.app_code', true)
              AND job.status = 'running'
              AND job.lease_owner = current_setting('app.knowledge_ingest_owner', true)
              AND job.lease_until >= NOW()
        )
    )
    WITH CHECK (
        metadata @> jsonb_build_object(
            'tenant_id', current_setting('app.tenant_id', true),
            'app_code', current_setting('app.app_code', true)
        )
    );
CREATE POLICY tenant_scope_knowledge_vectors_delete ON knowledge_vectors
    FOR DELETE
    USING (
        metadata @> jsonb_build_object(
            'tenant_id', current_setting('app.tenant_id', true),
            'app_code', current_setting('app.app_code', true)
        )
        AND (
            NULLIF(current_setting('app.knowledge_ingest_job', true), '') IS NULL
            OR EXISTS (
                SELECT 1 FROM knowledge_ingest_jobs AS job
                WHERE job.id = current_setting('app.knowledge_ingest_job', true)
                  AND job.tenant_id = current_setting('app.tenant_id', true)
                  AND job.app_code = current_setting('app.app_code', true)
                  AND job.status = 'running'
                  AND job.lease_owner = current_setting('app.knowledge_ingest_owner', true)
                  AND job.lease_until >= NOW()
            )
        )
    );
CREATE POLICY tenant_scope_knowledge_ingest_jobs ON knowledge_ingest_jobs
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_scope_knowledge_backend_migrations ON knowledge_backend_migrations
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_scope_artifacts ON artifacts
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_scope_execution_traces ON execution_traces
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE knowledge_documents FORCE ROW LEVEL SECURITY;
ALTER TABLE knowledge_document_sources FORCE ROW LEVEL SECURITY;
ALTER TABLE knowledge_vectors FORCE ROW LEVEL SECURITY;
ALTER TABLE knowledge_ingest_jobs FORCE ROW LEVEL SECURITY;
ALTER TABLE knowledge_backend_migrations FORCE ROW LEVEL SECURITY;
ALTER TABLE artifacts FORCE ROW LEVEL SECURITY;
ALTER TABLE execution_traces FORCE ROW LEVEL SECURITY;

ALTER TABLE session_execution_leases ENABLE ROW LEVEL SECURITY;
ALTER TABLE session_backend_migrations ENABLE ROW LEVEL SECURITY;
ALTER TABLE session_migration_repairs ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope_session_execution_leases ON session_execution_leases
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE session_execution_leases FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope_session_backend_migrations ON session_backend_migrations
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE session_backend_migrations FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope_session_migration_repairs ON session_migration_repairs
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE session_migration_repairs FORCE ROW LEVEL SECURITY;

ALTER TABLE channel_conversations ENABLE ROW LEVEL SECURITY;
ALTER TABLE inbound_message_routes ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope_channel_conversations ON channel_conversations
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_scope_inbound_message_routes ON inbound_message_routes
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE channel_conversations FORCE ROW LEVEL SECURITY;
ALTER TABLE inbound_message_routes FORCE ROW LEVEL SECURITY;

ALTER TABLE sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE messages ENABLE ROW LEVEL SECURITY;
ALTER TABLE message_retry_attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE outbox_events ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope_sessions ON sessions
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_scope_messages ON messages
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_scope_message_retry_attempts ON message_retry_attempts
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_scope_audit_events ON audit_events
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_scope_outbox_events ON outbox_events
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE sessions FORCE ROW LEVEL SECURITY;
ALTER TABLE messages FORCE ROW LEVEL SECURITY;
ALTER TABLE message_retry_attempts FORCE ROW LEVEL SECURITY;
ALTER TABLE audit_events FORCE ROW LEVEL SECURITY;
ALTER TABLE outbox_events FORCE ROW LEVEL SECURITY;

ALTER TABLE model_usage_ledger ENABLE ROW LEVEL SECURITY;
ALTER TABLE model_usage_reservations ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope_model_usage_ledger ON model_usage_ledger
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_scope_model_usage_reservations ON model_usage_reservations
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE model_usage_ledger FORCE ROW LEVEL SECURITY;
ALTER TABLE model_usage_reservations FORCE ROW LEVEL SECURITY;

ALTER TABLE tool_executions ENABLE ROW LEVEL SECURITY;
ALTER TABLE tool_approvals ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_scope_tool_executions ON tool_executions
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
CREATE POLICY tenant_scope_tool_approvals ON tool_approvals
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
ALTER TABLE tool_executions FORCE ROW LEVEL SECURITY;
ALTER TABLE tool_approvals FORCE ROW LEVEL SECURITY;

-- Tenant-scoped transactions switch from the login role to trpc_tenant so
-- FORCE RLS applies even when the service login owns the tables. Keep the
-- table privileges in the schema baseline itself: relying only on PostgreSQL
-- ALTER DEFAULT PRIVILEGES from container bootstrap makes a rebuilt schema
-- unusable when those defaults are absent.
GRANT USAGE ON SCHEMA public TO trpc_tenant;
DO $$
DECLARE
    relation RECORD;
BEGIN
    FOR relation IN
        SELECT namespace.nspname AS schema_name, class.relname AS table_name
        FROM pg_class AS class
        JOIN pg_namespace AS namespace ON namespace.oid = class.relnamespace
        WHERE namespace.nspname = 'public'
          AND class.relkind IN ('r', 'p')
          AND class.relrowsecurity
          AND class.relforcerowsecurity
    LOOP
        EXECUTE format(
            'GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE %I.%I TO trpc_tenant',
            relation.schema_name,
            relation.table_name
        );
    END LOOP;
END $$;

GRANT USAGE, SELECT ON SEQUENCE
    session_states_id_seq,
    session_events_id_seq,
    session_track_events_id_seq,
    session_summaries_id_seq,
    app_states_id_seq,
    user_states_id_seq
TO trpc_tenant;
