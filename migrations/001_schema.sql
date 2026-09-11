BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS schema_migrations (
    version text PRIMARY KEY,
    description text NOT NULL,
    checksum text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE tenants (
    tenant_id text PRIMARY KEY
        CHECK (tenant_id ~ '^[a-z][a-z0-9_-]{1,62}$'),
    display_name text NOT NULL,
    status text NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'suspended', 'deleting')),
    isolation_mode text NOT NULL DEFAULT 'shared_rls'
        CHECK (isolation_mode IN ('shared_rls', 'schema', 'database')),
    active_config_revision text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(metadata) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE tenant_config_revisions (
    tenant_id text NOT NULL REFERENCES tenants (tenant_id) ON DELETE CASCADE,
    revision text NOT NULL CHECK (revision <> ''),
    previous_revision text,
    status text NOT NULL DEFAULT 'draft'
        CHECK (status IN ('draft', 'validating', 'canary', 'active', 'rolled_back', 'rejected')),
    rollout_percent smallint NOT NULL DEFAULT 0
        CHECK (rollout_percent BETWEEN 0 AND 100),
    config jsonb NOT NULL CHECK (jsonb_typeof(config) = 'object'),
    config_sha256 text NOT NULL CHECK (config_sha256 ~ '^[0-9a-f]{64}$'),
    created_by text NOT NULL,
    change_reason text NOT NULL DEFAULT '',
    validated_at timestamptz,
    activated_at timestamptz,
    rolled_back_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, revision),
    FOREIGN KEY (tenant_id, previous_revision)
        REFERENCES tenant_config_revisions (tenant_id, revision)
        DEFERRABLE INITIALLY DEFERRED
);

ALTER TABLE tenants
    ADD CONSTRAINT tenants_active_config_revision_fk
    FOREIGN KEY (tenant_id, active_config_revision)
    REFERENCES tenant_config_revisions (tenant_id, revision)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE agent_apps (
    tenant_id text NOT NULL REFERENCES tenants (tenant_id) ON DELETE CASCADE,
    app_id text NOT NULL,
    agent_name text NOT NULL,
    display_name text NOT NULL,
    description text NOT NULL DEFAULT '',
    instruction_ciphertext bytea,
    config_revision text NOT NULL,
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, app_id),
    UNIQUE (tenant_id, agent_name),
    FOREIGN KEY (tenant_id, config_revision)
        REFERENCES tenant_config_revisions (tenant_id, revision)
);

CREATE TABLE model_configs (
    tenant_id text NOT NULL,
    model_config_id uuid NOT NULL DEFAULT gen_random_uuid(),
    app_id text NOT NULL,
    provider text NOT NULL,
    model_name text NOT NULL,
    variant text NOT NULL DEFAULT '',
    base_url text NOT NULL DEFAULT '',
    api_key_secret_ref text NOT NULL DEFAULT '',
    parameters jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(parameters) = 'object'),
    input_price_per_million numeric(18, 8) NOT NULL DEFAULT 0 CHECK (input_price_per_million >= 0),
    output_price_per_million numeric(18, 8) NOT NULL DEFAULT 0 CHECK (output_price_per_million >= 0),
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, model_config_id),
    FOREIGN KEY (tenant_id, app_id)
        REFERENCES agent_apps (tenant_id, app_id) ON DELETE CASCADE
);

COMMENT ON COLUMN model_configs.api_key_secret_ref IS
    'Reference to a secret manager entry; never store model API keys here.';

CREATE TABLE tool_policies (
    tenant_id text NOT NULL,
    policy_id uuid NOT NULL DEFAULT gen_random_uuid(),
    app_id text NOT NULL,
    config_revision text NOT NULL,
    allow_tools text[] NOT NULL DEFAULT '{}',
    deny_tools text[] NOT NULL DEFAULT '{}',
    require_approval_tools text[] NOT NULL DEFAULT '{}',
    max_calls_per_run integer NOT NULL DEFAULT 32 CHECK (max_calls_per_run > 0),
    policy jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(policy) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, policy_id),
    UNIQUE (tenant_id, app_id, config_revision),
    FOREIGN KEY (tenant_id, app_id)
        REFERENCES agent_apps (tenant_id, app_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, config_revision)
        REFERENCES tenant_config_revisions (tenant_id, revision)
);

CREATE TABLE data_backend_bindings (
    tenant_id text NOT NULL,
    backend_binding_id uuid NOT NULL DEFAULT gen_random_uuid(),
    app_id text NOT NULL,
    resource_kind text NOT NULL
        CHECK (resource_kind IN ('session', 'memory', 'summary', 'artifact', 'knowledge', 'audit')),
    backend_type text NOT NULL
        CHECK (backend_type IN ('inmemory', 'redis', 'sql', 'vector', 'object', 'external', 'stdout')),
    namespace text NOT NULL DEFAULT '',
    connection_secret_ref text NOT NULL DEFAULT '',
    settings jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(settings) = 'object'),
    state text NOT NULL DEFAULT 'active'
        CHECK (state IN ('active', 'shadow', 'migrating', 'retired')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, backend_binding_id),
    FOREIGN KEY (tenant_id, app_id)
        REFERENCES agent_apps (tenant_id, app_id) ON DELETE CASCADE
);

COMMENT ON COLUMN data_backend_bindings.connection_secret_ref IS
    'Reference to credentials in a secret manager; never store a DSN password here.';

CREATE TABLE channel_bindings (
    tenant_id text NOT NULL,
    binding_id text NOT NULL,
    app_id text NOT NULL,
    channel_type text NOT NULL
        CHECK (channel_type IN ('telegram', 'slack', 'wechat_work', 'wechat_official', 'wechat_service', 'api', 'other')),
    external_account_id text,
    webhook_path text NOT NULL,
    token_secret_ref text NOT NULL,
    signing_secret_ref text NOT NULL,
    settings jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(settings) = 'object'),
    enabled boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, channel_type, binding_id),
    UNIQUE (channel_type, binding_id),
    UNIQUE (channel_type, webhook_path),
    UNIQUE (tenant_id, channel_type, external_account_id),
    FOREIGN KEY (tenant_id, app_id)
        REFERENCES agent_apps (tenant_id, app_id) ON DELETE CASCADE
);

COMMENT ON COLUMN channel_bindings.token_secret_ref IS
    'Secret manager reference only; provider tokens must not be stored in this table.';
COMMENT ON COLUMN channel_bindings.signing_secret_ref IS
    'Secret manager reference only; webhook secrets must not be stored in this table.';

CREATE TABLE channel_user_mappings (
    tenant_id text NOT NULL,
    channel_type text NOT NULL,
    binding_id text NOT NULL,
    external_user_hash text NOT NULL,
    principal_id text NOT NULL,
    display_name_redacted text NOT NULL DEFAULT '',
    attributes jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(attributes) = 'object'),
    first_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, channel_type, binding_id, external_user_hash),
    UNIQUE (tenant_id, channel_type, binding_id, principal_id),
    FOREIGN KEY (tenant_id, channel_type, binding_id)
        REFERENCES channel_bindings (tenant_id, channel_type, binding_id) ON DELETE CASCADE
);

CREATE TABLE sessions (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    app_id text NOT NULL,
    channel_type text,
    binding_id text,
    principal_id text NOT NULL,
    conversation_hash text NOT NULL,
    scope text NOT NULL CHECK (scope IN ('direct', 'group', 'system')),
    thread_hash text NOT NULL DEFAULT '',
    status text NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'closed', 'expired', 'quarantined')),
    state jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(state) = 'object'),
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    last_event_sequence bigint NOT NULL DEFAULT 0 CHECK (last_event_sequence >= 0),
    expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, session_id),
    FOREIGN KEY (tenant_id, app_id)
        REFERENCES agent_apps (tenant_id, app_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, channel_type, binding_id)
        REFERENCES channel_bindings (tenant_id, channel_type, binding_id),
    UNIQUE (tenant_id, app_id, channel_type, binding_id, principal_id, conversation_hash, thread_hash)
);

CREATE TABLE inbox_messages (
    tenant_id text NOT NULL,
    inbox_id uuid NOT NULL DEFAULT gen_random_uuid(),
    channel_type text NOT NULL,
    binding_id text NOT NULL,
    external_message_id text NOT NULL,
    dedup_key text NOT NULL,
    payload_sha256 text NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    payload_ciphertext bytea,
    payload_metadata jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(payload_metadata) = 'object'),
    trace_id text NOT NULL DEFAULT '',
    status text NOT NULL DEFAULT 'received'
        CHECK (status IN ('received', 'processing', 'processed', 'ignored', 'retry', 'dead_letter')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    lease_owner text NOT NULL DEFAULT '',
    lease_expires_at timestamptz,
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    processed_at timestamptz,
    last_error_type text NOT NULL DEFAULT '',
    received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, inbox_id),
    UNIQUE (tenant_id, channel_type, binding_id, external_message_id),
    UNIQUE (tenant_id, dedup_key),
    FOREIGN KEY (tenant_id, channel_type, binding_id)
        REFERENCES channel_bindings (tenant_id, channel_type, binding_id) ON DELETE CASCADE
);

CREATE TABLE session_events (
    tenant_id text NOT NULL,
    event_id uuid NOT NULL DEFAULT gen_random_uuid(),
    session_id text NOT NULL,
    inbox_id uuid,
    sequence_no bigint NOT NULL CHECK (sequence_no > 0),
    event_type text NOT NULL
        CHECK (event_type IN ('message', 'tool_call', 'tool_result', 'state_delta', 'system', 'error')),
    role text NOT NULL DEFAULT ''
        CHECK (role IN ('', 'user', 'assistant', 'tool', 'system')),
    author text NOT NULL DEFAULT '',
    content_ciphertext bytea,
    content_redacted text NOT NULL DEFAULT '',
    content_sha256 text NOT NULL DEFAULT '',
    state_delta jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(state_delta) = 'object'),
    tool_call_id text NOT NULL DEFAULT '',
    tool_name text NOT NULL DEFAULT '',
    request_id text NOT NULL DEFAULT '',
    trace_id text NOT NULL DEFAULT '',
    filter_key text NOT NULL DEFAULT '',
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, event_id),
    UNIQUE (tenant_id, session_id, sequence_no),
    FOREIGN KEY (tenant_id, session_id)
        REFERENCES sessions (tenant_id, session_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, inbox_id)
        REFERENCES inbox_messages (tenant_id, inbox_id)
);

CREATE TABLE memories (
    tenant_id text NOT NULL,
    memory_id uuid NOT NULL DEFAULT gen_random_uuid(),
    app_id text NOT NULL,
    principal_id text NOT NULL,
    kind text NOT NULL DEFAULT 'fact' CHECK (kind IN ('fact', 'episode')),
    content_ciphertext bytea,
    content_redacted text NOT NULL DEFAULT '',
    content_sha256 text NOT NULL,
    topics text[] NOT NULL DEFAULT '{}',
    embedding_ref text NOT NULL DEFAULT '',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(metadata) = 'object'),
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    event_time timestamptz,
    deleted_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, memory_id),
    FOREIGN KEY (tenant_id, app_id)
        REFERENCES agent_apps (tenant_id, app_id) ON DELETE CASCADE
);

CREATE TABLE session_summaries (
    tenant_id text NOT NULL,
    summary_id uuid NOT NULL DEFAULT gen_random_uuid(),
    session_id text NOT NULL,
    filter_key text NOT NULL DEFAULT '',
    revision bigint NOT NULL CHECK (revision > 0),
    through_sequence bigint NOT NULL CHECK (through_sequence >= 0),
    summary_ciphertext bytea,
    summary_redacted text NOT NULL DEFAULT '',
    summary_sha256 text NOT NULL,
    model_config_id uuid,
    token_count integer NOT NULL DEFAULT 0 CHECK (token_count >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, summary_id),
    UNIQUE (tenant_id, session_id, filter_key, revision),
    FOREIGN KEY (tenant_id, session_id)
        REFERENCES sessions (tenant_id, session_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, model_config_id)
        REFERENCES model_configs (tenant_id, model_config_id)
);

CREATE TABLE artifacts (
    tenant_id text NOT NULL,
    artifact_id uuid NOT NULL DEFAULT gen_random_uuid(),
    session_id text NOT NULL,
    filename text NOT NULL,
    revision integer NOT NULL CHECK (revision >= 0),
    media_type text NOT NULL DEFAULT 'application/octet-stream',
    size_bytes bigint NOT NULL DEFAULT 0 CHECK (size_bytes >= 0),
    sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    object_uri text NOT NULL,
    encryption_key_ref text NOT NULL DEFAULT '',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(metadata) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, artifact_id),
    UNIQUE (tenant_id, session_id, filename, revision),
    FOREIGN KEY (tenant_id, session_id)
        REFERENCES sessions (tenant_id, session_id) ON DELETE CASCADE
);

CREATE TABLE knowledge_documents (
    tenant_id text NOT NULL,
    document_id uuid NOT NULL DEFAULT gen_random_uuid(),
    app_id text NOT NULL,
    external_key text NOT NULL,
    title text NOT NULL DEFAULT '',
    source_uri text NOT NULL DEFAULT '',
    content_sha256 text NOT NULL,
    object_uri text NOT NULL DEFAULT '',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(metadata) = 'object'),
    status text NOT NULL DEFAULT 'ready'
        CHECK (status IN ('pending', 'ready', 'failed', 'deleted')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, document_id),
    UNIQUE (tenant_id, app_id, external_key),
    FOREIGN KEY (tenant_id, app_id)
        REFERENCES agent_apps (tenant_id, app_id) ON DELETE CASCADE
);

CREATE TABLE knowledge_chunks (
    tenant_id text NOT NULL,
    chunk_id uuid NOT NULL DEFAULT gen_random_uuid(),
    document_id uuid NOT NULL,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    content_redacted text NOT NULL DEFAULT '',
    content_sha256 text NOT NULL,
    vector_ref text NOT NULL DEFAULT '',
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(metadata) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, chunk_id),
    UNIQUE (tenant_id, document_id, ordinal),
    FOREIGN KEY (tenant_id, document_id)
        REFERENCES knowledge_documents (tenant_id, document_id) ON DELETE CASCADE
);

CREATE TABLE outbox_messages (
    tenant_id text NOT NULL,
    outbox_id uuid NOT NULL DEFAULT gen_random_uuid(),
    operation_key text NOT NULL,
    operation_version smallint NOT NULL DEFAULT 1 CHECK (operation_version > 0),
    part_index integer NOT NULL DEFAULT 0 CHECK (part_index >= 0),
    part_count integer NOT NULL DEFAULT 1 CHECK (part_count > 0 AND part_index < part_count),
    channel_type text NOT NULL,
    binding_id text NOT NULL,
    session_id text NOT NULL,
    event_id uuid,
    dedup_key text NOT NULL,
    target_hash text NOT NULL,
    message_kind text NOT NULL DEFAULT 'text'
        CHECK (message_kind IN ('text', 'stream', 'card', 'image', 'file', 'reaction', 'recall')),
    payload_ciphertext bytea,
    payload_metadata jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(payload_metadata) = 'object'),
    payload_sha256 text NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'sending', 'sent', 'retry', 'dead_letter', 'cancelled')),
    delivery_state text NOT NULL DEFAULT 'pending'
        CHECK (delivery_state IN (
            'pending', 'in_flight', 'confirmed', 'retryable_not_sent',
            'permanent_rejected', 'unknown', 'retry_exhausted', 'canceled'
        )),
    state_version bigint NOT NULL DEFAULT 0 CHECK (state_version >= 0),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    lease_owner text NOT NULL DEFAULT '',
    lease_expires_at timestamptz,
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    provider_code text NOT NULL DEFAULT '',
    provider_message_id text NOT NULL DEFAULT '',
    provider_request_id text NOT NULL DEFAULT '',
    response_sha256 text NOT NULL DEFAULT ''
        CHECK (response_sha256 = '' OR response_sha256 ~ '^[0-9a-f]{64}$'),
    last_error_type text NOT NULL DEFAULT '',
    trace_id text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    sent_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, outbox_id),
    UNIQUE (tenant_id, dedup_key),
    UNIQUE (tenant_id, operation_key),
    FOREIGN KEY (tenant_id, channel_type, binding_id)
        REFERENCES channel_bindings (tenant_id, channel_type, binding_id),
    FOREIGN KEY (tenant_id, session_id)
        REFERENCES sessions (tenant_id, session_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, event_id)
        REFERENCES session_events (tenant_id, event_id)
);

CREATE TABLE outbox_delivery_attempts (
    tenant_id text NOT NULL,
    outbox_id uuid NOT NULL,
    attempt_no integer NOT NULL CHECK (attempt_no > 0),
    operation_key text NOT NULL,
    lease_owner text NOT NULL,
    phase text NOT NULL CHECK (phase IN ('leased', 'dispatched', 'finished')),
    outcome text NOT NULL DEFAULT ''
        CHECK (outcome IN ('', 'confirmed', 'retryable_not_sent', 'permanent_rejected', 'unknown')),
    error_type text NOT NULL DEFAULT '',
    provider_code text NOT NULL DEFAULT '',
    http_status integer NOT NULL DEFAULT 0 CHECK (http_status BETWEEN 0 AND 999),
    provider_message_id text NOT NULL DEFAULT '',
    provider_request_id text NOT NULL DEFAULT '',
    response_sha256 text NOT NULL DEFAULT ''
        CHECK (response_sha256 = '' OR response_sha256 ~ '^[0-9a-f]{64}$'),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    dispatched_at timestamptz,
    finished_at timestamptz,
    PRIMARY KEY (tenant_id, outbox_id, attempt_no),
    FOREIGN KEY (tenant_id, outbox_id)
        REFERENCES outbox_messages (tenant_id, outbox_id) ON DELETE CASCADE
);

CREATE TABLE outbox_delivery_resolutions (
    tenant_id text NOT NULL,
    resolution_id uuid NOT NULL,
    outbox_id uuid NOT NULL,
    expected_version bigint NOT NULL CHECK (expected_version >= 0),
    expected_attempt integer NOT NULL CHECK (expected_attempt > 0),
    action text NOT NULL CHECK (action IN ('assume_delivered', 'retry', 'cancel')),
    actor_hash text NOT NULL,
    reason_redacted text NOT NULL,
    resulting_status text NOT NULL,
    resulting_delivery_state text NOT NULL,
    resulting_version bigint NOT NULL CHECK (resulting_version > expected_version),
    resolved_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, resolution_id),
    FOREIGN KEY (tenant_id, outbox_id)
        REFERENCES outbox_messages (tenant_id, outbox_id) ON DELETE CASCADE
);

CREATE TABLE tool_approvals (
    tenant_id text NOT NULL,
    approval_id uuid NOT NULL DEFAULT gen_random_uuid(),
    session_id text NOT NULL,
    request_id text NOT NULL,
    tool_call_id text NOT NULL,
    tool_name text NOT NULL,
    arguments_sha256 text NOT NULL CHECK (arguments_sha256 ~ '^[0-9a-f]{64}$'),
    nonce_sha256 text NOT NULL CHECK (nonce_sha256 ~ '^[0-9a-f]{64}$'),
    requested_by_hash text NOT NULL,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'approved', 'denied', 'expired', 'consumed')),
    decision text NOT NULL DEFAULT '',
    reason_redacted text NOT NULL DEFAULT '',
    decided_by_hash text NOT NULL DEFAULT '',
    expires_at timestamptz NOT NULL,
    decided_at timestamptz,
    consumed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, approval_id),
    UNIQUE (tenant_id, tool_call_id),
    FOREIGN KEY (tenant_id, session_id)
        REFERENCES sessions (tenant_id, session_id) ON DELETE CASCADE
);

CREATE TABLE model_usage (
    tenant_id text NOT NULL,
    usage_id uuid NOT NULL DEFAULT gen_random_uuid(),
    session_id text NOT NULL,
    request_id text NOT NULL,
    model_config_id uuid,
    prompt_tokens bigint NOT NULL DEFAULT 0 CHECK (prompt_tokens >= 0),
    completion_tokens bigint NOT NULL DEFAULT 0 CHECK (completion_tokens >= 0),
    cost_usd numeric(18, 8) NOT NULL DEFAULT 0 CHECK (cost_usd >= 0),
    latency_ms bigint NOT NULL DEFAULT 0 CHECK (latency_ms >= 0),
    error_type text NOT NULL DEFAULT '',
    trace_id text NOT NULL DEFAULT '',
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, usage_id),
    UNIQUE (tenant_id, request_id),
    FOREIGN KEY (tenant_id, session_id)
        REFERENCES sessions (tenant_id, session_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, model_config_id)
        REFERENCES model_configs (tenant_id, model_config_id)
);

CREATE TABLE audit_logs (
    tenant_id text NOT NULL,
    audit_id uuid NOT NULL DEFAULT gen_random_uuid(),
    occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    channel text NOT NULL,
    binding_id text NOT NULL DEFAULT '',
    user_id text NOT NULL,
    session_id text NOT NULL,
    agent_name text NOT NULL,
    tool_name text NOT NULL DEFAULT '',
    decision text NOT NULL,
    reason_redacted text NOT NULL DEFAULT '',
    latency_ms bigint NOT NULL DEFAULT 0 CHECK (latency_ms >= 0),
    error_type text NOT NULL DEFAULT '',
    cost_usd numeric(18, 8) NOT NULL DEFAULT 0 CHECK (cost_usd >= 0),
    trace_id text NOT NULL DEFAULT '',
    request_id text NOT NULL DEFAULT '',
    config_revision text,
    content_sha256 text NOT NULL DEFAULT '',
    tool_arguments_sha256 text NOT NULL DEFAULT '',
    attributes jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(attributes) = 'object'),
    PRIMARY KEY (tenant_id, audit_id),
    FOREIGN KEY (tenant_id, config_revision)
        REFERENCES tenant_config_revisions (tenant_id, revision)
);

CREATE TABLE migration_checkpoints (
    tenant_id text NOT NULL,
    migration_id uuid NOT NULL DEFAULT gen_random_uuid(),
    app_id text NOT NULL,
    resource_kind text NOT NULL
        CHECK (resource_kind IN ('session', 'event', 'memory', 'summary', 'artifact', 'knowledge', 'audit')),
    source_backend text NOT NULL,
    target_backend text NOT NULL,
    phase text NOT NULL DEFAULT 'snapshot'
        CHECK (phase IN ('prepare', 'snapshot', 'backfill', 'catch_up', 'dual_write', 'shadow_read', 'canary', 'verify', 'cutover', 'drain', 'finalize', 'rollback', 'complete', 'failed')),
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'paused', 'complete', 'failed', 'rolled_back')),
    checkpoint jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(checkpoint) = 'object'),
    last_source_key text NOT NULL DEFAULT '',
    copied_count bigint NOT NULL DEFAULT 0 CHECK (copied_count >= 0),
    verified_count bigint NOT NULL DEFAULT 0 CHECK (verified_count >= 0),
    failed_count bigint NOT NULL DEFAULT 0 CHECK (failed_count >= 0),
    source_checksum text NOT NULL DEFAULT '',
    target_checksum text NOT NULL DEFAULT '',
    dual_write_started_at timestamptz,
    cutover_at timestamptz,
    last_error_type text NOT NULL DEFAULT '',
    lease_owner text NOT NULL DEFAULT '',
    lease_expires_at timestamptz,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (tenant_id, migration_id),
    UNIQUE (tenant_id, app_id, resource_kind, source_backend, target_backend, started_at),
    FOREIGN KEY (tenant_id, app_id)
        REFERENCES agent_apps (tenant_id, app_id) ON DELETE CASCADE
);

CREATE INDEX tenant_config_revision_status_idx
    ON tenant_config_revisions (tenant_id, status, created_at DESC);
CREATE INDEX model_configs_active_idx
    ON model_configs (tenant_id, app_id) WHERE enabled;
CREATE UNIQUE INDEX data_backend_binding_state_idx
    ON data_backend_bindings (tenant_id, app_id, resource_kind, state)
    WHERE state <> 'retired';
CREATE INDEX sessions_principal_idx
    ON sessions (tenant_id, app_id, principal_id, updated_at DESC);
CREATE INDEX sessions_expiry_idx
    ON sessions (expires_at) WHERE expires_at IS NOT NULL;
CREATE INDEX inbox_dispatch_idx
    ON inbox_messages (status, next_attempt_at, received_at)
    WHERE status IN ('received', 'retry');
CREATE INDEX inbox_lease_idx
    ON inbox_messages (lease_expires_at) WHERE status = 'processing';
CREATE INDEX session_events_read_idx
    ON session_events (tenant_id, session_id, sequence_no);
CREATE INDEX memories_principal_idx
    ON memories (tenant_id, app_id, principal_id, updated_at DESC)
    WHERE deleted_at IS NULL;
CREATE INDEX summaries_latest_idx
    ON session_summaries (tenant_id, session_id, filter_key, revision DESC);
CREATE INDEX knowledge_chunks_document_idx
    ON knowledge_chunks (tenant_id, document_id, ordinal);
CREATE INDEX outbox_dispatch_idx
    ON outbox_messages (status, next_attempt_at, created_at)
    WHERE status IN ('pending', 'retry');
CREATE INDEX outbox_lease_idx
    ON outbox_messages (lease_expires_at) WHERE status = 'sending';
CREATE INDEX outbox_unknown_idx
    ON outbox_messages (tenant_id, updated_at, outbox_id)
    WHERE status = 'sending' AND delivery_state = 'unknown';
CREATE INDEX outbox_attempt_phase_idx
    ON outbox_delivery_attempts (tenant_id, phase, started_at)
    WHERE phase <> 'finished';
CREATE INDEX outbox_resolution_time_idx
    ON outbox_delivery_resolutions (tenant_id, outbox_id, resolved_at DESC);
CREATE INDEX approvals_pending_idx
    ON tool_approvals (tenant_id, session_id, expires_at)
    WHERE status = 'pending';
CREATE INDEX model_usage_cost_idx
    ON model_usage (tenant_id, occurred_at DESC);
CREATE INDEX audit_tenant_time_idx
    ON audit_logs (tenant_id, occurred_at DESC);
CREATE INDEX audit_trace_idx
    ON audit_logs (tenant_id, trace_id) WHERE trace_id <> '';
CREATE INDEX migration_active_idx
    ON migration_checkpoints (tenant_id, app_id, resource_kind, updated_at)
    WHERE status IN ('pending', 'running', 'paused');

CREATE OR REPLACE FUNCTION platform_touch_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = clock_timestamp();
    RETURN NEW;
END;
$$;

DO $$
DECLARE
    target_table text;
BEGIN
    FOREACH target_table IN ARRAY ARRAY[
        'tenants', 'agent_apps', 'model_configs', 'data_backend_bindings',
        'channel_bindings', 'sessions', 'inbox_messages', 'memories',
        'knowledge_documents', 'outbox_messages', 'tool_approvals',
        'migration_checkpoints'
    ]
    LOOP
        EXECUTE format(
            'CREATE TRIGGER touch_updated_at BEFORE UPDATE ON %I '
            'FOR EACH ROW EXECUTE FUNCTION platform_touch_updated_at()',
            target_table
        );
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION platform_current_tenant_id()
RETURNS text
LANGUAGE sql
STABLE
AS $$
    SELECT NULLIF(current_setting('app.tenant_id', true), '')
$$;

DO $$
DECLARE
    target_table text;
BEGIN
    FOREACH target_table IN ARRAY ARRAY[
        'tenants', 'tenant_config_revisions', 'agent_apps', 'model_configs',
        'tool_policies', 'data_backend_bindings', 'channel_bindings',
        'channel_user_mappings', 'sessions', 'inbox_messages', 'session_events',
        'memories', 'session_summaries', 'artifacts', 'knowledge_documents',
        'knowledge_chunks', 'outbox_messages', 'outbox_delivery_attempts',
        'outbox_delivery_resolutions', 'tool_approvals', 'model_usage',
        'audit_logs', 'migration_checkpoints'
    ]
    LOOP
        EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', target_table);
        EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', target_table);
        EXECUTE format(
            'CREATE POLICY tenant_isolation ON %I '
            'USING (tenant_id = platform_current_tenant_id()) '
            'WITH CHECK (tenant_id = platform_current_tenant_id())',
            target_table
        );
    END LOOP;
END;
$$;

-- Runtime connections must assume this non-login, non-bypass role (or an
-- equivalently constrained role). Migration/owner credentials are never used
-- by the application data plane.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tenant_agent_runtime') THEN
        CREATE ROLE tenant_agent_runtime NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS;
    END IF;
END;
$$;

ALTER ROLE tenant_agent_runtime NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS;
GRANT USAGE ON SCHEMA public TO tenant_agent_runtime;
GRANT SELECT ON tenants, tenant_config_revisions, agent_apps, model_configs,
    tool_policies, data_backend_bindings, channel_bindings TO tenant_agent_runtime;
GRANT SELECT, INSERT, UPDATE ON channel_user_mappings, sessions,
    inbox_messages, memories, session_summaries, artifacts, outbox_messages,
    outbox_delivery_attempts, tool_approvals TO tenant_agent_runtime;
GRANT SELECT, INSERT ON session_events, outbox_delivery_resolutions,
    model_usage, audit_logs TO tenant_agent_runtime;
GRANT SELECT ON knowledge_documents, knowledge_chunks TO tenant_agent_runtime;

-- The migration executor, not the migration itself, records the actual file
-- SHA-256 in schema_migrations after a successful commit. A self-embedded hash
-- would change the file being hashed and cannot detect content drift.

COMMIT;
