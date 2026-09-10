CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SCHEMA IF NOT EXISTS platform;

CREATE TABLE platform.tenant (
    tenant_id TEXT PRIMARY KEY,
    name TEXT NOT NULL CHECK (name <> ''),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'SUSPENDED')),
    audit_policy JSONB NOT NULL DEFAULT '{}'::JSONB CHECK (jsonb_typeof(audit_policy) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE platform.agent_app (
    tenant_id TEXT NOT NULL REFERENCES platform.tenant (tenant_id),
    app_id TEXT NOT NULL CHECK (app_id <> ''),
    name TEXT NOT NULL CHECK (name <> ''),
    active_config_version TEXT NOT NULL CHECK (active_config_version <> ''),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'SUSPENDED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id)
);

CREATE TABLE platform.app_config_version (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    version TEXT NOT NULL CHECK (version <> ''),
    model_config JSONB NOT NULL CHECK (jsonb_typeof(model_config) = 'object'),
    tool_policy JSONB NOT NULL CHECK (jsonb_typeof(tool_policy) = 'object'),
    backend_config JSONB NOT NULL CHECK (jsonb_typeof(backend_config) = 'object'),
    audit_policy JSONB NOT NULL CHECK (jsonb_typeof(audit_policy) = 'object'),
    secret_refs JSONB NOT NULL DEFAULT '[]'::JSONB CHECK (jsonb_typeof(secret_refs) IN ('array', 'null')),
    channel_binding_ids JSONB NOT NULL DEFAULT '[]'::JSONB CHECK (jsonb_typeof(channel_binding_ids) IN ('array', 'null')),
    knowledge_base_ids JSONB NOT NULL DEFAULT '[]'::JSONB CHECK (jsonb_typeof(knowledge_base_ids) = 'array'),
    status TEXT NOT NULL DEFAULT 'PUBLISHED' CHECK (status = 'PUBLISHED'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, version),
    FOREIGN KEY (tenant_id, app_id) REFERENCES platform.agent_app (tenant_id, app_id)
);

ALTER TABLE platform.agent_app
    ADD CONSTRAINT agent_app_active_config_fk
    FOREIGN KEY (tenant_id, app_id, active_config_version)
    REFERENCES platform.app_config_version (tenant_id, app_id, version)
    DEFERRABLE INITIALLY DEFERRED;

CREATE FUNCTION platform.reject_app_config_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'published app config versions are immutable' USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER app_config_version_immutable
    BEFORE UPDATE OR DELETE ON platform.app_config_version
    FOR EACH ROW EXECUTE FUNCTION platform.reject_app_config_mutation();

CREATE TABLE platform.api_credential (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    credential_id TEXT NOT NULL CHECK (credential_id <> ''),
    key_digest BYTEA NOT NULL UNIQUE CHECK (octet_length(key_digest) = 32),
    key_prefix TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'SUSPENDED', 'REVOKED')),
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, app_id, credential_id),
    FOREIGN KEY (tenant_id, app_id) REFERENCES platform.agent_app (tenant_id, app_id)
);

CREATE FUNCTION platform.reject_credential_reactivation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.status = 'REVOKED' AND NEW.status <> 'REVOKED' THEN
        RAISE EXCEPTION 'revoked api credential cannot be reactivated' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER api_credential_revocation_immutable
    BEFORE UPDATE ON platform.api_credential
    FOR EACH ROW EXECUTE FUNCTION platform.reject_credential_reactivation();

CREATE TABLE platform.channel_binding (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    binding_id TEXT NOT NULL CHECK (binding_id <> ''),
    channel TEXT NOT NULL CHECK (channel <> ''),
    external_account TEXT NOT NULL CHECK (external_account <> ''),
    external_account_scope TEXT NOT NULL DEFAULT '',
    webhook_url TEXT NOT NULL DEFAULT '',
    token_ref JSONB NOT NULL DEFAULT '{}'::JSONB,
    signing_secret_ref JSONB NOT NULL DEFAULT '{}'::JSONB,
    secret_ref JSONB NOT NULL DEFAULT '{}'::JSONB,
    public_route_id TEXT NOT NULL DEFAULT (
        'r_' || translate(rtrim(encode(gen_random_bytes(16), 'base64'), '='), '+/', '-_')
    ),
    binding_revision BIGINT NOT NULL DEFAULT 1,
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'SUSPENDED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, binding_id),
    FOREIGN KEY (tenant_id, app_id) REFERENCES platform.agent_app (tenant_id, app_id),
    CHECK (public_route_id <> ''),
    CHECK (binding_revision > 0)
);

CREATE UNIQUE INDEX channel_binding_public_route_uidx
    ON platform.channel_binding (public_route_id);

CREATE FUNCTION platform.bump_channel_binding_revision()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
       OR NEW.app_id IS DISTINCT FROM OLD.app_id
       OR NEW.binding_id IS DISTINCT FROM OLD.binding_id THEN
        RAISE EXCEPTION 'channel binding scope is immutable' USING ERRCODE = '55000';
    END IF;

    IF NEW.channel IS DISTINCT FROM OLD.channel
       OR NEW.external_account IS DISTINCT FROM OLD.external_account
       OR NEW.external_account_scope IS DISTINCT FROM OLD.external_account_scope
       OR NEW.webhook_url IS DISTINCT FROM OLD.webhook_url
       OR NEW.token_ref IS DISTINCT FROM OLD.token_ref
       OR NEW.signing_secret_ref IS DISTINCT FROM OLD.signing_secret_ref
       OR NEW.secret_ref IS DISTINCT FROM OLD.secret_ref
       OR NEW.status IS DISTINCT FROM OLD.status
       OR NEW.public_route_id IS DISTINCT FROM OLD.public_route_id THEN
        NEW.binding_revision := OLD.binding_revision + 1;
    ELSE
        NEW.binding_revision := OLD.binding_revision;
    END IF;
    NEW.updated_at := clock_timestamp();
    RETURN NEW;
END;
$$;

CREATE TRIGGER channel_binding_revision_trg
    BEFORE UPDATE ON platform.channel_binding
    FOR EACH ROW EXECUTE FUNCTION platform.bump_channel_binding_revision();

CREATE TABLE platform.session_lane (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    session_principal_id TEXT NOT NULL CHECK (session_principal_id <> ''),
    session_id TEXT NOT NULL CHECK (session_id <> ''),
    next_turn_seq BIGINT NOT NULL DEFAULT 1 CHECK (next_turn_seq > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, session_principal_id, session_id),
    FOREIGN KEY (tenant_id, app_id) REFERENCES platform.agent_app (tenant_id, app_id)
);

CREATE TABLE platform.execution (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    request_id TEXT NOT NULL CHECK (request_id <> ''),
    session_principal_id TEXT NOT NULL CHECK (session_principal_id <> ''),
    session_id TEXT NOT NULL CHECK (session_id <> ''),
    user_id TEXT NOT NULL CHECK (user_id <> ''),
    turn_seq BIGINT NOT NULL CHECK (turn_seq > 0),
    config_version TEXT NOT NULL CHECK (config_version <> ''),
    tenant_source TEXT NOT NULL CHECK (tenant_source IN ('authenticated_claims', 'verified_channel_binding')),
    source_id TEXT NOT NULL CHECK (source_id <> ''),
    idempotency_key TEXT NOT NULL CHECK (idempotency_key <> ''),
    payload_hash BYTEA NOT NULL CHECK (octet_length(payload_hash) = 32),
    command JSONB NOT NULL CHECK (jsonb_typeof(command) = 'object'),
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELED')),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner TEXT,
    run_token TEXT,
    lease_until TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    trace_id TEXT NOT NULL,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, request_id),
    UNIQUE (tenant_id, app_id, tenant_source, source_id, idempotency_key),
    UNIQUE (tenant_id, app_id, session_principal_id, session_id, turn_seq),
    FOREIGN KEY (tenant_id, app_id, session_principal_id, session_id)
        REFERENCES platform.session_lane (tenant_id, app_id, session_principal_id, session_id),
    FOREIGN KEY (tenant_id, app_id, config_version)
        REFERENCES platform.app_config_version (tenant_id, app_id, version)
);

CREATE INDEX execution_active_lane_idx
    ON platform.execution (tenant_id, app_id, session_principal_id, session_id, turn_seq)
    WHERE status IN ('PENDING', 'RUNNING');

CREATE INDEX execution_trace_idx
    ON platform.execution (tenant_id, app_id, trace_id);

CREATE TABLE platform.dispatch_outbox (
    outbox_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'PUBLISHING', 'SENT', 'CONSUMED')),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner TEXT,
    lease_until TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (tenant_id, app_id, request_id)
        REFERENCES platform.execution (tenant_id, app_id, request_id)
);

CREATE INDEX dispatch_outbox_publish_idx
    ON platform.dispatch_outbox (status, next_attempt_at, outbox_id);

CREATE TABLE platform.execution_event (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    event_seq BIGINT NOT NULL CHECK (event_seq > 0),
    event_type TEXT NOT NULL CHECK (event_type <> ''),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, request_id, event_seq),
    FOREIGN KEY (tenant_id, app_id, request_id)
        REFERENCES platform.execution (tenant_id, app_id, request_id)
);

CREATE TABLE platform.data_migration (
    migration_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    source_config_version TEXT NOT NULL,
    target_config_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'DRAINING', 'COPYING', 'VERIFYING', 'SUCCEEDED', 'FAILED')),
    lease_owner TEXT,
    lease_until TIMESTAMPTZ,
    run_token TEXT,
    drain_deadline TIMESTAMPTZ,
    failure_reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((lease_owner IS NULL AND lease_until IS NULL AND run_token IS NULL)
        OR (lease_owner IS NOT NULL AND lease_until IS NOT NULL AND run_token IS NOT NULL)),
    CHECK (source_config_version <> target_config_version),
    FOREIGN KEY (tenant_id, app_id) REFERENCES platform.agent_app (tenant_id, app_id)
);

CREATE UNIQUE INDEX data_migration_active_scope_idx
    ON platform.data_migration (tenant_id, app_id)
    WHERE status IN ('PENDING', 'DRAINING', 'COPYING', 'VERIFYING');

CREATE INDEX data_migration_admission_gate_idx
    ON platform.data_migration (tenant_id, app_id, status);

CREATE TABLE platform.artifact (
    artifact_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    session_principal_id TEXT NOT NULL CHECK (session_principal_id <> ''),
    session_id TEXT NOT NULL CHECK (session_id <> ''),
    filename TEXT NOT NULL CHECK (filename <> ''),
    version INTEGER NOT NULL CHECK (version >= 0),
    object_key TEXT NOT NULL DEFAULT '',
    mime_type TEXT NOT NULL DEFAULT '',
    size_bytes BIGINT NOT NULL CHECK (size_bytes >= 0),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'AVAILABLE', 'DELETED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, app_id, session_principal_id, session_id, filename, version),
    FOREIGN KEY (tenant_id, app_id, session_principal_id, session_id)
        REFERENCES platform.session_lane (tenant_id, app_id, session_principal_id, session_id),
    CHECK (status = 'PENDING' OR object_key <> '')
);

CREATE INDEX artifact_session_available_idx
    ON platform.artifact (tenant_id, app_id, session_principal_id, session_id, filename, version DESC)
    WHERE status = 'AVAILABLE';

CREATE TABLE platform.knowledge_base (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    knowledge_base_id TEXT NOT NULL CHECK (knowledge_base_id <> ''),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'DELETED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, knowledge_base_id),
    FOREIGN KEY (tenant_id, app_id) REFERENCES platform.agent_app (tenant_id, app_id)
);

CREATE TABLE platform.knowledge_document (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    knowledge_base_id TEXT NOT NULL CHECK (knowledge_base_id <> ''),
    document_id TEXT NOT NULL CHECK (document_id <> ''),
    version INTEGER NOT NULL CHECK (version >= 0),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'AVAILABLE', 'DELETED')),
    index_generation TEXT NOT NULL CHECK (index_generation <> ''),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, knowledge_base_id, document_id, version),
    FOREIGN KEY (tenant_id, app_id, knowledge_base_id)
        REFERENCES platform.knowledge_base (tenant_id, app_id, knowledge_base_id)
);

CREATE INDEX knowledge_document_available_idx
    ON platform.knowledge_document (tenant_id, app_id, knowledge_base_id, document_id, version DESC)
    WHERE status = 'AVAILABLE';

CREATE TABLE platform.knowledge_chunk (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    knowledge_base_id TEXT NOT NULL CHECK (knowledge_base_id <> ''),
    document_id TEXT NOT NULL CHECK (document_id <> ''),
    document_version INTEGER NOT NULL CHECK (document_version >= 0),
    index_generation TEXT NOT NULL CHECK (index_generation <> ''),
    chunk_id TEXT NOT NULL CHECK (chunk_id <> ''),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'AVAILABLE', 'DELETED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        tenant_id, app_id, knowledge_base_id, document_id, document_version,
        index_generation, chunk_id
    ),
    FOREIGN KEY (tenant_id, app_id, knowledge_base_id, document_id, document_version)
        REFERENCES platform.knowledge_document (tenant_id, app_id, knowledge_base_id, document_id, version)
);

CREATE INDEX knowledge_chunk_available_idx
    ON platform.knowledge_chunk (
        tenant_id, app_id, knowledge_base_id, document_id, document_version,
        index_generation, chunk_id
    ) WHERE status = 'AVAILABLE';

CREATE TABLE platform.channel_identity (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    channel TEXT NOT NULL CHECK (channel <> ''),
    external_user_key_hash BYTEA NOT NULL CHECK (octet_length(external_user_key_hash) = 32),
    user_id TEXT NOT NULL CHECK (user_id <> ''),
    status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'SUSPENDED')),
    key_version TEXT NOT NULL CHECK (key_version <> ''),
    provider_target_envelope JSONB NOT NULL CHECK (jsonb_typeof(provider_target_envelope) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, user_id),
    UNIQUE (tenant_id, app_id, binding_id, external_user_key_hash),
    FOREIGN KEY (tenant_id, app_id, binding_id)
        REFERENCES platform.channel_binding (tenant_id, app_id, binding_id)
);

CREATE INDEX channel_identity_scope_idx
    ON platform.channel_identity (tenant_id, app_id, binding_id, user_id);

CREATE TABLE platform.channel_conversation (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    channel TEXT NOT NULL CHECK (channel <> ''),
    external_chat_key_hash BYTEA NOT NULL CHECK (octet_length(external_chat_key_hash) = 32),
    thread_key_hash BYTEA NOT NULL CHECK (octet_length(thread_key_hash) = 32),
    conversation_id TEXT NOT NULL CHECK (conversation_id <> ''),
    session_principal_id TEXT NOT NULL CHECK (session_principal_id <> ''),
    scope TEXT NOT NULL CHECK (scope IN ('group', 'topic')),
    key_version TEXT NOT NULL CHECK (key_version <> ''),
    provider_target_envelope JSONB NOT NULL CHECK (jsonb_typeof(provider_target_envelope) = 'object'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, conversation_id),
    UNIQUE (tenant_id, app_id, binding_id, external_chat_key_hash, thread_key_hash),
    FOREIGN KEY (tenant_id, app_id, binding_id)
        REFERENCES platform.channel_binding (tenant_id, app_id, binding_id)
);

CREATE INDEX channel_conversation_scope_idx
    ON platform.channel_conversation (tenant_id, app_id, binding_id, conversation_id);

CREATE TABLE platform.channel_inbox (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    external_message_id TEXT NOT NULL CHECK (external_message_id <> ''),
    payload_hash BYTEA NOT NULL CHECK (octet_length(payload_hash) = 32),
    request_id TEXT NOT NULL CHECK (request_id <> ''),
    status TEXT NOT NULL CHECK (status IN ('ADMITTED', 'REJECTED')),
    message_type TEXT NOT NULL CHECK (message_type IN ('text', 'image', 'file', 'mixed', 'card', 'event', 'unsupported')),
    reject_reason TEXT,
    provider_reply_target_envelope JSONB,
    reply_target_expires_at TIMESTAMPTZ,
    provider_timestamp TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, binding_id, external_message_id),
    FOREIGN KEY (tenant_id, app_id, binding_id)
        REFERENCES platform.channel_binding (tenant_id, app_id, binding_id),
    CHECK (
        (status = 'ADMITTED' AND reject_reason IS NULL)
        OR (status = 'REJECTED' AND reject_reason IN ('UNSUPPORTED_MESSAGE_TYPE', 'ATTACHMENT_REJECTED'))
    ),
    CHECK (
        (provider_reply_target_envelope IS NULL AND reply_target_expires_at IS NULL)
        OR (provider_reply_target_envelope IS NOT NULL AND reply_target_expires_at IS NOT NULL)
    ),
    CHECK (
        provider_reply_target_envelope IS NULL
        OR jsonb_typeof(provider_reply_target_envelope) = 'object'
    )
);

CREATE INDEX channel_inbox_request_idx
    ON platform.channel_inbox (tenant_id, app_id, request_id);

CREATE TABLE platform.reply_outbox (
    reply_id TEXT PRIMARY KEY CHECK (reply_id <> ''),
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    channel TEXT NOT NULL CHECK (channel <> ''),
    request_id TEXT NOT NULL CHECK (request_id <> ''),
    source_event_id TEXT NOT NULL CHECK (source_event_id <> ''),
    revision BIGINT NOT NULL CHECK (revision > 0),
    target_ref JSONB NOT NULL CHECK (jsonb_typeof(target_ref) = 'object'),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    status TEXT NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING', 'SENDING', 'SENT', 'PERMANENTLY_FAILED')),
    attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_owner TEXT,
    lease_until TIMESTAMPTZ,
    provider_message_id TEXT NOT NULL DEFAULT '',
    last_error_type TEXT NOT NULL DEFAULT '',
    last_error TEXT NOT NULL DEFAULT '',
    binding_revision BIGINT NOT NULL DEFAULT 1 CHECK (binding_revision > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (tenant_id, app_id, binding_id)
        REFERENCES platform.channel_binding (tenant_id, app_id, binding_id),
    FOREIGN KEY (tenant_id, app_id, request_id)
        REFERENCES platform.execution (tenant_id, app_id, request_id),
    CHECK (
        (lease_owner IS NULL AND lease_until IS NULL)
        OR (lease_owner IS NOT NULL AND lease_until IS NOT NULL)
    ),
    UNIQUE (
        tenant_id, app_id, binding_id, request_id, source_event_id,
        revision
    )
);

CREATE INDEX reply_outbox_claim_idx
    ON platform.reply_outbox (status, next_attempt_at, created_at, reply_id);

CREATE INDEX reply_outbox_order_idx
    ON platform.reply_outbox (
        tenant_id, app_id, binding_id, request_id, revision
    );

CREATE TABLE platform.inbound_artifact (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    external_message_id TEXT NOT NULL,
    item_no INTEGER NOT NULL CHECK (item_no >= 0),
    artifact_ref TEXT NOT NULL CHECK (artifact_ref LIKE 'artifact://%'),
    config_version TEXT NOT NULL,
    filename TEXT NOT NULL CHECK (filename <> ''),
    object_key TEXT NOT NULL CHECK (object_key <> ''),
    mime_type TEXT NOT NULL DEFAULT '',
    size_bytes BIGINT NOT NULL CHECK (size_bytes > 0),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'ATTACHED', 'DELETED')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, binding_id, external_message_id, item_no),
    UNIQUE (tenant_id, app_id, binding_id, external_message_id, artifact_ref),
    UNIQUE (tenant_id, app_id, artifact_ref),
    FOREIGN KEY (tenant_id, app_id, binding_id)
        REFERENCES platform.channel_binding (tenant_id, app_id, binding_id),
    FOREIGN KEY (tenant_id, app_id, config_version)
        REFERENCES platform.app_config_version (tenant_id, app_id, version)
);

CREATE INDEX inbound_artifact_pending_idx
    ON platform.inbound_artifact (status, updated_at)
    WHERE status = 'PENDING';

CREATE TABLE platform.channel_recall_inbox (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    external_event_id TEXT NOT NULL CHECK (external_event_id <> ''),
    request_id TEXT,
    payload_hash BYTEA NOT NULL CHECK (octet_length(payload_hash) = 32),
    status TEXT NOT NULL CHECK (status IN ('APPLIED', 'REJECTED')),
    reject_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, binding_id, external_event_id),
    FOREIGN KEY (tenant_id, app_id, binding_id)
        REFERENCES platform.channel_binding (tenant_id, app_id, binding_id),
    CHECK (
        (status = 'APPLIED' AND request_id IS NOT NULL AND request_id <> '' AND reject_reason IS NULL)
        OR (status = 'REJECTED' AND reject_reason IN ('REQUEST_NOT_FOUND', 'UNSUPPORTED_EVENT'))
    )
);

CREATE INDEX channel_recall_inbox_request_idx
    ON platform.channel_recall_inbox (tenant_id, app_id, request_id);
