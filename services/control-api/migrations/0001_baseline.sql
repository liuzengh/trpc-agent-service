CREATE TABLE user_accounts (
    id                  text PRIMARY KEY,
    username            text NOT NULL,
    normalized_username text NOT NULL,
    display_name        text NOT NULL DEFAULT '',
    status              text NOT NULL DEFAULT 'ACTIVE'
                        CHECK (status IN ('ACTIVE', 'DISABLED')),
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT user_accounts_normalized_username_unique
        UNIQUE (normalized_username)
);

CREATE TABLE password_credentials (
    user_id                   text PRIMARY KEY
                              REFERENCES user_accounts(id) ON DELETE CASCADE,
    encoded_hash              text NOT NULL,
    must_change_at_next_login boolean NOT NULL DEFAULT false,
    changed_at                timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE user_sessions (
    id         text PRIMARY KEY,
    user_id    text NOT NULL REFERENCES user_accounts(id) ON DELETE CASCADE,
    token_hash bytea NOT NULL,
    restricted boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    CONSTRAINT user_sessions_token_hash_unique UNIQUE (token_hash),
    CONSTRAINT user_sessions_expiry_after_creation
        CHECK (expires_at > created_at)
);

CREATE INDEX user_sessions_active_user_idx
    ON user_sessions (user_id, expires_at)
    WHERE revoked_at IS NULL;

CREATE TABLE platform_operator_grants (
    user_id               text PRIMARY KEY
                           REFERENCES user_accounts(id) ON DELETE CASCADE,
    granted_by_actor_type text NOT NULL
                           CHECK (granted_by_actor_type IN ('USER', 'SYSTEM_BOOTSTRAP')),
    granted_by_user_id    text REFERENCES user_accounts(id) ON DELETE SET NULL,
    granted_at            timestamptz NOT NULL,
    revoked_by_user_id    text REFERENCES user_accounts(id) ON DELETE SET NULL,
    revoked_at            timestamptz,
    CONSTRAINT platform_operator_grant_actor_valid CHECK (
        (granted_by_actor_type = 'SYSTEM_BOOTSTRAP' AND granted_by_user_id IS NULL)
        OR
        (granted_by_actor_type = 'USER' AND granted_by_user_id IS NOT NULL)
    )
);

CREATE INDEX platform_operator_grants_active_idx
    ON platform_operator_grants (granted_at, user_id)
    WHERE revoked_at IS NULL;

CREATE TABLE tenants (
    id         text PRIMARY KEY,
    slug       text NOT NULL,
    name       text NOT NULL,
    status     text NOT NULL DEFAULT 'ACTIVE'
               CHECK (status IN ('ACTIVE')),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT tenants_slug_unique UNIQUE (slug)
);

CREATE TABLE tenant_memberships (
    id         text PRIMARY KEY,
    tenant_id  text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id    text NOT NULL REFERENCES user_accounts(id) ON DELETE CASCADE,
    role       text NOT NULL CHECK (role IN ('OWNER', 'MEMBER')),
    created_by text NOT NULL REFERENCES user_accounts(id),
    created_at timestamptz NOT NULL,
    CONSTRAINT tenant_memberships_tenant_user_unique UNIQUE (tenant_id, user_id)
);

CREATE INDEX tenant_memberships_user_idx
    ON tenant_memberships (user_id, tenant_id);

CREATE TABLE agents (
    tenant_id            text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    id                   text NOT NULL,
    name                 text NOT NULL,
    description          text NOT NULL DEFAULT '',
    latest_version_number bigint,
    created_by           text NOT NULL REFERENCES user_accounts(id),
    created_at           timestamptz NOT NULL,
    updated_at           timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, id),
    CONSTRAINT agents_name_not_blank CHECK (btrim(name) <> ''),
    CONSTRAINT agents_latest_version_positive
        CHECK (latest_version_number IS NULL OR latest_version_number > 0)
);

CREATE INDEX agents_tenant_updated_idx
    ON agents (tenant_id, updated_at DESC, id);

CREATE TABLE agent_drafts (
    tenant_id    text NOT NULL,
    agent_id     text NOT NULL,
    spec_revision bigint NOT NULL,
    spec_jsonb   jsonb NOT NULL,
    updated_by   text NOT NULL REFERENCES user_accounts(id),
    updated_at   timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, agent_id),
    CONSTRAINT agent_drafts_agent_fk
        FOREIGN KEY (tenant_id, agent_id)
        REFERENCES agents(tenant_id, id) ON DELETE CASCADE,
    CONSTRAINT agent_drafts_revision_positive CHECK (spec_revision > 0),
    CONSTRAINT agent_drafts_spec_object CHECK (jsonb_typeof(spec_jsonb) = 'object')
);

CREATE TABLE agent_versions (
    tenant_id             text NOT NULL,
    id                    text NOT NULL,
    agent_id              text NOT NULL,
    version_number        bigint NOT NULL,
    source_draft_revision bigint NOT NULL,
    schema_version        text NOT NULL,
    spec_jsonb            jsonb NOT NULL,
    spec_digest           text NOT NULL,
    published_by          text NOT NULL REFERENCES user_accounts(id),
    published_at          timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, id),
    CONSTRAINT agent_versions_agent_fk
        FOREIGN KEY (tenant_id, agent_id)
        REFERENCES agents(tenant_id, id) ON DELETE CASCADE,
    CONSTRAINT agent_versions_number_positive CHECK (version_number > 0),
    CONSTRAINT agent_versions_source_revision_positive
        CHECK (source_draft_revision > 0),
    CONSTRAINT agent_versions_spec_object CHECK (jsonb_typeof(spec_jsonb) = 'object'),
    CONSTRAINT agent_versions_digest_format
        CHECK (spec_digest ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT agent_versions_tenant_agent_number_unique
        UNIQUE (tenant_id, agent_id, version_number),
    CONSTRAINT agent_versions_tenant_agent_source_revision_unique
        UNIQUE (tenant_id, agent_id, source_draft_revision)
);

CREATE INDEX agent_versions_tenant_agent_published_idx
    ON agent_versions (tenant_id, agent_id, published_at DESC);

CREATE FUNCTION reject_agent_version_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'agent_versions are immutable';
END
$$;

CREATE TRIGGER agent_versions_immutable
BEFORE UPDATE OR DELETE ON agent_versions
FOR EACH ROW EXECUTE FUNCTION reject_agent_version_mutation();

CREATE TABLE runtime_profiles (
    tenant_id             text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    id                    text NOT NULL,
    name                  text NOT NULL,
    description           text NOT NULL DEFAULT '',
    latest_revision_number bigint,
    created_by            text NOT NULL REFERENCES user_accounts(id),
    created_at            timestamptz NOT NULL,
    updated_at            timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, id),
    CONSTRAINT runtime_profiles_name_not_blank CHECK (btrim(name) <> ''),
    CONSTRAINT runtime_profiles_latest_revision_positive
        CHECK (latest_revision_number IS NULL OR latest_revision_number > 0)
);

CREATE INDEX runtime_profiles_tenant_updated_idx
    ON runtime_profiles (tenant_id, updated_at DESC, id);

CREATE TABLE runtime_profile_drafts (
    tenant_id     text NOT NULL,
    profile_id    text NOT NULL,
    spec_revision bigint NOT NULL,
    spec_jsonb    jsonb NOT NULL,
    updated_by    text NOT NULL REFERENCES user_accounts(id),
    updated_at    timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, profile_id),
    CONSTRAINT runtime_profile_drafts_profile_fk
        FOREIGN KEY (tenant_id, profile_id)
        REFERENCES runtime_profiles(tenant_id, id) ON DELETE CASCADE,
    CONSTRAINT runtime_profile_drafts_revision_positive CHECK (spec_revision > 0),
    CONSTRAINT runtime_profile_drafts_spec_object CHECK (jsonb_typeof(spec_jsonb) = 'object')
);

CREATE TABLE runtime_profile_revisions (
    tenant_id             text NOT NULL,
    id                    text NOT NULL,
    profile_id            text NOT NULL,
    revision_number       bigint NOT NULL,
    source_draft_revision bigint NOT NULL,
    schema_version        text NOT NULL,
    spec_jsonb            jsonb NOT NULL,
    spec_digest           text NOT NULL,
    published_by          text NOT NULL REFERENCES user_accounts(id),
    published_at          timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, id),
    CONSTRAINT runtime_profile_revisions_profile_fk
        FOREIGN KEY (tenant_id, profile_id)
        REFERENCES runtime_profiles(tenant_id, id) ON DELETE CASCADE,
    CONSTRAINT runtime_profile_revisions_number_positive CHECK (revision_number > 0),
    CONSTRAINT runtime_profile_revisions_source_revision_positive
        CHECK (source_draft_revision > 0),
    CONSTRAINT runtime_profile_revisions_spec_object
        CHECK (jsonb_typeof(spec_jsonb) = 'object'),
    CONSTRAINT runtime_profile_revisions_digest_format
        CHECK (spec_digest ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT runtime_profile_revisions_tenant_profile_number_unique
        UNIQUE (tenant_id, profile_id, revision_number),
    CONSTRAINT runtime_profile_revisions_tenant_profile_source_unique
        UNIQUE (tenant_id, profile_id, source_draft_revision)
);

CREATE INDEX runtime_profile_revisions_tenant_profile_published_idx
    ON runtime_profile_revisions (tenant_id, profile_id, published_at DESC);

CREATE FUNCTION reject_runtime_profile_revision_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'runtime_profile_revisions are immutable';
END
$$;

CREATE TRIGGER runtime_profile_revisions_immutable
BEFORE UPDATE OR DELETE ON runtime_profile_revisions
FOR EACH ROW EXECUTE FUNCTION reject_runtime_profile_revision_mutation();

-- Profile owns the current encrypted value. Published specs contain only its ID.
CREATE TABLE runtime_profile_credentials (
    tenant_id           text NOT NULL,
    profile_id          text NOT NULL,
    id                  text NOT NULL,
    category            text NOT NULL CHECK (btrim(category) <> ''),
    resource_name       text NOT NULL CHECK (btrim(resource_name) <> ''),
    purpose             text NOT NULL CHECK (btrim(purpose) <> ''),
    audience_digest     text NOT NULL CHECK (btrim(audience_digest) <> ''),
    credential_revision bigint NOT NULL CHECK (credential_revision > 0),
    status              text NOT NULL CHECK (status IN ('active', 'cleared')),
    ciphertext          bytea,
    created_by          text NOT NULL REFERENCES user_accounts(id),
    updated_by          text NOT NULL REFERENCES user_accounts(id),
    created_at          timestamptz NOT NULL,
    updated_at          timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, profile_id, id),
    CONSTRAINT runtime_profile_credentials_profile_fk
        FOREIGN KEY (tenant_id, profile_id)
        REFERENCES runtime_profiles(tenant_id, id) ON DELETE CASCADE,
    CONSTRAINT runtime_profile_credentials_value_state CHECK (
        (status = 'active' AND ciphertext IS NOT NULL AND octet_length(ciphertext) > 0)
        OR (status = 'cleared' AND ciphertext IS NULL)
    )
);

CREATE FUNCTION protect_runtime_profile_credential_identity() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.tenant_id, NEW.profile_id, NEW.id, NEW.category, NEW.resource_name,
           NEW.purpose, NEW.audience_digest, NEW.created_by, NEW.created_at)
       IS DISTINCT FROM
       ROW(OLD.tenant_id, OLD.profile_id, OLD.id, OLD.category, OLD.resource_name,
           OLD.purpose, OLD.audience_digest, OLD.created_by, OLD.created_at) THEN
        RAISE EXCEPTION 'profile credential identity and purpose are immutable';
    END IF;
    IF OLD.status <> 'active' THEN
        RAISE EXCEPTION 'cleared profile credential is terminal';
    END IF;
    IF NEW.credential_revision <> OLD.credential_revision + 1 THEN
        RAISE EXCEPTION 'profile credential revision must advance by one';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER runtime_profile_credentials_protected
BEFORE UPDATE ON runtime_profile_credentials
FOR EACH ROW EXECUTE FUNCTION protect_runtime_profile_credential_identity();

CREATE TABLE runtime_profile_credential_receipts (
    tenant_id       text NOT NULL,
    profile_id      text NOT NULL,
    actor_user_id   text NOT NULL REFERENCES user_accounts(id),
    idempotency_key text NOT NULL CHECK (btrim(idempotency_key) <> ''),
    request_mac     text NOT NULL CHECK (btrim(request_mac) <> ''),
    result_jsonb    jsonb NOT NULL CHECK (jsonb_typeof(result_jsonb) = 'object'),
    created_at     timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, profile_id, actor_user_id, idempotency_key),
    CONSTRAINT runtime_profile_credential_receipts_profile_fk
        FOREIGN KEY (tenant_id, profile_id)
        REFERENCES runtime_profiles(tenant_id, id) ON DELETE CASCADE
);

CREATE TABLE deployments (
    tenant_id              text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    id                     text NOT NULL,
    name                   text NOT NULL,
    description            text NOT NULL DEFAULT '',
    metadata_revision      bigint NOT NULL,
    latest_revision_number bigint,
    created_by             text NOT NULL REFERENCES user_accounts(id),
    created_at             timestamptz NOT NULL,
    updated_at             timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, id),
    CONSTRAINT deployments_name_not_blank CHECK (btrim(name) <> ''),
    CONSTRAINT deployments_metadata_revision_positive
        CHECK (metadata_revision > 0),
    CONSTRAINT deployments_latest_revision_positive
        CHECK (latest_revision_number IS NULL OR latest_revision_number > 0)
);

CREATE INDEX deployments_tenant_updated_idx
    ON deployments (tenant_id, updated_at DESC, id);

CREATE TABLE deployment_revisions (
    tenant_id              text NOT NULL,
    id                     text NOT NULL,
    deployment_id          text NOT NULL,
    revision_number        bigint NOT NULL,
    schema_version         text NOT NULL,
    input_jsonb            jsonb NOT NULL,
    input_digest           text NOT NULL,
    agent_id               text NOT NULL,
    agent_version_id       text NOT NULL,
    agent_version_number   bigint NOT NULL,
    agent_schema_version   text NOT NULL,
    agent_spec_digest      text NOT NULL,
    profile_id             text NOT NULL,
    profile_revision_id    text NOT NULL,
    profile_revision_number bigint NOT NULL,
    profile_schema_version text NOT NULL,
    profile_spec_digest    text NOT NULL,
    published_by           text NOT NULL REFERENCES user_accounts(id),
    published_at           timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, id),
    CONSTRAINT deployment_revisions_deployment_fk
        FOREIGN KEY (tenant_id, deployment_id)
        REFERENCES deployments(tenant_id, id),
    CONSTRAINT deployment_revisions_agent_version_fk
        FOREIGN KEY (tenant_id, agent_version_id)
        REFERENCES agent_versions(tenant_id, id),
    CONSTRAINT deployment_revisions_profile_revision_fk
        FOREIGN KEY (tenant_id, profile_revision_id)
        REFERENCES runtime_profile_revisions(tenant_id, id),
    CONSTRAINT deployment_revisions_number_positive
        CHECK (revision_number > 0),
    CONSTRAINT deployment_revisions_agent_version_positive
        CHECK (agent_version_number > 0),
    CONSTRAINT deployment_revisions_profile_revision_positive
        CHECK (profile_revision_number > 0),
    CONSTRAINT deployment_revisions_schema_not_blank
        CHECK (btrim(schema_version) <> ''),
    CONSTRAINT deployment_revisions_agent_schema_not_blank
        CHECK (btrim(agent_schema_version) <> ''),
    CONSTRAINT deployment_revisions_profile_schema_not_blank
        CHECK (btrim(profile_schema_version) <> ''),
    CONSTRAINT deployment_revisions_input_object
        CHECK (jsonb_typeof(input_jsonb) = 'object'),
    CONSTRAINT deployment_revisions_input_digest_format
        CHECK (input_digest ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT deployment_revisions_agent_digest_format
        CHECK (agent_spec_digest ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT deployment_revisions_profile_digest_format
        CHECK (profile_spec_digest ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT deployment_revisions_tenant_deployment_number_unique
        UNIQUE (tenant_id, deployment_id, revision_number)
);

CREATE INDEX deployment_revisions_tenant_deployment_idx
    ON deployment_revisions (tenant_id, deployment_id, revision_number DESC);

CREATE TABLE runtime_manifests (
    tenant_id                text NOT NULL,
    id                       text NOT NULL,
    deployment_revision_id   text NOT NULL,
    schema_version           text NOT NULL,
    compiler_version         text NOT NULL,
    runtime_contract_version text NOT NULL,
    content_jsonb            jsonb NOT NULL,
    content_digest           text NOT NULL,
    published_at             timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, id),
    CONSTRAINT runtime_manifests_revision_fk
        FOREIGN KEY (tenant_id, deployment_revision_id)
        REFERENCES deployment_revisions(tenant_id, id),
    CONSTRAINT runtime_manifests_revision_unique
        UNIQUE (tenant_id, deployment_revision_id),
    CONSTRAINT runtime_manifests_schema_not_blank
        CHECK (btrim(schema_version) <> ''),
    CONSTRAINT runtime_manifests_compiler_not_blank
        CHECK (btrim(compiler_version) <> ''),
    CONSTRAINT runtime_manifests_runtime_contract_not_blank
        CHECK (btrim(runtime_contract_version) <> ''),
    CONSTRAINT runtime_manifests_content_object
        CHECK (jsonb_typeof(content_jsonb) = 'object'),
    CONSTRAINT runtime_manifests_content_digest_format
        CHECK (content_digest ~ '^sha256:[0-9a-f]{64}$')
);

CREATE TABLE deployment_command_receipts (
    tenant_id              text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    operation              text NOT NULL,
    scope_id               text NOT NULL,
    key_hash               text NOT NULL,
    request_digest         text NOT NULL,
    deployment_id          text NOT NULL,
    deployment_revision_id text,
    runtime_manifest_id    text,
    result_jsonb           jsonb NOT NULL,
    created_by             text NOT NULL REFERENCES user_accounts(id),
    created_at             timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, operation, scope_id, key_hash),
    CONSTRAINT deployment_command_receipts_operation
        CHECK (operation IN ('create', 'publish')),
    CONSTRAINT deployment_command_receipts_scope_not_blank
        CHECK (btrim(scope_id) <> ''),
    CONSTRAINT deployment_command_receipts_key_hash_format
        CHECK (key_hash ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT deployment_command_receipts_request_digest_format
        CHECK (request_digest ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT deployment_command_receipts_result_object
        CHECK (jsonb_typeof(result_jsonb) = 'object'),
    CONSTRAINT deployment_command_receipts_deployment_fk
        FOREIGN KEY (tenant_id, deployment_id)
        REFERENCES deployments(tenant_id, id),
    CONSTRAINT deployment_command_receipts_revision_fk
        FOREIGN KEY (tenant_id, deployment_revision_id)
        REFERENCES deployment_revisions(tenant_id, id),
    CONSTRAINT deployment_command_receipts_manifest_fk
        FOREIGN KEY (tenant_id, runtime_manifest_id)
        REFERENCES runtime_manifests(tenant_id, id),
    CONSTRAINT deployment_command_receipts_shape CHECK (
        (
            operation = 'create'
            AND scope_id = tenant_id
            AND deployment_revision_id IS NULL
            AND runtime_manifest_id IS NULL
        )
        OR
        (
            operation = 'publish'
            AND scope_id = deployment_id
            AND deployment_revision_id IS NOT NULL
            AND runtime_manifest_id IS NOT NULL
        )
    )
);

CREATE TABLE control_outbox (
    tenant_id           text NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    id                  text NOT NULL,
    aggregate_type      text NOT NULL,
    aggregate_id        text NOT NULL,
    aggregate_revision  bigint NOT NULL,
    event_type          text NOT NULL,
    schema_version      text NOT NULL,
    payload_jsonb       jsonb NOT NULL,
    payload_digest      text NOT NULL,
    status              text NOT NULL DEFAULT 'PENDING',
    attempt_count       integer NOT NULL DEFAULT 0,
    available_at        timestamptz NOT NULL,
    claimed_by          text,
    claimed_until       timestamptz,
    last_error          text,
    published_at        timestamptz,
    created_at          timestamptz NOT NULL,
    updated_at          timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, id),
    CONSTRAINT control_outbox_event_unique
        UNIQUE (tenant_id, event_type, aggregate_type, aggregate_id, aggregate_revision),
    CONSTRAINT control_outbox_aggregate_type_not_blank
        CHECK (btrim(aggregate_type) <> ''),
    CONSTRAINT control_outbox_aggregate_id_not_blank
        CHECK (btrim(aggregate_id) <> ''),
    CONSTRAINT control_outbox_aggregate_revision_positive
        CHECK (aggregate_revision > 0),
    CONSTRAINT control_outbox_event_type_not_blank
        CHECK (btrim(event_type) <> ''),
    CONSTRAINT control_outbox_schema_not_blank
        CHECK (btrim(schema_version) <> ''),
    CONSTRAINT control_outbox_payload_object
        CHECK (jsonb_typeof(payload_jsonb) = 'object'),
    CONSTRAINT control_outbox_payload_digest_format
        CHECK (payload_digest ~ '^sha256:[0-9a-f]{64}$'),
    CONSTRAINT control_outbox_status
        CHECK (status IN ('PENDING', 'IN_FLIGHT', 'PUBLISHED', 'FAILED')),
    CONSTRAINT control_outbox_attempt_count_nonnegative
        CHECK (attempt_count >= 0),
    CONSTRAINT control_outbox_delivery_shape CHECK (
        (
            status = 'PENDING'
            AND published_at IS NULL
            AND claimed_by IS NULL
            AND claimed_until IS NULL
        )
        OR
        (
            status = 'IN_FLIGHT'
            AND published_at IS NULL
            AND claimed_by IS NOT NULL
            AND btrim(claimed_by) <> ''
            AND claimed_until IS NOT NULL
        )
        OR
        (
            status = 'PUBLISHED'
            AND published_at IS NOT NULL
            AND claimed_by IS NULL
            AND claimed_until IS NULL
        )
        OR
        (
            status = 'FAILED'
            AND published_at IS NULL
            AND claimed_by IS NULL
            AND claimed_until IS NULL
            AND last_error IS NOT NULL
            AND btrim(last_error) <> ''
        )
    )
);

CREATE INDEX control_outbox_pending_claim_idx
    ON control_outbox (available_at, created_at, id)
    WHERE status = 'PENDING';

CREATE FUNCTION reject_deployment_publication_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% is immutable', TG_TABLE_NAME;
END
$$;

CREATE TRIGGER deployment_revisions_immutable
BEFORE UPDATE OR DELETE ON deployment_revisions
FOR EACH ROW EXECUTE FUNCTION reject_deployment_publication_mutation();

CREATE TRIGGER runtime_manifests_immutable
BEFORE UPDATE OR DELETE ON runtime_manifests
FOR EACH ROW EXECUTE FUNCTION reject_deployment_publication_mutation();

CREATE TRIGGER deployment_command_receipts_immutable
BEFORE UPDATE OR DELETE ON deployment_command_receipts
FOR EACH ROW EXECUTE FUNCTION reject_deployment_publication_mutation();

CREATE FUNCTION protect_control_outbox_content() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(
        NEW.tenant_id,
        NEW.id,
        NEW.aggregate_type,
        NEW.aggregate_id,
        NEW.aggregate_revision,
        NEW.event_type,
        NEW.schema_version,
        NEW.payload_jsonb,
        NEW.payload_digest,
        NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.tenant_id,
        OLD.id,
        OLD.aggregate_type,
        OLD.aggregate_id,
        OLD.aggregate_revision,
        OLD.event_type,
        OLD.schema_version,
        OLD.payload_jsonb,
        OLD.payload_digest,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'control_outbox event content is immutable';
    END IF;
    IF NEW.attempt_count < OLD.attempt_count THEN
        RAISE EXCEPTION 'control_outbox attempt_count cannot decrease';
    END IF;
    IF OLD.status = 'PUBLISHED' THEN
        RAISE EXCEPTION 'published control_outbox record is terminal';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER control_outbox_content_protected
BEFORE UPDATE ON control_outbox
FOR EACH ROW EXECUTE FUNCTION protect_control_outbox_content();

-- Channel V1 is a current development baseline, not a compatibility migration.
-- Runtime credentials are ciphertext owned by Channel; projections never contain values.
CREATE TABLE channel_account_catalog (
    scope_id text PRIMARY KEY,
    source_epoch uuid NOT NULL,
    snapshot_revision bigint NOT NULL DEFAULT 1 CHECK (snapshot_revision BETWEEN 1 AND 9007199254740991),
    account_count integer NOT NULL DEFAULT 0 CHECK (account_count BETWEEN 0 AND 1000)
);
CREATE TABLE channel_accounts (
    tenant_id text NOT NULL REFERENCES tenants(id),
    id text NOT NULL,
    scope_id text NOT NULL REFERENCES channel_account_catalog(scope_id),
    provider text NOT NULL CHECK (provider IN ('telegram','wecom')),
    provider_account_id text NOT NULL CHECK (octet_length(provider_account_id) BETWEEN 1 AND 1024),
    name text NOT NULL CHECK (char_length(name) BETWEEN 1 AND 128),
    description text NOT NULL DEFAULT '' CHECK (char_length(description) <= 4096),
    account_revision bigint NOT NULL CHECK (account_revision BETWEEN 1 AND 9007199254740991),
    connection_revision bigint NOT NULL CHECK (connection_revision BETWEEN 1 AND account_revision),
    min_route_generation bigint NOT NULL DEFAULT 0 CHECK (min_route_generation BETWEEN 0 AND 9007199254740991),
    enabled boolean NOT NULL DEFAULT false,
    config_jsonb jsonb NOT NULL CHECK (jsonb_typeof(config_jsonb)='object'),
    created_by text NOT NULL REFERENCES user_accounts(id),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id,id),
    CONSTRAINT channel_accounts_global_id UNIQUE (id),
    CONSTRAINT channel_accounts_provider_identity UNIQUE (scope_id,provider,provider_account_id),
    CONSTRAINT channel_accounts_provider_fk UNIQUE (tenant_id,id,provider)
);
CREATE INDEX channel_accounts_scope_order ON channel_accounts(scope_id,provider,id);
CREATE INDEX channel_accounts_tenant_order ON channel_accounts(tenant_id,id);
CREATE TABLE channel_account_credentials (
    tenant_id text NOT NULL,
    account_id text NOT NULL,
    provider text NOT NULL,
    purpose text NOT NULL,
    id text NOT NULL UNIQUE,
    credential_version bigint NOT NULL CHECK (credential_version BETWEEN 1 AND 9007199254740991),
    configured boolean NOT NULL,
    key_id text,
    ciphertext bytea,
    PRIMARY KEY (tenant_id,account_id,purpose),
    FOREIGN KEY (tenant_id,account_id,provider) REFERENCES channel_accounts(tenant_id,id,provider),
    CHECK ((provider='telegram' AND purpose IN ('telegram.bot_token','telegram.webhook_secret')) OR (provider='wecom' AND purpose='wecom.bot_secret')),
    CHECK ((configured AND key_id IS NOT NULL AND key_id<>'' AND ciphertext IS NOT NULL AND octet_length(ciphertext) BETWEEN 30 AND 16413) OR
           (NOT configured AND key_id IS NULL AND ciphertext IS NULL))
);
CREATE TABLE channel_bindings (
    tenant_id text NOT NULL,
    id text NOT NULL,
    account_id text NOT NULL,
    binding_revision bigint NOT NULL CHECK (binding_revision BETWEEN 1 AND 9007199254740991),
    enabled boolean NOT NULL DEFAULT false,
    deployment_id text NOT NULL,
    revision_number bigint NOT NULL CHECK (revision_number BETWEEN 1 AND 9007199254740991),
    deployment_revision_id text NOT NULL,
    manifest_ref text NOT NULL,
    manifest_digest text NOT NULL CHECK (manifest_digest ~ '^sha256:[0-9a-f]{64}$'),
    created_by text NOT NULL REFERENCES user_accounts(id),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id,id),
    CONSTRAINT channel_bindings_one_account UNIQUE (tenant_id,account_id),
    FOREIGN KEY (tenant_id,account_id) REFERENCES channel_accounts(tenant_id,id),
    FOREIGN KEY (tenant_id,deployment_id) REFERENCES deployments(tenant_id,id),
    FOREIGN KEY (tenant_id,deployment_revision_id) REFERENCES deployment_revisions(tenant_id,id),
    FOREIGN KEY (tenant_id,manifest_ref) REFERENCES runtime_manifests(tenant_id,id)
);
CREATE TABLE channel_account_route_states (
    tenant_id text NOT NULL,
    account_id text NOT NULL,
    generation bigint NOT NULL DEFAULT 0 CHECK (generation BETWEEN 0 AND 9007199254740991),
    projection_jsonb jsonb,
    payload_digest text,
    PRIMARY KEY (tenant_id,account_id),
    FOREIGN KEY (tenant_id,account_id) REFERENCES channel_accounts(tenant_id,id),
    CHECK ((generation=0 AND projection_jsonb IS NULL AND payload_digest IS NULL) OR
           (generation>0 AND projection_jsonb IS NOT NULL AND payload_digest IS NOT NULL AND jsonb_typeof(projection_jsonb)='object' AND payload_digest ~ '^sha256:[0-9a-f]{64}$'))
);
CREATE TABLE channel_command_receipts (
    tenant_id text NOT NULL REFERENCES tenants(id),
    operation text NOT NULL,
    scope_id text NOT NULL,
    key_hash text NOT NULL CHECK (key_hash ~ '^[0-9a-f]{64}$'),
    mac_key_id text NOT NULL CHECK (mac_key_id<>''),
    request_mac text NOT NULL CHECK (request_mac ~ '^[0-9a-f]{64}$'),
    result_jsonb jsonb NOT NULL CHECK (jsonb_typeof(result_jsonb)='object'),
    created_by text NOT NULL REFERENCES user_accounts(id),
    created_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id,operation,scope_id,key_hash),
    CHECK (operation IN ('CreateChannelAccount','UpdateChannelAccount','UpdateAccountCredential','SetChannelAccountEnabled','CreateChannelBinding','SetChannelBindingTarget','SetChannelBindingEnabled'))
);
CREATE TABLE channel_account_observations (
    tenant_id text NOT NULL,
    account_id text NOT NULL,
    scope_id text NOT NULL,
    source_epoch uuid NOT NULL,
    provider text NOT NULL CHECK (provider IN ('telegram','wecom')),
    connection_revision bigint NOT NULL CHECK (connection_revision BETWEEN 1 AND 9007199254740991),
    instance_id text NOT NULL,
    instance_epoch uuid NOT NULL,
    report_sequence bigint NOT NULL CHECK (report_sequence BETWEEN 1 AND 9007199254740991),
    state text NOT NULL CHECK (state IN ('CONFIG_APPLIED','CONNECTING','READY','DISABLED','ERROR')),
    reason_code text NOT NULL,
    owner_epoch bigint CHECK (owner_epoch BETWEEN 1 AND 9007199254740991),
    observed_at timestamptz NOT NULL,
    received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    observation_digest text NOT NULL CHECK (observation_digest ~ '^sha256:[0-9a-f]{64}$'),
    PRIMARY KEY (tenant_id,account_id,instance_id,instance_epoch),
    FOREIGN KEY (tenant_id,account_id,provider) REFERENCES channel_accounts(tenant_id,id,provider),
    CHECK (provider='wecom' OR owner_epoch IS NULL)
);
CREATE INDEX channel_observations_expiry ON channel_account_observations(received_at);
