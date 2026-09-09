ALTER TABLE channel_binding
    ADD COLUMN verify_token_ref text,
    ADD COLUMN status text,
    ADD COLUMN version bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
    ADD COLUMN expires_at timestamptz,
    ADD COLUMN disabled_at timestamptz,
    ADD COLUMN external_target_type text NOT NULL DEFAULT 'message',
    ADD COLUMN external_target_id text;

UPDATE channel_binding
SET status = CASE WHEN enabled THEN 'active' ELSE 'disabled' END
WHERE status IS NULL;

ALTER TABLE channel_binding
    ALTER COLUMN status SET NOT NULL,
    ALTER COLUMN status SET DEFAULT 'active',
    ADD CONSTRAINT ck_channel_binding_status
        CHECK (status IN ('active', 'disabled', 'expired', 'retired')),
    ADD CONSTRAINT ck_channel_binding_state
        CHECK ((status = 'active') = enabled),
    ADD CONSTRAINT ck_channel_binding_target
        CHECK (external_target_type IN ('message', 'user', 'chat')
            AND (external_target_type = 'message' OR length(btrim(external_target_id)) > 0));

CREATE UNIQUE INDEX ux_channel_binding_provider_identity
    ON channel_binding (channel, external_app_id)
    WHERE channel IN ('lark', 'telegram');
CREATE INDEX ix_channel_binding_status
    ON channel_binding (tenant_id, status, updated_at);

ALTER TABLE user_identity
    ADD COLUMN internal_user_id text,
    ADD COLUMN scope text NOT NULL DEFAULT 'private',
    ADD COLUMN external_chat text,
    ADD COLUMN external_thread_id text,
    ADD COLUMN status text NOT NULL DEFAULT 'active',
    ADD COLUMN version bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
    ADD COLUMN updated_at timestamptz NOT NULL DEFAULT now();

UPDATE user_identity
SET internal_user_id = identity_id
WHERE internal_user_id IS NULL;

ALTER TABLE user_identity
    ALTER COLUMN internal_user_id SET NOT NULL,
    ADD CONSTRAINT ck_user_identity_status
        CHECK (status IN ('active', 'disabled')),
    ADD CONSTRAINT ck_user_identity_scope
        CHECK (scope IN ('private', 'group', 'topic'));

CREATE INDEX ix_user_identity_scope
    ON user_identity (tenant_id, channel, binding_id, external_chat, external_thread_id);

CREATE TABLE channel_binding_audit (
    tenant_id text NOT NULL,
    audit_id text NOT NULL,
    channel text NOT NULL,
    binding_id text NOT NULL,
    identity_fingerprint text,
    secret_fingerprint text,
    operation text NOT NULL,
    success boolean NOT NULL,
    error_type text,
    version bigint NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, audit_id),
    FOREIGN KEY (tenant_id, channel, binding_id)
        REFERENCES channel_binding (tenant_id, channel, binding_id) ON DELETE RESTRICT,
    CHECK (length(btrim(operation)) BETWEEN 1 AND 64),
    CHECK (identity_fingerprint IS NULL OR length(identity_fingerprint) <= 80),
    CHECK (secret_fingerprint IS NULL OR length(secret_fingerprint) <= 80),
    CHECK (error_type IS NULL OR length(error_type) <= 80)
);
CREATE INDEX ix_channel_binding_audit_created
    ON channel_binding_audit (tenant_id, created_at);
