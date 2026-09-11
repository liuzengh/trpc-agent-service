CREATE TABLE channel_credential (
    tenant_id TEXT NOT NULL REFERENCES tenant(tenant_id),
    reference TEXT NOT NULL,
    purposes TEXT[] NOT NULL,
    ciphertext BYTEA NOT NULL,
    key_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id, reference),
    CHECK (cardinality(purposes) BETWEEN 1 AND 3)
);

CREATE TABLE channel_connection (
    tenant_id TEXT NOT NULL,
    connection_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    channel_type TEXT NOT NULL CHECK(channel_type IN ('telegram','wecom_mcp')),
    account_key TEXT NOT NULL,
    display_name TEXT NOT NULL,
    binding_id TEXT,
    credential_ref TEXT NOT NULL,
    webhook_ref TEXT NOT NULL DEFAULT '',
    callback_url TEXT NOT NULL DEFAULT '',
    remote_hash TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'draft',
    message TEXT NOT NULL DEFAULT '',
    settings JSONB NOT NULL DEFAULT '{}',
    busy_until TIMESTAMPTZ,
    last_received_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id,connection_id),
    UNIQUE(channel_type,account_key),
    UNIQUE(binding_id),
    FOREIGN KEY(tenant_id,app_id) REFERENCES agent_app(tenant_id,app_id),
    FOREIGN KEY(tenant_id,credential_ref) REFERENCES channel_credential(tenant_id,reference),
    FOREIGN KEY(tenant_id,binding_id) REFERENCES channel_binding(tenant_id,channel_binding_id),
    CHECK(status IN ('draft','needs_confirmation','connecting','connected','paused','error','unknown'))
);

CREATE TABLE channel_connection_group (
    tenant_id TEXT NOT NULL,
    connection_id TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    display_name TEXT NOT NULL,
    seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id,connection_id,chat_id),
    FOREIGN KEY(tenant_id,connection_id) REFERENCES channel_connection(tenant_id,connection_id)
);
REVOKE ALL ON channel_credential,channel_connection,channel_connection_group FROM PUBLIC;

CREATE TABLE channel_connection_setting (
    name TEXT PRIMARY KEY CHECK(name='public_url'),
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
REVOKE ALL ON channel_connection_setting FROM PUBLIC;
