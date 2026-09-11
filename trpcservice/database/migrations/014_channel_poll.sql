ALTER TABLE channel_binding ADD CONSTRAINT channel_binding_tenant_identity UNIQUE (tenant_id, channel_binding_id);

CREATE TABLE channel_poll_checkpoint (
    tenant_id TEXT NOT NULL,
    channel_binding_id TEXT NOT NULL,
    chat_hash TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    through_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, channel_binding_id, chat_hash),
    FOREIGN KEY (tenant_id, channel_binding_id) REFERENCES channel_binding(tenant_id, channel_binding_id)
);
CREATE TABLE channel_poll_seen (
    tenant_id TEXT NOT NULL,
    channel_binding_id TEXT NOT NULL,
    chat_hash TEXT NOT NULL,
    message_fingerprint TEXT NOT NULL,
    accepted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, channel_binding_id, chat_hash, message_fingerprint),
    FOREIGN KEY (tenant_id, channel_binding_id, chat_hash) REFERENCES channel_poll_checkpoint(tenant_id, channel_binding_id, chat_hash)
);
CREATE TABLE channel_delivery_attempt (
    tenant_id TEXT NOT NULL,
    channel_binding_id TEXT NOT NULL,
    outbound_id TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('attempting','sent','rejected','unknown')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, channel_binding_id, outbound_id),
    FOREIGN KEY (tenant_id, channel_binding_id) REFERENCES channel_binding(tenant_id, channel_binding_id)
);
CREATE INDEX channel_delivery_review ON channel_delivery_attempt(status, updated_at) WHERE status IN ('attempting','unknown');
