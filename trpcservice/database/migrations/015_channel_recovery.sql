ALTER TABLE channel_poll_checkpoint ADD COLUMN floor_at TIMESTAMPTZ;
CREATE TABLE channel_message_rejection (
    tenant_id TEXT NOT NULL,
    channel_binding_id TEXT NOT NULL,
    chat_hash TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    reason TEXT NOT NULL CHECK(reason IN ('invalid_identity','invalid_message','invalid_timestamp_or_type','unsupported_type','oversized_text')),
    window_from TIMESTAMPTZ NOT NULL,
    window_to TIMESTAMPTZ NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    trace_id TEXT NOT NULL DEFAULT '',
    PRIMARY KEY(tenant_id,channel_binding_id,chat_hash,fingerprint),
    FOREIGN KEY(tenant_id,channel_binding_id,chat_hash) REFERENCES channel_poll_checkpoint(tenant_id,channel_binding_id,chat_hash)
);
CREATE TABLE channel_checkpoint_recovery (
    tenant_id TEXT NOT NULL,
    channel_binding_id TEXT NOT NULL,
    chat_hash TEXT NOT NULL,
    version BIGINT NOT NULL,
    action TEXT NOT NULL CHECK(action IN ('rewind','resume')),
    previous_through TIMESTAMPTZ NOT NULL,
    resume_from TIMESTAMPTZ NOT NULL,
    previous_config_hash TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    acknowledge_gap BOOLEAN NOT NULL,
    trace_id TEXT NOT NULL DEFAULT '',
    recovered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id,channel_binding_id,chat_hash,version),
    FOREIGN KEY(tenant_id,channel_binding_id,chat_hash) REFERENCES channel_poll_checkpoint(tenant_id,channel_binding_id,chat_hash)
);
