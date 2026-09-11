-- Freshness is separate from execution recovery and manual checkpoint moves.
ALTER TABLE agent_run ADD COLUMN message_mode VARCHAR(16);
ALTER TABLE agent_run ADD COLUMN message_expires_at TIMESTAMPTZ;

CREATE TABLE channel_poll_gap (
    tenant_id TEXT NOT NULL,
    channel_binding_id TEXT NOT NULL,
    chat_hash TEXT NOT NULL,
    checkpoint_version BIGINT NOT NULL,
    previous_through TIMESTAMPTZ NOT NULL,
    recent_from TIMESTAMPTZ NOT NULL,
    processed_through TIMESTAMPTZ NOT NULL,
    reason TEXT NOT NULL CHECK (reason='realtime_window'),
    trace_id TEXT NOT NULL DEFAULT '',
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,channel_binding_id,chat_hash,checkpoint_version),
    FOREIGN KEY (tenant_id,channel_binding_id) REFERENCES channel_binding(tenant_id,channel_binding_id)
);
CREATE INDEX channel_poll_gap_recent ON channel_poll_gap(tenant_id,channel_binding_id,recorded_at DESC);

CREATE TABLE channel_message_disposition (
    tenant_id TEXT NOT NULL,
    channel_binding_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,channel_binding_id,message_id),
    FOREIGN KEY (tenant_id,channel_binding_id) REFERENCES channel_binding(tenant_id,channel_binding_id)
);
CREATE INDEX channel_disposition_recent ON channel_message_disposition(tenant_id,channel_binding_id,recorded_at DESC);
CREATE INDEX run_unstarted_expiry ON agent_run(message_expires_at) WHERE status='queued' AND started_at IS NULL;
