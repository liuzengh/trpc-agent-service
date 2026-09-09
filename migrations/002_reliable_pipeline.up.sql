-- Canonical durable Inbox/Outbox schema. These are the only queue tables used
-- by Gateway, Consumer and Delivery.

CREATE TABLE IF NOT EXISTS inbox_messages (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id),
    channel_type VARCHAR(32) NOT NULL,
    channel_account_id VARCHAR(128) NOT NULL,
    agent_app_name VARCHAR(128) NOT NULL,
    external_message_id VARCHAR(256) NOT NULL,
    conversation_id VARCHAR(256) NOT NULL,
    user_id VARCHAR(256) NOT NULL,
    session_id VARCHAR(512) NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    payload BYTEA NOT NULL,
    trace_parent VARCHAR(256) NOT NULL DEFAULT '',
    status VARCHAR(32) NOT NULL DEFAULT 'RECEIVED'
        CHECK (status IN ('RECEIVED','PROCESSING','RETRY_WAIT','COMPLETED','DEAD_LETTERED')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    next_attempt_at TIMESTAMPTZ,
    lease_owner VARCHAR(256),
    lease_version BIGINT NOT NULL DEFAULT 0 CHECK (lease_version >= 0),
    lease_until TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, channel_type, channel_account_id, external_message_id)
);

CREATE INDEX IF NOT EXISTS idx_inbox_claimable
    ON inbox_messages (status, next_attempt_at, lease_until, created_at)
    WHERE status IN ('RECEIVED','PROCESSING','RETRY_WAIT');
CREATE INDEX IF NOT EXISTS idx_inbox_tenant_session
    ON inbox_messages (tenant_id, session_id, created_at);
CREATE INDEX IF NOT EXISTS idx_inbox_dead_letter
    ON inbox_messages (tenant_id, updated_at)
    WHERE status = 'DEAD_LETTERED';

CREATE TABLE IF NOT EXISTS outbox_messages (
    id BIGSERIAL PRIMARY KEY,
    inbox_id BIGINT NOT NULL UNIQUE REFERENCES inbox_messages(id),
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id),
    channel_type VARCHAR(32) NOT NULL,
    channel_account_id VARCHAR(128) NOT NULL,
    conversation_id VARCHAR(256) NOT NULL,
    reply_to_id VARCHAR(256) NOT NULL DEFAULT '',
    content_type VARCHAR(32) NOT NULL DEFAULT 'text',
    content TEXT NOT NULL,
    trace_parent VARCHAR(256) NOT NULL DEFAULT '',
    status VARCHAR(32) NOT NULL DEFAULT 'REPLY_PENDING'
        CHECK (status IN ('REPLY_PENDING','DELIVERING','RETRY_WAIT','REPLIED','DEAD_LETTERED')),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 8 CHECK (max_attempts > 0),
    next_attempt_at TIMESTAMPTZ,
    lease_owner VARCHAR(256),
    lease_version BIGINT NOT NULL DEFAULT 0 CHECK (lease_version >= 0),
    lease_until TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    delivered_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_outbox_claimable
    ON outbox_messages (status, next_attempt_at, lease_until, created_at)
    WHERE status IN ('REPLY_PENDING','DELIVERING','RETRY_WAIT');
CREATE INDEX IF NOT EXISTS idx_outbox_tenant
    ON outbox_messages (tenant_id, created_at);
CREATE INDEX IF NOT EXISTS idx_outbox_dead_letter
    ON outbox_messages (tenant_id, updated_at)
    WHERE status = 'DEAD_LETTERED';

CREATE TABLE IF NOT EXISTS message_replay_audit (
    id BIGSERIAL PRIMARY KEY,
    queue_type VARCHAR(16) NOT NULL CHECK (queue_type IN ('inbox','outbox')),
    message_id BIGINT NOT NULL,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id),
    requested_by VARCHAR(256) NOT NULL,
    reason TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_replay_audit_tenant_time
    ON message_replay_audit (tenant_id, created_at DESC);
