-- Create tenants table
CREATE TABLE IF NOT EXISTS tenants (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'active',
    config JSONB NOT NULL,
    config_version BIGINT NOT NULL DEFAULT 1
        CONSTRAINT tenants_config_version_positive CHECK (config_version > 0),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_tenants_status ON tenants(status);
CREATE INDEX idx_tenants_name ON tenants(name);

-- Create tenant_channels table
CREATE TABLE IF NOT EXISTS tenant_channels (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    channel_type VARCHAR(32) NOT NULL,
    webhook_token VARCHAR(128) NOT NULL,
    config JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(webhook_token),
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
);

CREATE INDEX idx_channels_tenant ON tenant_channels(tenant_id);
CREATE INDEX idx_channels_webhook ON tenant_channels(webhook_token);

-- Create audit_logs table
CREATE TABLE IF NOT EXISTS audit_logs (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    channel VARCHAR(32),
    user_id VARCHAR(128),
    session_id VARCHAR(64),
    agent_name VARCHAR(128),
    tool_name VARCHAR(128),
    decision VARCHAR(32),
    latency_ms INTEGER,
    error_type VARCHAR(64),
    token_count INTEGER,
    cost_usd DECIMAL(10, 6),
    trace_id VARCHAR(64),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_audit_tenant_time ON audit_logs(tenant_id, created_at);
CREATE INDEX idx_audit_trace ON audit_logs(trace_id);
