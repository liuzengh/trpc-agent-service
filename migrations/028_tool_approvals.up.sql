-- Durable, tenant-scoped approval capabilities for confirmation-gated tools.
-- Raw tokens are never persisted; token_hash is compared in one conditional
-- UPDATE during consumption so concurrent workers cannot both execute a call.
CREATE TABLE IF NOT EXISTS tool_approvals (
    challenge_id VARCHAR(128) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id VARCHAR(255) NOT NULL,
    session_owner_id VARCHAR(255) NOT NULL,
    session_id VARCHAR(255) NOT NULL,
    tool_name VARCHAR(255) NOT NULL,
    args_hash VARCHAR(71) NOT NULL,
    invocation_id VARCHAR(255) NOT NULL,
    token_hash BYTEA,
    expires_at TIMESTAMPTZ NOT NULL,
    granted_at TIMESTAMPTZ,
    granted_by VARCHAR(256),
    consumed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT tool_approvals_args_hash_format CHECK (args_hash LIKE 'sha256:%'),
    CONSTRAINT tool_approvals_grant_pair CHECK ((granted_at IS NULL) = (granted_by IS NULL)),
    CONSTRAINT tool_approvals_token_pair CHECK ((granted_at IS NULL) = (token_hash IS NULL)),
    CONSTRAINT tool_approvals_token_length CHECK (token_hash IS NULL OR octet_length(token_hash) = 32)
);

CREATE INDEX IF NOT EXISTS idx_tool_approvals_scope
    ON tool_approvals (tenant_id, user_id, session_owner_id, session_id, invocation_id);
CREATE INDEX IF NOT EXISTS idx_tool_approvals_expiry
    ON tool_approvals (expires_at)
    WHERE consumed_at IS NULL;
