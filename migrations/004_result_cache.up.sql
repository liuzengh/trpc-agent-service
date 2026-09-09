CREATE TABLE IF NOT EXISTS invocation_results (
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id),
    idempotency_key VARCHAR(256) NOT NULL,
    payload_hash CHAR(64) NOT NULL,
    response BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT now() + INTERVAL '7 days',
    PRIMARY KEY (tenant_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS idx_invocation_results_expiry ON invocation_results(expires_at);
