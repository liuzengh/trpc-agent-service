CREATE TABLE background_job (
    job_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    app_id VARCHAR(64) NOT NULL,
    revision_id VARCHAR(64) NOT NULL,
    job_type VARCHAR(64) NOT NULL,
    dedupe_key VARCHAR(512) NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 5,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by VARCHAR(128),
    locked_until TIMESTAMPTZ,
    last_error TEXT,
    trace_parent VARCHAR(128),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    FOREIGN KEY (tenant_id, app_id) REFERENCES agent_app(tenant_id, app_id),
    FOREIGN KEY (tenant_id, revision_id) REFERENCES agent_revision(tenant_id, revision_id),
    UNIQUE (tenant_id, job_type, dedupe_key)
);

CREATE INDEX idx_background_job_claim
    ON background_job(next_attempt_at, created_at)
    WHERE status IN ('pending', 'running');
