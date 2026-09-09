-- Durable summary generation queue. The tRPC Session backend remains the
-- source of events; this table stores only coordination and CAS metadata.
CREATE TABLE IF NOT EXISTS summary_jobs (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id),
    agent_app_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(512) NOT NULL,
    filter_key VARCHAR(512) NOT NULL DEFAULT '',
    target_event_sequence BIGINT NOT NULL CHECK (target_event_sequence > 0),
    status VARCHAR(16) NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','PROCESSING','COMPLETED','FAILED')),
    lease_owner VARCHAR(128),
    lease_version BIGINT NOT NULL DEFAULT 0 CHECK (lease_version >= 0),
    lease_until TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts INTEGER NOT NULL DEFAULT 8 CHECK (max_attempts BETWEEN 1 AND 100),
    next_attempt_at TIMESTAMPTZ,
    last_error TEXT NOT NULL DEFAULT '',
    completed_event_sequence BIGINT NOT NULL DEFAULT 0 CHECK (completed_event_sequence >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, agent_app_id, session_id, filter_key),
    FOREIGN KEY (agent_app_id, tenant_id) REFERENCES agent_apps(id, tenant_id),
    CHECK (attempts <= max_attempts),
    CHECK ((status = 'PROCESSING' AND lease_owner IS NOT NULL AND lease_owner <> '' AND lease_until IS NOT NULL)
        OR (status <> 'PROCESSING' AND lease_owner IS NULL AND lease_until IS NULL)),
    CHECK (status = 'COMPLETED' OR completed_event_sequence <= target_event_sequence)
);

CREATE INDEX IF NOT EXISTS idx_summary_jobs_claimable
    ON summary_jobs (status, next_attempt_at, lease_until, updated_at)
    WHERE status IN ('PENDING','FAILED');

CREATE INDEX IF NOT EXISTS idx_summary_jobs_expired_processing
    ON summary_jobs (lease_until, updated_at)
    WHERE status = 'PROCESSING';

CREATE TABLE IF NOT EXISTS summary_checkpoints (
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id),
    agent_app_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(512) NOT NULL,
    filter_key VARCHAR(512) NOT NULL DEFAULT '',
    max_event_sequence BIGINT NOT NULL CHECK (max_event_sequence >= 0),
    content TEXT NOT NULL,
    content_sha256 CHAR(64) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, agent_app_id, session_id, filter_key),
    FOREIGN KEY (agent_app_id, tenant_id) REFERENCES agent_apps(id, tenant_id),
    CHECK (octet_length(content) <= 1048576),
    CHECK (content_sha256 ~ '^[0-9a-fA-F]{64}$')
);
