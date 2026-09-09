CREATE TABLE IF NOT EXISTS agent_apps (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id),
    name VARCHAR(128) NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    status VARCHAR(16) NOT NULL DEFAULT 'active'
        CHECK (status IN ('active','suspended','deleted')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name),
    UNIQUE (id, tenant_id)
);

CREATE TABLE IF NOT EXISTS agent_versions (
    id VARCHAR(64) PRIMARY KEY,
    agent_app_id VARCHAR(64) NOT NULL REFERENCES agent_apps(id),
    version_number BIGINT NOT NULL CHECK (version_number > 0),
    config_snapshot JSONB NOT NULL,
    config_hash CHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'draft'
        CHECK (status IN ('draft','published','retired')),
    created_by VARCHAR(256) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    UNIQUE (agent_app_id, version_number),
    UNIQUE (agent_app_id, config_hash),
    UNIQUE (id, agent_app_id)
);

CREATE TABLE IF NOT EXISTS deployments (
    id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id),
    agent_app_id VARCHAR(64) NOT NULL,
    agent_version_id VARCHAR(64) NOT NULL,
    kind VARCHAR(16) NOT NULL CHECK (kind IN ('stable','canary')),
    traffic_bps INTEGER NOT NULL DEFAULT 0 CHECK (traffic_bps BETWEEN 0 AND 10000),
    status VARCHAR(16) NOT NULL DEFAULT 'active'
        CHECK (status IN ('active','paused','rolled_back','completed')),
    created_by VARCHAR(256) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (agent_app_id, tenant_id) REFERENCES agent_apps(id, tenant_id),
    FOREIGN KEY (agent_version_id, agent_app_id) REFERENCES agent_versions(id, agent_app_id),
    UNIQUE (id, tenant_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_active_deployment_kind
    ON deployments (tenant_id, agent_app_id, kind)
    WHERE status = 'active';

CREATE TABLE IF NOT EXISTS execution_records (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id),
    session_id VARCHAR(512) NOT NULL,
    agent_app_id VARCHAR(64) NOT NULL,
    agent_version_id VARCHAR(64) NOT NULL,
    deployment_id VARCHAR(64) NOT NULL,
    status VARCHAR(16) NOT NULL CHECK (status IN ('RUNNING','SUCCEEDED','FAILED')),
    error_message TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    FOREIGN KEY (agent_app_id, tenant_id) REFERENCES agent_apps(id, tenant_id),
    FOREIGN KEY (agent_version_id, agent_app_id) REFERENCES agent_versions(id, agent_app_id),
    FOREIGN KEY (deployment_id, tenant_id) REFERENCES deployments(id, tenant_id)
);

CREATE INDEX IF NOT EXISTS idx_execution_tenant_session
    ON execution_records (tenant_id, session_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_execution_version
    ON execution_records (tenant_id, agent_version_id, started_at DESC);
