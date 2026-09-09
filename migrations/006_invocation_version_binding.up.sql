-- Pin the first resolved Agent version before model execution. Retries of the
-- same durable Inbox item must not jump to a new deployment after a rollout.
CREATE TABLE IF NOT EXISTS invocation_bindings (
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id),
    idempotency_key VARCHAR(256) NOT NULL,
    agent_app_id VARCHAR(64) NOT NULL,
    agent_version_id VARCHAR(64) NOT NULL,
    deployment_id VARCHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, idempotency_key),
    FOREIGN KEY (agent_app_id, tenant_id) REFERENCES agent_apps(id, tenant_id),
    FOREIGN KEY (agent_version_id, agent_app_id) REFERENCES agent_versions(id, agent_app_id),
    FOREIGN KEY (deployment_id, tenant_id) REFERENCES deployments(id, tenant_id)
);

CREATE INDEX IF NOT EXISTS idx_invocation_bindings_version
    ON invocation_bindings (tenant_id, agent_version_id, created_at DESC);
