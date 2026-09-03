CREATE TABLE tool_execution (
    execution_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    request_id VARCHAR(128) NOT NULL,
    revision_id VARCHAR(64) NOT NULL,
    tool_call_id VARCHAR(255) NOT NULL,
    tool_name VARCHAR(255) NOT NULL,
    arguments_hash VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    result_hash VARCHAR(64),
    error_type VARCHAR(128),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    FOREIGN KEY (tenant_id, revision_id) REFERENCES agent_revision(tenant_id, revision_id),
    UNIQUE (request_id, tool_call_id)
);

CREATE INDEX idx_tool_execution_tenant_time
    ON tool_execution(tenant_id, started_at DESC);
