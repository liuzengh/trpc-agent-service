ALTER TABLE tool_execution ADD COLUMN operation_id VARCHAR(64);
CREATE INDEX idx_tool_execution_operation ON tool_execution(tenant_id, operation_id) WHERE operation_id IS NOT NULL;

CREATE TABLE tool_operation (
    operation_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    app_id VARCHAR(64) NOT NULL,
    user_id VARCHAR(512) NOT NULL,
    tool_name VARCHAR(255) NOT NULL,
    business_key_hash VARCHAR(64) NOT NULL,
    input_hash VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL CHECK(status IN ('running','unknown','succeeded','failed')),
    result JSONB,
    error_type VARCHAR(128) NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY(tenant_id, app_id) REFERENCES agent_app(tenant_id, app_id),
    UNIQUE(tenant_id, app_id, user_id, tool_name, business_key_hash)
);
CREATE INDEX idx_tool_operation_reconcile ON tool_operation(tenant_id, status, operation_id);

-- Reference business backend. A work item is a real local row, not an external
-- ticketing integration. Unique operation_id enforces backend idempotency.
CREATE TABLE work_item (
    work_item_id VARCHAR(64) PRIMARY KEY,
    operation_id VARCHAR(64) NOT NULL UNIQUE REFERENCES tool_operation(operation_id),
    tenant_id VARCHAR(64) NOT NULL,
    app_id VARCHAR(64) NOT NULL,
    user_id VARCHAR(512) NOT NULL,
    input_hash VARCHAR(64) NOT NULL,
    title TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
