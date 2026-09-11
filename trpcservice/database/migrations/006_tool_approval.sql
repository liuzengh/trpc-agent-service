CREATE TABLE tool_approval (
    approval_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    app_id VARCHAR(64) NOT NULL,
    revision_id VARCHAR(64) NOT NULL,
    channel_binding_id VARCHAR(64) NOT NULL REFERENCES channel_binding(channel_binding_id),
    request_id VARCHAR(128) NOT NULL REFERENCES agent_run(request_id),
    message_id VARCHAR(512) NOT NULL,
    user_id VARCHAR(512) NOT NULL,
    session_id VARCHAR(512) NOT NULL,
    tool_call_id VARCHAR(255) NOT NULL,
    tool_name VARCHAR(255) NOT NULL,
    arguments_hash VARCHAR(64) NOT NULL,
    resume_text TEXT NOT NULL,
    reply_target VARCHAR(1024) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    decision_message_id VARCHAR(512),
    decision_reason TEXT,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    decided_at TIMESTAMPTZ,
    resumed_at TIMESTAMPTZ,
    FOREIGN KEY (tenant_id, app_id) REFERENCES agent_app(tenant_id, app_id),
    FOREIGN KEY (tenant_id, revision_id) REFERENCES agent_revision(tenant_id, revision_id),
    UNIQUE (tenant_id, request_id, tool_call_id)
);

CREATE UNIQUE INDEX idx_tool_approval_decision_message
    ON tool_approval(channel_binding_id, decision_message_id)
    WHERE decision_message_id IS NOT NULL;

CREATE INDEX idx_tool_approval_pending_request
    ON tool_approval(tenant_id, request_id, created_at)
    WHERE status = 'pending';
