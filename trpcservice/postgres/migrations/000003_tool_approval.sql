CREATE TABLE platform.tool_approval (
    approval_id TEXT PRIMARY KEY CHECK (approval_id <> ''),
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    config_version TEXT NOT NULL CHECK (config_version <> ''),
    request_id TEXT NOT NULL CHECK (request_id <> ''),
    session_id TEXT NOT NULL CHECK (session_id <> ''),
    tool_name TEXT NOT NULL CHECK (tool_name <> ''),
    tool_call_id TEXT NOT NULL DEFAULT '',
    argument_digest TEXT NOT NULL CHECK (argument_digest ~ '^[0-9a-f]{64}$'),
    status TEXT NOT NULL CHECK (status IN ('PENDING', 'APPROVED', 'DENIED', 'EXPIRED')),
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    decided_at TIMESTAMPTZ,
    FOREIGN KEY (tenant_id, app_id)
        REFERENCES platform.agent_app (tenant_id, app_id),
    FOREIGN KEY (tenant_id, app_id, config_version)
        REFERENCES platform.app_config_version (tenant_id, app_id, version),
    CHECK ((status = 'PENDING' AND decided_at IS NULL)
        OR (status <> 'PENDING' AND decided_at IS NOT NULL))
);

CREATE UNIQUE INDEX tool_approval_pending_context_uidx
    ON platform.tool_approval (
        tenant_id, app_id, config_version, request_id, session_id,
        tool_name, tool_call_id, argument_digest
    ) WHERE status = 'PENDING';

CREATE INDEX tool_approval_scope_idx
    ON platform.tool_approval (tenant_id, app_id, status, created_at DESC);

CREATE INDEX tool_approval_context_idx
    ON platform.tool_approval (
        tenant_id, app_id, config_version, request_id, session_id,
        tool_name, tool_call_id, argument_digest, created_at DESC
    );
