-- Console data is separate from operational IM conversations. Separate tables
-- keep Worker database grants from exposing browser authentication sessions.
CREATE TABLE admin_session (
    tenant_id VARCHAR(128) NOT NULL DEFAULT '',
    app_id VARCHAR(128) NOT NULL DEFAULT '',
    record_id VARCHAR(128) NOT NULL,
    owner_id VARCHAR(128) NOT NULL,
    status VARCHAR(32) NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    data JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, record_id)
);
CREATE TABLE agent_draft (LIKE admin_session INCLUDING ALL);
CREATE TABLE debug_snapshot (LIKE admin_session INCLUDING ALL);
CREATE TABLE debug_session (LIKE admin_session INCLUDING ALL);
CREATE TABLE debug_run (LIKE admin_session INCLUDING ALL);
CREATE TABLE debug_event (LIKE admin_session INCLUDING ALL);
CREATE TABLE debug_tool_execution (LIKE admin_session INCLUDING ALL);
CREATE TABLE debug_tool_approval (LIKE admin_session INCLUDING ALL);
CREATE TABLE debug_approval_decision (LIKE admin_session INCLUDING ALL);
CREATE TABLE console_worker (LIKE admin_session INCLUDING ALL);
CREATE INDEX admin_session_expiry ON admin_session(expires_at);
CREATE INDEX agent_draft_app ON agent_draft(tenant_id, app_id, updated_at DESC);
CREATE INDEX debug_run_queue ON debug_run(status, updated_at);
CREATE INDEX debug_run_app ON debug_run(tenant_id, app_id, created_at DESC);
CREATE INDEX debug_run_session ON debug_run(tenant_id, owner_id, (data->>'session_id'), created_at);
CREATE INDEX debug_event_run ON debug_event(tenant_id, app_id, created_at);
CREATE INDEX debug_session_expiry ON debug_session(expires_at);
CREATE INDEX debug_snapshot_expiry ON debug_snapshot(expires_at);
