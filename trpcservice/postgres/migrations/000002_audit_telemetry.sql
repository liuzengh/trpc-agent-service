ALTER TABLE platform.execution
    ADD COLUMN IF NOT EXISTS trace_parent TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS trace_state TEXT NOT NULL DEFAULT '';

DO $$
DECLARE
    constraint_name TEXT;
BEGIN
    FOR constraint_name IN
        SELECT con.conname
        FROM pg_constraint con
        WHERE con.conrelid = 'platform.channel_inbox'::regclass
          AND pg_get_constraintdef(con.oid) LIKE '%UNSUPPORTED_MESSAGE_TYPE%'
    LOOP
        EXECUTE format('ALTER TABLE platform.channel_inbox DROP CONSTRAINT %I', constraint_name);
    END LOOP;
END;
$$;

ALTER TABLE platform.channel_inbox
    ADD CONSTRAINT channel_inbox_reject_reason_check CHECK (
        (status = 'ADMITTED' AND reject_reason IS NULL)
        OR (status = 'REJECTED' AND reject_reason IN (
            'UNSUPPORTED_MESSAGE_TYPE', 'ATTACHMENT_REJECTED', 'IM_ACCESS_DENIED'
        ))
    );

CREATE TABLE platform.audit_event (
    audit_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    channel TEXT NOT NULL DEFAULT '',
    user_id TEXT NOT NULL DEFAULT '',
    session_id TEXT NOT NULL DEFAULT '',
    agent_name TEXT NOT NULL DEFAULT '',
    tool_name TEXT NOT NULL DEFAULT '',
    decision TEXT NOT NULL,
    latency BIGINT NOT NULL DEFAULT 0 CHECK (latency >= 0),
    error_type TEXT NOT NULL DEFAULT '',
    input_tokens INTEGER NOT NULL DEFAULT 0 CHECK (input_tokens >= 0),
    output_tokens INTEGER NOT NULL DEFAULT 0 CHECK (output_tokens >= 0),
    total_tokens INTEGER NOT NULL DEFAULT 0 CHECK (total_tokens >= 0),
    cost DOUBLE PRECISION CHECK (cost IS NULL OR cost >= 0),
    trace_id TEXT NOT NULL,
    request_id TEXT NOT NULL,
    config_version TEXT NOT NULL,
    event_type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (tenant_id, app_id)
        REFERENCES platform.agent_app (tenant_id, app_id)
);

CREATE INDEX audit_event_scope_idx
    ON platform.audit_event (tenant_id, app_id, created_at, audit_event_id);

CREATE INDEX audit_event_execution_idx
    ON platform.audit_event (tenant_id, app_id, request_id, created_at);

CREATE INDEX audit_event_trace_idx
    ON platform.audit_event (tenant_id, app_id, trace_id, created_at);
