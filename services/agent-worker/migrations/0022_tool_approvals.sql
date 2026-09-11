CREATE TABLE tool_approval_operations (
 tenant_id text NOT NULL,
 operation_id text NOT NULL,
 run_id text NOT NULL,
 attempt_id text NOT NULL,
 invocation_id text NOT NULL,
 tool_call_id text NOT NULL,
 node_id text NOT NULL,
 tool_name text NOT NULL,
 tool_resource text NOT NULL,
 capability text NOT NULL,
 target text NOT NULL,
 parameter_summary text NOT NULL,
 arguments_digest text NOT NULL,
 arguments_json jsonb NOT NULL,
 status text NOT NULL,
 requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 expires_at timestamptz NOT NULL,
 decided_by text NOT NULL DEFAULT '',
 decided_at timestamptz,
 decision_reason text NOT NULL DEFAULT '',
 execution_started_at timestamptz,
 execution_finished_at timestamptz,
 result_summary text NOT NULL DEFAULT '',
 result_digest text NOT NULL DEFAULT '',
 result_json jsonb,
 PRIMARY KEY(tenant_id,operation_id),
 UNIQUE(tenant_id,attempt_id,invocation_id,tool_call_id),
 FOREIGN KEY(tenant_id,run_id) REFERENCES execution_runs(tenant_id,run_id),
 FOREIGN KEY(tenant_id,attempt_id) REFERENCES execution_attempts(tenant_id,attempt_id),
 CHECK(capability='test.ticket.status.update'),
 CHECK(status IN ('PENDING','APPROVED','REJECTED','EXPIRED','EXECUTING','SUCCEEDED','UNKNOWN')),
 CHECK(expires_at>requested_at),
 CHECK(jsonb_typeof(arguments_json)='object'),
 CHECK(arguments_digest ~ '^sha256:[0-9a-f]{64}$'),
 CHECK(result_digest='' OR result_digest ~ '^sha256:[0-9a-f]{64}$')
);
CREATE INDEX tool_approval_tenant_status
 ON tool_approval_operations(tenant_id,status,requested_at DESC);

CREATE FUNCTION protect_tool_approval_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF ROW(NEW.tenant_id,NEW.operation_id,NEW.run_id,NEW.attempt_id,NEW.invocation_id,
        NEW.tool_call_id,NEW.node_id,NEW.tool_name,NEW.tool_resource,NEW.capability,
        NEW.target,NEW.parameter_summary,NEW.arguments_digest,NEW.arguments_json,
        NEW.requested_at,NEW.expires_at)
    IS DISTINCT FROM
    ROW(OLD.tenant_id,OLD.operation_id,OLD.run_id,OLD.attempt_id,OLD.invocation_id,
        OLD.tool_call_id,OLD.node_id,OLD.tool_name,OLD.tool_resource,OLD.capability,
        OLD.target,OLD.parameter_summary,OLD.arguments_digest,OLD.arguments_json,
        OLD.requested_at,OLD.expires_at) THEN
   RAISE EXCEPTION 'tool approval identity is immutable';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER tool_approval_identity_immutable BEFORE UPDATE ON tool_approval_operations
 FOR EACH ROW EXECUTE FUNCTION protect_tool_approval_identity();
