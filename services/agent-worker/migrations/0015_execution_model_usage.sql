-- Final reported usage is an Execution fact, independent of Session Stage and
-- Completion success. It is not a provider invoice or a budget authorization.
CREATE TABLE execution_model_usage (
 operation_id text PRIMARY KEY,
 tenant_id text NOT NULL,
 run_id text NOT NULL,
 attempt_id text NOT NULL,
 input_tokens bigint NOT NULL CHECK(input_tokens BETWEEN 0 AND 9007199254740991),
 output_tokens bigint NOT NULL CHECK(output_tokens BETWEEN 0 AND 9007199254740991),
 total_tokens bigint NOT NULL CHECK(total_tokens BETWEEN 0 AND 9007199254740991),
 result_digest text NOT NULL CHECK(result_digest ~ '^sha256:[0-9a-f]{64}$'),
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK(input_tokens+output_tokens=total_tokens),
 UNIQUE(tenant_id,attempt_id),
 FOREIGN KEY(tenant_id,attempt_id) REFERENCES execution_attempts(tenant_id,attempt_id),
 FOREIGN KEY(tenant_id,run_id) REFERENCES execution_runs(tenant_id,run_id)
);
CREATE FUNCTION execution_model_usage_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='MODEL_USAGE_IMMUTABLE'; END $$;
CREATE TRIGGER execution_model_usage_immutable BEFORE UPDATE OR DELETE ON execution_model_usage
 FOR EACH ROW EXECUTE FUNCTION execution_model_usage_immutable();
