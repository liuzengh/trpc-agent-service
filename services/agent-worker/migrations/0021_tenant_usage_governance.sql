-- Shared Worker quota state. Unknown Provider usage deliberately holds the
-- reservation instead of being accounted as zero.
CREATE TABLE worker_tenant_usage_reservations_v1 (
 tenant_id text NOT NULL,
 attempt_id text NOT NULL,
 run_id text NOT NULL,
 policy_revision bigint NOT NULL CHECK(policy_revision BETWEEN 1 AND 9007199254740991),
 period_start timestamptz NOT NULL,
 period_seconds bigint NOT NULL CHECK(period_seconds BETWEEN 3600 AND 31536000),
 reserved_tokens bigint NOT NULL CHECK(reserved_tokens BETWEEN 1 AND 9007199254740991),
 token_limit bigint NOT NULL CHECK(token_limit BETWEEN 1 AND 9007199254740991),
 input_micros_per_million_tokens bigint NOT NULL CHECK(input_micros_per_million_tokens >= 0),
 output_micros_per_million_tokens bigint NOT NULL CHECK(output_micros_per_million_tokens >= 0),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,attempt_id),
 FOREIGN KEY(tenant_id,attempt_id) REFERENCES execution_attempts(tenant_id,attempt_id),
 FOREIGN KEY(tenant_id,run_id) REFERENCES execution_runs(tenant_id,run_id)
);
CREATE TABLE worker_tenant_usage_settlements_v1 (
 tenant_id text NOT NULL,
 attempt_id text NOT NULL,
 usage_known boolean NOT NULL,
 input_tokens bigint CHECK(input_tokens BETWEEN 0 AND 9007199254740991),
 output_tokens bigint CHECK(output_tokens BETWEEN 0 AND 9007199254740991),
 total_tokens bigint CHECK(total_tokens BETWEEN 0 AND 9007199254740991),
 estimated_cost_micros bigint CHECK(estimated_cost_micros >= 0),
 settled_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,attempt_id),
 FOREIGN KEY(tenant_id,attempt_id) REFERENCES worker_tenant_usage_reservations_v1(tenant_id,attempt_id),
 CHECK((usage_known AND input_tokens IS NOT NULL AND output_tokens IS NOT NULL AND total_tokens=input_tokens+output_tokens AND estimated_cost_micros IS NOT NULL)
    OR (NOT usage_known AND input_tokens IS NULL AND output_tokens IS NULL AND total_tokens IS NULL AND estimated_cost_micros IS NULL))
);
CREATE INDEX worker_tenant_usage_period_v1 ON worker_tenant_usage_reservations_v1(tenant_id,period_start);
