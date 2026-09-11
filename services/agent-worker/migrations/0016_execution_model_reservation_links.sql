-- Execution links an operation to a verified Budget receipt via an owner port,
-- not cross-module SQL. Same-owner transaction commits both facts atomically.
CREATE TABLE execution_model_reservation_links (
 operation_id text PRIMARY KEY,
 tenant_id text NOT NULL,
 run_id text NOT NULL,
 attempt_id text NOT NULL,
 reservation_fingerprint text NOT NULL CHECK(reservation_fingerprint ~ '^sha256:[0-9a-f]{64}$'),
 input_digest text NOT NULL CHECK(input_digest ~ '^sha256:[0-9a-f]{64}$'),
 bound_digest text NOT NULL CHECK(bound_digest ~ '^sha256:[0-9a-f]{64}$'),
 maximum bigint NOT NULL CHECK(maximum BETWEEN 1 AND 9007199254740991),
 linked_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(tenant_id,attempt_id),
 FOREIGN KEY(tenant_id,attempt_id) REFERENCES execution_attempts(tenant_id,attempt_id),
 FOREIGN KEY(tenant_id,run_id) REFERENCES execution_runs(tenant_id,run_id)
);
CREATE TRIGGER execution_model_reservation_link_immutable BEFORE UPDATE OR DELETE ON execution_model_reservation_links
 FOR EACH ROW EXECUTE FUNCTION execution_model_usage_immutable();
