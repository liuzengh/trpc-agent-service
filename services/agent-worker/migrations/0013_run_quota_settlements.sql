ALTER TABLE worker_run_quota_reservations ADD UNIQUE(run_id,tenant_id,fingerprint);
CREATE TABLE worker_run_quota_settlements (
 run_id text PRIMARY KEY,
 tenant_id text NOT NULL,
 reservation_fingerprint text NOT NULL,
 completion_id text NOT NULL,
 result_digest text NOT NULL CHECK(result_digest ~ '^sha256:[0-9a-f]{64}$'),
 disposition text NOT NULL CHECK(disposition IN ('NO_ATTEMPT','SUCCEEDED_ATTEMPT')),
 released_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 FOREIGN KEY(run_id,tenant_id,reservation_fingerprint) REFERENCES worker_run_quota_reservations(run_id,tenant_id,fingerprint)
);
CREATE TRIGGER worker_run_quota_settlement_immutable BEFORE UPDATE OR DELETE ON worker_run_quota_settlements
 FOR EACH ROW EXECUTE FUNCTION worker_run_quota_immutable();
