-- Run-count reservations are distinct from model/tool cost reservations.
CREATE TABLE worker_run_quota_reservations (
 run_id text PRIMARY KEY,
 tenant_id text NOT NULL,
 input_digest text NOT NULL CHECK(input_digest ~ '^sha256:[0-9a-f]{64}$'),
 fingerprint text NOT NULL CHECK(fingerprint ~ '^sha256:[0-9a-f]{64}$'),
 quota_id text NOT NULL,
 quota_revision bigint NOT NULL CHECK(quota_revision BETWEEN 1 AND 9007199254740991),
 quota_digest text NOT NULL CHECK(quota_digest ~ '^sha256:[0-9a-f]{64}$'),
 quota_jsonb jsonb NOT NULL CHECK(jsonb_typeof(quota_jsonb)='object'),
 reserved_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK((quota_jsonb->>'tenant_id') IS NOT DISTINCT FROM tenant_id),
 CHECK((quota_jsonb->>'policy_id') IS NOT DISTINCT FROM quota_id),
 CHECK((quota_jsonb->>'revision') IS NOT DISTINCT FROM quota_revision::text),
 CHECK((quota_jsonb->>'digest') IS NOT DISTINCT FROM quota_digest),
 CHECK((quota_jsonb->>'kind') IS NOT DISTINCT FROM 'quota')
);
CREATE INDEX worker_run_quota_tenant_time ON worker_run_quota_reservations(tenant_id,reserved_at);
CREATE FUNCTION worker_run_quota_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='RUN_QUOTA_RESERVATION_IMMUTABLE'; END $$;
CREATE TRIGGER worker_run_quota_immutable BEFORE UPDATE OR DELETE ON worker_run_quota_reservations
 FOR EACH ROW EXECUTE FUNCTION worker_run_quota_immutable();
