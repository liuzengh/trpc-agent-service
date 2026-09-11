-- Durable unassigned inputs are not Execution Receipts/Runs or broker ACKs.
CREATE TABLE execution_pending_intakes (
 event_id text PRIMARY KEY,
 event_digest text NOT NULL,
 run_id text NOT NULL,
 admission_id text NOT NULL,
 run_digest text NOT NULL,
 tenant_id text NOT NULL,
 scope_id text NOT NULL,
 source_epoch text NOT NULL,
 account_id text NOT NULL,
 provider text NOT NULL CHECK(provider IN ('telegram','wecom')),
 generation bigint NOT NULL CHECK(generation BETWEEN 1 AND 9007199254740991),
 request_json jsonb NOT NULL CHECK(jsonb_typeof(request_json)='object' AND octet_length(request_json::text)<=2097152),
 policy_json jsonb NOT NULL CHECK(jsonb_typeof(policy_json)='object'),
 expires_at timestamptz NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 CHECK((request_json->>'EventID') IS NOT DISTINCT FROM event_id),
 CHECK((request_json->>'EventDigest') IS NOT DISTINCT FROM event_digest),
 CHECK((request_json->>'RunID') IS NOT DISTINCT FROM run_id),
 CHECK((request_json->>'RunDigest') IS NOT DISTINCT FROM run_digest),
 CHECK((request_json->>'AdmissionID') IS NOT DISTINCT FROM admission_id),
 CHECK((request_json->'Route'->>'TenantID') IS NOT DISTINCT FROM tenant_id),
 CHECK((request_json->'Route'->>'AccountID') IS NOT DISTINCT FROM account_id),
 CHECK((request_json->'Route'->>'Provider') IS NOT DISTINCT FROM provider),
 CHECK((request_json->'Authorization'->>'scope_id') IS NOT DISTINCT FROM scope_id),
 CHECK((request_json->'Authorization'->>'source_epoch') IS NOT DISTINCT FROM source_epoch),
 CHECK((request_json->'Authorization'->>'generation') IS NOT DISTINCT FROM generation::text)
);
CREATE INDEX execution_pending_intakes_run ON execution_pending_intakes(run_id);
CREATE INDEX execution_pending_intakes_admission ON execution_pending_intakes(admission_id);
CREATE INDEX execution_pending_intakes_targets ON execution_pending_intakes(scope_id,expires_at);
CREATE FUNCTION execution_pending_intake_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='PENDING_INTAKE_IMMUTABLE'; END $$;
CREATE TRIGGER execution_pending_intake_immutable BEFORE UPDATE ON execution_pending_intakes
 FOR EACH ROW EXECUTE FUNCTION execution_pending_intake_immutable();
