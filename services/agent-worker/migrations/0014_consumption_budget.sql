-- Consumption reservations do not count Runs and do not infer provider pricing.
CREATE TABLE worker_consumption_reservations (
 operation_id text PRIMARY KEY,
 tenant_id text NOT NULL,
 run_id text NOT NULL,
 input_digest text NOT NULL CHECK(input_digest ~ '^sha256:[0-9a-f]{64}$'),
 bound_digest text NOT NULL CHECK(bound_digest ~ '^sha256:[0-9a-f]{64}$'),
 unit text NOT NULL CHECK(unit IN ('model_tokens','tool_units')),
 maximum bigint NOT NULL CHECK(maximum BETWEEN 1 AND 9007199254740991),
 fingerprint text NOT NULL CHECK(fingerprint ~ '^sha256:[0-9a-f]{64}$'),
 grant_jsonb jsonb NOT NULL CHECK(jsonb_typeof(grant_jsonb)='object'),
 reserved_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(operation_id,tenant_id,fingerprint,unit,maximum)
);
CREATE INDEX worker_consumption_tenant_unit ON worker_consumption_reservations(tenant_id,unit);
CREATE TABLE worker_consumption_settlements (
 operation_id text PRIMARY KEY,
 tenant_id text NOT NULL,
 fingerprint text NOT NULL,
 unit text NOT NULL,
 maximum bigint NOT NULL,
 actual bigint NOT NULL CHECK(actual BETWEEN 0 AND 9007199254740991),
 evidence_id text NOT NULL,
 evidence_digest text NOT NULL CHECK(evidence_digest ~ '^sha256:[0-9a-f]{64}$'),
 bound_violated boolean GENERATED ALWAYS AS (actual > maximum) STORED,
 settled_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(tenant_id,evidence_id),
 FOREIGN KEY(operation_id,tenant_id,fingerprint,unit,maximum)
 REFERENCES worker_consumption_reservations(operation_id,tenant_id,fingerprint,unit,maximum)
);
CREATE FUNCTION worker_consumption_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='CONSUMPTION_FACT_IMMUTABLE'; END $$;
CREATE TRIGGER worker_consumption_reservation_immutable BEFORE UPDATE OR DELETE ON worker_consumption_reservations
 FOR EACH ROW EXECUTE FUNCTION worker_consumption_immutable();
CREATE TRIGGER worker_consumption_settlement_immutable BEFORE UPDATE OR DELETE ON worker_consumption_settlements
 FOR EACH ROW EXECUTE FUNCTION worker_consumption_immutable();
