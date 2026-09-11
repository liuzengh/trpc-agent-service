-- Durable historical association; registry generation may advance independently.
CREATE TABLE execution_session_partitions (
 tenant_id text NOT NULL,
 session_id text NOT NULL,
 scope_key text NOT NULL,
 generation bigint NOT NULL CHECK(generation BETWEEN 1 AND 9007199254740991),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,session_id),
 UNIQUE(tenant_id,scope_key,generation),
 FOREIGN KEY(tenant_id,session_id) REFERENCES execution_sessions(tenant_id,session_id),
 FOREIGN KEY(tenant_id,scope_key) REFERENCES worker_conversation_registry(tenant_id,scope_key)
);
CREATE FUNCTION execution_session_partition_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='SESSION_PARTITION_IMMUTABLE'; END $$;
CREATE TRIGGER execution_session_partition_immutable BEFORE UPDATE ON execution_session_partitions
 FOR EACH ROW EXECUTE FUNCTION execution_session_partition_immutable();

-- A policy-bound Run cannot commit while its source still occupies pending.
-- The final authority check needs that row, so enforce consumption at commit,
-- not at INSERT time. Legacy receipts/runs without a partition link are unchanged.
CREATE FUNCTION execution_partitioned_run_consumed() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM execution_session_partitions s WHERE s.tenant_id=NEW.tenant_id AND s.session_id=NEW.session_id)
 AND EXISTS(SELECT 1 FROM execution_pending_intakes p WHERE p.event_id=NEW.request_json->>'EventID' OR p.run_id=NEW.run_id OR p.admission_id=NEW.admission_id)
 THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='PARTITIONED_RUN_PENDING_NOT_CONSUMED'; END IF;
 RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER execution_partitioned_run_consumed AFTER INSERT ON execution_runs
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION execution_partitioned_run_consumed();
