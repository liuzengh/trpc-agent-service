ALTER TABLE gateway_policy_sources
 ADD COLUMN processed_sequence bigint NOT NULL DEFAULT 0 CHECK(processed_sequence BETWEEN 0 AND 9007199254740991),
 ADD COLUMN observed_sequence bigint NOT NULL DEFAULT 0 CHECK(observed_sequence BETWEEN processed_sequence AND 9007199254740991);
ALTER TABLE gateway_policy_sources DROP CONSTRAINT gateway_policy_sources_blocked_reason_check;
ALTER TABLE gateway_policy_sources ADD CONSTRAINT gateway_policy_sources_blocked_reason_check
 CHECK(blocked_reason IN ('','SOURCE_CHANGED','UNBOUND_HISTORY','POSITION_CONFLICT'));
CREATE TABLE gateway_policy_processed_messages (
 scope_id text NOT NULL REFERENCES gateway_policy_sources(scope_id),
 sequence bigint NOT NULL CHECK(sequence BETWEEN 1 AND 9007199254740991),
 event_digest text NOT NULL CHECK(event_digest ~ '^sha256:[0-9a-f]{64}$'),
 PRIMARY KEY(scope_id,sequence)
);
CREATE TRIGGER gateway_policy_processed_message_immutable BEFORE UPDATE OR DELETE
 ON gateway_policy_processed_messages FOR EACH ROW EXECUTE FUNCTION gateway_policy_document_immutable();
CREATE FUNCTION gateway_policy_checkpoint_fence() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.processed_sequence < OLD.processed_sequence OR NEW.observed_sequence < OLD.observed_sequence THEN
  RAISE EXCEPTION 'policy checkpoint cannot regress' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END;
$$;
CREATE TRIGGER gateway_policy_checkpoint_fence BEFORE UPDATE ON gateway_policy_sources
 FOR EACH ROW EXECUTE FUNCTION gateway_policy_checkpoint_fence();
