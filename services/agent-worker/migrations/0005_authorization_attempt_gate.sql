-- Protect rolling upgrades: an older binary may ignore the new Requested field.
-- An Admission observation is not current execution authority. Replace this
-- fail-closed gate with the independent current-projection fence only when the
-- complete Worker authorization path is implemented; do not remove it to deploy.
CREATE FUNCTION execution_attempt_authorization_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE body jsonb;
BEGIN
 SELECT request_json INTO body FROM execution_runs
 WHERE tenant_id=NEW.tenant_id AND run_id=NEW.run_id FOR UPDATE;
 IF (body ? 'Authorization' AND body->'Authorization'<>'null'::jsonb)
    OR (body ? 'authorization' AND body->'authorization'<>'null'::jsonb) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='AUTHORIZATION_NOT_READY';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER execution_attempt_authorization_guard BEFORE INSERT ON execution_attempts
 FOR EACH ROW EXECUTE FUNCTION execution_attempt_authorization_guard();
