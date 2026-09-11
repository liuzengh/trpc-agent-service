-- A physical-dispatch transition is not an idempotent permission receipt.
-- UPDATE OF avoids rejecting legitimate lease renewal on EXECUTING attempts.
CREATE FUNCTION execution_single_dispatch() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.agent_started_at IS NOT NULL AND NEW.agent_started_at IS DISTINCT FROM OLD.agent_started_at THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='MODEL_DISPATCH_ALREADY_STARTED';
 END IF;
 IF NEW.status='PREPARING' AND OLD.status<>'PREPARING' THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='MODEL_DISPATCH_STATE_REGRESSION';
 END IF;
 IF NEW.status='EXECUTING' AND (OLD.status<>'PREPARING' OR OLD.agent_started_at IS NOT NULL OR NEW.agent_started_at IS NULL) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='MODEL_DISPATCH_ALREADY_STARTED';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER execution_single_dispatch BEFORE UPDATE OF status,agent_started_at ON execution_attempts
 FOR EACH ROW EXECUTE FUNCTION execution_single_dispatch();
