-- Dependencies are one atomic extension of the current snapshot, never an
-- independently renewable grant. Legacy rows stay NULL until a complete read.
ALTER TABLE worker_authorization_snapshots
 ADD COLUMN session_policy_jsonb jsonb,
 ADD COLUMN quota_policy_jsonb jsonb,
 ADD CONSTRAINT worker_authorization_dependencies_complete CHECK (
  (session_policy_jsonb IS NULL AND quota_policy_jsonb IS NULL) OR
  ((jsonb_typeof(session_policy_jsonb)='object' AND jsonb_typeof(quota_policy_jsonb)='object'
   AND octet_length(session_policy_jsonb::text)<=65536 AND octet_length(quota_policy_jsonb::text)<=65536
   AND session_policy_jsonb->>'tenant_id'=tenant_id AND quota_policy_jsonb->>'tenant_id'=tenant_id
   AND session_policy_jsonb->>'kind'='session' AND quota_policy_jsonb->>'kind'='quota'
   AND session_policy_jsonb->>'policy_id'=policy_jsonb->'body'->'session_policy'->>'id'
   AND session_policy_jsonb->>'revision'=policy_jsonb->'body'->'session_policy'->>'revision'
   AND session_policy_jsonb->>'digest'=policy_jsonb->'body'->'session_policy'->>'digest'
   AND quota_policy_jsonb->>'policy_id'=policy_jsonb->'body'->'tenant_quota_ref'->>'id'
   AND quota_policy_jsonb->>'revision'=policy_jsonb->'body'->'tenant_quota_ref'->>'revision'
   AND quota_policy_jsonb->>'digest'=policy_jsonb->'body'->'tenant_quota_ref'->>'digest') IS TRUE)
 );
CREATE FUNCTION worker_authorization_dependencies_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.session_policy_jsonb IS NOT NULL AND NEW.policy_id=OLD.policy_id
 AND NEW.policy_revision=OLD.policy_revision
 AND (NEW.session_policy_jsonb,NEW.quota_policy_jsonb) IS DISTINCT FROM (OLD.session_policy_jsonb,OLD.quota_policy_jsonb) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='AUTHORIZATION_DEPENDENCIES_IMMUTABLE';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER worker_authorization_dependencies_guard BEFORE UPDATE ON worker_authorization_snapshots
 FOR EACH ROW EXECUTE FUNCTION worker_authorization_dependencies_guard();
