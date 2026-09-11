-- Forward-only replacement of the retired governance path. Previously published
-- migration bytes and historical facts remain intact; this is not a data purge.
-- Outstanding governed inputs need explicit disposition before this cutover;
-- never silently turn an old permission-bearing request into a public message.
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM execution_pending_intakes)
 OR EXISTS(SELECT 1 FROM execution_runs WHERE status IN ('QUEUED','RUNNING','RETRY_WAIT')
   AND (COALESCE(request_json->'Authorization','null'::jsonb)<>'null'::jsonb
     OR COALESCE(request_json->'authorization','null'::jsonb)<>'null'::jsonb)) THEN
  RAISE EXCEPTION 'MINIMAL_SESSION_CUTOVER_REQUIRES_GOVERNED_INPUT_DISPOSITION';
 END IF;
END $$;
DROP TRIGGER execution_attempt_authorization_guard ON execution_attempts;
DROP FUNCTION execution_attempt_authorization_guard();
DROP TRIGGER execution_partitioned_run_consumed ON execution_runs;
DROP FUNCTION execution_partitioned_run_consumed();

-- Only observed identity and conversation ownership; no access state, policy,
-- quota, login linkage, cross-provider merge, or automatic history migration.
CREATE TABLE execution_social_identities (
 tenant_id text NOT NULL,
 identity_id text NOT NULL,
 provider text NOT NULL CHECK(provider IN ('telegram','wecom')),
 account_id text NOT NULL,
 external_user_id text NOT NULL CHECK(length(external_user_id)>0),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,identity_id),
 UNIQUE(tenant_id,provider,account_id,external_user_id)
);
CREATE TABLE execution_session_identities (
 tenant_id text NOT NULL,
 session_id text NOT NULL,
 identity_id text NOT NULL,
 PRIMARY KEY(tenant_id,session_id),
 FOREIGN KEY(tenant_id,session_id) REFERENCES execution_sessions(tenant_id,session_id) ON DELETE CASCADE,
 FOREIGN KEY(tenant_id,identity_id) REFERENCES execution_social_identities(tenant_id,identity_id)
);
CREATE INDEX execution_session_identity_lookup ON execution_session_identities(tenant_id,identity_id);
