-- Complete current state is separate from retained notification history/cursors.
-- No Admission or Worker authorization is enabled by this migration.
CREATE TABLE worker_authorization_snapshots (
 scope_id text NOT NULL,
 account_id text NOT NULL,
 source_epoch text NOT NULL,
 tenant_id text NOT NULL,
 provider text NOT NULL CHECK(provider IN ('telegram','wecom')),
 generation bigint NOT NULL CHECK(generation BETWEEN 1 AND 9007199254740991),
 account_revision bigint NOT NULL CHECK(account_revision BETWEEN 1 AND 9007199254740991),
 account_enabled boolean NOT NULL,
 policy_id text NOT NULL,
 policy_revision bigint NOT NULL CHECK(policy_revision BETWEEN 1 AND 9007199254740991),
 policy_digest text NOT NULL CHECK(policy_digest ~ '^sha256:[0-9a-f]{64}$'),
 policy_jsonb jsonb NOT NULL CHECK(jsonb_typeof(policy_jsonb)='object'),
 principal_count bigint NOT NULL CHECK(principal_count>=0 AND principal_count<=9007199254740991),
 principal_digest text NOT NULL CHECK(principal_digest ~ '^sha256:[0-9a-f]{64}$'),
 captured_at timestamptz NOT NULL,
 read_started_at timestamptz NOT NULL,
 fresh_until timestamptz NOT NULL,
 blocked_reason text NOT NULL DEFAULT '' CHECK(blocked_reason IN ('','SOURCE_CHANGED','SNAPSHOT_REGRESSION','SNAPSHOT_CONFLICT')),
 PRIMARY KEY(scope_id,account_id),
 UNIQUE(scope_id,account_id,generation,tenant_id,provider),
 CHECK(fresh_until>read_started_at AND fresh_until<=read_started_at+interval '30 seconds'),
 CHECK((policy_jsonb->>'tenant_id') IS NOT DISTINCT FROM tenant_id),
 CHECK((policy_jsonb->>'account_id') IS NOT DISTINCT FROM account_id),
 CHECK((policy_jsonb->>'provider') IS NOT DISTINCT FROM provider),
 CHECK((policy_jsonb->>'policy_id') IS NOT DISTINCT FROM policy_id),
 CHECK((policy_jsonb->>'revision') IS NOT DISTINCT FROM policy_revision::text),
 CHECK((policy_jsonb->>'digest') IS NOT DISTINCT FROM policy_digest)
);
CREATE TABLE worker_authorization_principals (
 scope_id text NOT NULL,
 account_id text NOT NULL,
 generation bigint NOT NULL,
 tenant_id text NOT NULL,
 provider text NOT NULL,
 principal_id text COLLATE "C" NOT NULL,
 external_user_id text NOT NULL CHECK(octet_length(external_user_id) BETWEEN 1 AND 1024),
 state text NOT NULL CHECK(state IN ('ACTIVE','REVOKED')),
 revision bigint NOT NULL CHECK(revision BETWEEN 1 AND 9007199254740991),
 PRIMARY KEY(scope_id,account_id,principal_id),
 UNIQUE(scope_id,account_id,external_user_id),
 FOREIGN KEY(scope_id,account_id,generation,tenant_id,provider)
   REFERENCES worker_authorization_snapshots(scope_id,account_id,generation,tenant_id,provider)
   DEFERRABLE INITIALLY DEFERRED
);
CREATE FUNCTION worker_authorization_snapshot_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='AUTHORIZATION_SNAPSHOT_IMMUTABLE';
 END IF;
 IF (NEW.scope_id,NEW.account_id,NEW.source_epoch,NEW.tenant_id,NEW.provider)
   IS DISTINCT FROM (OLD.scope_id,OLD.account_id,OLD.source_epoch,OLD.tenant_id,OLD.provider)
   OR NEW.generation<OLD.generation OR NEW.account_revision<OLD.account_revision
   OR NEW.policy_id<>OLD.policy_id OR NEW.policy_revision<OLD.policy_revision
   OR NEW.read_started_at<OLD.read_started_at
   OR (OLD.blocked_reason<>'' AND NEW.blocked_reason<>OLD.blocked_reason) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='AUTHORIZATION_SNAPSHOT_REGRESSION';
 END IF;
 IF NEW.policy_id=OLD.policy_id AND NEW.policy_revision=OLD.policy_revision AND
 (NEW.policy_digest,NEW.policy_jsonb) IS DISTINCT FROM (OLD.policy_digest,OLD.policy_jsonb) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='AUTHORIZATION_SNAPSHOT_CONFLICT';
 END IF;
 IF NEW.generation=OLD.generation AND (NEW.account_revision,NEW.account_enabled,NEW.policy_id,NEW.policy_revision,NEW.policy_digest,NEW.policy_jsonb,NEW.principal_count,NEW.principal_digest)
   IS DISTINCT FROM (OLD.account_revision,OLD.account_enabled,OLD.policy_id,OLD.policy_revision,OLD.policy_digest,OLD.policy_jsonb,OLD.principal_count,OLD.principal_digest) THEN
  RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='AUTHORIZATION_SNAPSHOT_CONFLICT';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER worker_authorization_snapshot_guard BEFORE UPDATE OR DELETE ON worker_authorization_snapshots
 FOR EACH ROW EXECUTE FUNCTION worker_authorization_snapshot_guard();

-- Staging uses normal DML, not CREATE TEMP TABLE: worker_runtime intentionally
-- has CONNECT only at database level. Rows exist only inside Refresh's open
-- transaction, are deleted before commit, and roll back automatically on failure.
CREATE TABLE worker_authorization_staging (
 stage_id uuid NOT NULL,
 principal_id text COLLATE "C" NOT NULL,
 external_user_id text NOT NULL CHECK(octet_length(external_user_id) BETWEEN 1 AND 1024),
 state text NOT NULL CHECK(state IN ('ACTIVE','REVOKED')),
 revision bigint NOT NULL CHECK(revision BETWEEN 1 AND 9007199254740991),
 PRIMARY KEY(stage_id,principal_id),
 UNIQUE(stage_id,external_user_id)
);
