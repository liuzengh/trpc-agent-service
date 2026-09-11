-- Worker owns generation and command result, not Gateway or a process-local map.
CREATE TABLE worker_conversation_registry (
 tenant_id text NOT NULL,
 scope_key text NOT NULL,
 scope_json jsonb NOT NULL CHECK(jsonb_typeof(scope_json)='array'),
 generation bigint NOT NULL CHECK(generation BETWEEN 1 AND 9007199254740991),
 PRIMARY KEY(tenant_id,scope_key)
);
CREATE FUNCTION worker_conversation_registry_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  IF NEW.generation<>1 THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='SESSION_INITIAL_GENERATION_INVALID'; END IF;
  RETURN NEW;
 END IF;
 IF TG_OP='DELETE' THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='SESSION_REGISTRY_IMMUTABLE'; END IF;
 IF (NEW.tenant_id,NEW.scope_key,NEW.scope_json) IS DISTINCT FROM (OLD.tenant_id,OLD.scope_key,OLD.scope_json)
 OR NEW.generation<>OLD.generation+1 THEN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='SESSION_REGISTRY_INVALID_ADVANCE'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER worker_conversation_registry_guard BEFORE INSERT OR UPDATE OR DELETE ON worker_conversation_registry
 FOR EACH ROW EXECUTE FUNCTION worker_conversation_registry_guard();
CREATE TABLE worker_conversation_commands (
 command_id text PRIMARY KEY,
 request_digest text NOT NULL CHECK(request_digest ~ '^sha256:[0-9a-f]{64}$'),
 tenant_id text NOT NULL,
 scope_key text NOT NULL,
 actor_id text NOT NULL,
 operation text NOT NULL CHECK(operation IN ('session.new','session.reset_shared')),
 generation bigint NOT NULL CHECK(generation BETWEEN 2 AND 9007199254740991),
 session_id text NOT NULL,
 applied_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 FOREIGN KEY(tenant_id,scope_key) REFERENCES worker_conversation_registry(tenant_id,scope_key)
);
CREATE TABLE worker_conversation_audit_outbox (
 command_id text PRIMARY KEY REFERENCES worker_conversation_commands(command_id),
 tenant_id text NOT NULL,
 scope_key text NOT NULL,
 actor_id text NOT NULL,
 operation text NOT NULL,
 generation bigint NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE FUNCTION worker_conversation_command_guard() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='SESSION_COMMAND_IMMUTABLE'; END $$;
CREATE TRIGGER worker_conversation_command_guard BEFORE UPDATE OR DELETE ON worker_conversation_commands
 FOR EACH ROW EXECUTE FUNCTION worker_conversation_command_guard();
ALTER TABLE worker_conversation_commands ADD CONSTRAINT worker_conversation_generation_unique UNIQUE(tenant_id,scope_key,generation);
CREATE FUNCTION worker_conversation_advance_complete() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM worker_conversation_commands c
 JOIN worker_conversation_audit_outbox a ON a.command_id=c.command_id AND a.tenant_id=c.tenant_id AND a.scope_key=c.scope_key AND a.actor_id=c.actor_id AND a.operation=c.operation AND a.generation=c.generation
 WHERE c.tenant_id=NEW.tenant_id AND c.scope_key=NEW.scope_key AND c.generation=NEW.generation) THEN
 RAISE EXCEPTION USING ERRCODE='23514',MESSAGE='SESSION_ADVANCE_REQUIRES_COMMAND_AND_AUDIT';
 END IF;
 RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER worker_conversation_advance_complete AFTER UPDATE ON worker_conversation_registry
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION worker_conversation_advance_complete();
