-- Down migration for P2-01. It removes only the objects this migration
-- created: the three narrow SECURITY DEFINER functions, the per-table RLS
-- policies and the RLS flags. The trpc_claim_owner role is cluster-level
-- deployment state and is intentionally not dropped here; deployments that
-- want full symmetric teardown may drop it manually once no dependent
-- functions remain.
DO $mig$
DECLARE
    schema_name text := current_schema();
BEGIN
    IF schema_name IS NULL OR schema_name = '' THEN
        RAISE EXCEPTION 'current_schema is empty; refusing to remove RLS without a target schema';
    END IF;
    EXECUTE 'DROP FUNCTION IF EXISTS ' || quote_ident(schema_name) || '.trpc_queue_claim_next(bigint, text)';
    EXECUTE 'DROP FUNCTION IF EXISTS ' || quote_ident(schema_name) || '.trpc_vector_task_candidate(timestamptz)';
    EXECUTE 'DROP FUNCTION IF EXISTS ' || quote_ident(schema_name) || '.trpc_binding_resolve(text, text)';
END
$mig$;

DO $mig$
DECLARE
    tenant_tables text[] := ARRAY[
        'tenant', 'agent_app', 'channel_binding', 'user_identity',
        'session', 'session_event', 'message_dedup', 'memory', 'summary',
        'artifact', 'audit_log', 'outbox_message', 'dead_letter',
        'agent_release', 'tenant_config_version', 'coordination_epoch',
        'session_lease', 'execution_result', 'job_queue',
        'channel_binding_audit', 'vector_projection_task',
        'vector_rebuild_run', 'tenant_config_rollout',
        'tenant_config_operation'
    ];
    t text;
    policy_name text;
    schema_name text := current_schema();
BEGIN
    IF schema_name IS NULL OR schema_name = '' THEN
        RAISE EXCEPTION 'current_schema is empty';
    END IF;
    FOREACH t IN ARRAY tenant_tables LOOP
        IF EXISTS (
            SELECT 1 FROM pg_tables
            WHERE schemaname = schema_name AND tablename = t
        ) THEN
            policy_name := t || '_tenant_isolation';
            EXECUTE format('DROP POLICY IF EXISTS %I ON %I.%I', policy_name, schema_name, t);
            EXECUTE format('ALTER TABLE %I.%I NO FORCE ROW LEVEL SECURITY', schema_name, t);
            EXECUTE format('ALTER TABLE %I.%I DISABLE ROW LEVEL SECURITY', schema_name, t);
        END IF;
    END LOOP;
END
$mig$;
