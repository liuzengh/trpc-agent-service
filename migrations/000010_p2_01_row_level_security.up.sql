-- P2-01: database-enforced tenant isolation (row level security).
--
-- Every tenant fact/coordination/derived-task table gets a mandatory RLS
-- policy keyed on the transaction-local custom GUC trpc.tenant_id, which the
-- application sets with set_config('trpc.tenant_id', $1, true) inside the
-- same transaction that runs the SQL. A missing or empty setting hides every
-- row (reads return zero rows) and rejects every write (WITH CHECK fails).
-- The application-level tenant predicates remain in place; RLS is an
-- additional database-enforced defence, not a replacement.
--
-- The three narrow cross-tenant capabilities required by the verified
-- runtime contract (global queue claim, vector task candidate discovery and
-- pre-tenant webhook binding resolution) are exposed as fixed SECURITY
-- DEFINER functions owned by the dedicated NOLOGIN BYPASSRLS role
-- trpc_claim_owner. They contain no dynamic SQL, accept bounded parameters,
-- return bounded projections, are revoked from PUBLIC and granted only to
-- the runtime role when it exists. They cannot list tenants, enumerate
-- arbitrary tables or return secrets beyond the binding reference columns
-- the pre-tenant resolver already returns.

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
        RAISE EXCEPTION 'current_schema is empty; refusing to install RLS without a target schema';
    END IF;
    FOREACH t IN ARRAY tenant_tables LOOP
        IF NOT EXISTS (
            SELECT 1 FROM pg_tables
            WHERE schemaname = schema_name AND tablename = t
        ) THEN
            RAISE EXCEPTION 'expected table %.% is missing; migration order broken', schema_name, t;
        END IF;
        EXECUTE format('ALTER TABLE %I.%I ENABLE ROW LEVEL SECURITY', schema_name, t);
        EXECUTE format('ALTER TABLE %I.%I FORCE ROW LEVEL SECURITY', schema_name, t);
        policy_name := t || '_tenant_isolation';
        IF NOT EXISTS (
            SELECT 1
            FROM pg_policy p
            JOIN pg_class c ON c.oid = p.polrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = schema_name AND c.relname = t AND p.polname = policy_name
        ) THEN
            EXECUTE format(
                'CREATE POLICY %I ON %I.%I USING (tenant_id = current_setting(''trpc.tenant_id'', true)) WITH CHECK (tenant_id = current_setting(''trpc.tenant_id'', true))',
                policy_name, schema_name, t);
        END IF;
    END LOOP;
END
$mig$;

-- Dedicated NOLOGIN owner for the narrow SECURITY DEFINER functions. It owns
-- no tables, cannot log in, and is not a superuser; BYPASSRLS is required so
-- the fixed claim functions keep working under FORCE ROW LEVEL SECURITY.
DO $mig$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_claim_owner') THEN
        IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = current_user AND rolsuper) THEN
            CREATE ROLE trpc_claim_owner NOLOGIN NOSUPERUSER NOINHERIT NOCREATEDB NOCREATEROLE BYPASSRLS;
        ELSE
            RAISE EXCEPTION 'role trpc_claim_owner must be pre-created by the deployment before running this migration';
        END IF;
    END IF;
END
$mig$;

CREATE FUNCTION trpc_queue_claim_next(
    p_visibility_micros bigint,
    p_delivery_id text
) RETURNS TABLE (
    tenant_id text,
    job_id text,
    execution_id text,
    schema_version integer,
    payload jsonb,
    attempt integer,
    leased_until timestamptz,
    received_at timestamptz
) LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT AS $fn$
DECLARE
    candidate record;
    claim_leased_until timestamptz;
    claim_received_at timestamptz;
BEGIN
    UPDATE job_queue
    SET status = 'queued', delivery_id = NULL, leased_until = NULL,
        available_at = clock_timestamp(), attempt = job_queue.attempt + 1,
        updated_at = clock_timestamp()
    WHERE job_queue.status = 'in_flight' AND job_queue.leased_until <= clock_timestamp();

    SELECT j.tenant_id, j.job_id, j.execution_id, j.schema_version, j.payload, j.attempt
    INTO candidate
    FROM job_queue j
    WHERE j.status = 'queued' AND j.available_at <= clock_timestamp()
    ORDER BY j.available_at, j.created_at, j.job_id
    FOR UPDATE SKIP LOCKED
    LIMIT 1;
    IF NOT FOUND THEN
        RETURN;
    END IF;

    UPDATE job_queue j
    SET status = 'in_flight', delivery_id = p_delivery_id,
        leased_until = clock_timestamp() + (p_visibility_micros::double precision * interval '1 microsecond'),
        delivery_count = j.delivery_count + 1, updated_at = clock_timestamp()
    WHERE j.tenant_id = candidate.tenant_id AND j.job_id = candidate.job_id
      AND j.status = 'queued' AND j.available_at <= clock_timestamp()
    RETURNING j.leased_until, clock_timestamp()
    INTO claim_leased_until, claim_received_at;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'claim candidate disappeared' USING ERRCODE = 'P0002';
    END IF;

    RETURN QUERY
    SELECT candidate.tenant_id, candidate.job_id, candidate.execution_id,
           candidate.schema_version, candidate.payload, candidate.attempt,
           claim_leased_until, claim_received_at;
END
$fn$;

CREATE FUNCTION trpc_vector_task_candidate(
    p_now timestamptz
) RETURNS TABLE (
    tenant_id text,
    task_id text
) LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $fn$
    SELECT v.tenant_id, v.task_id FROM vector_projection_task v
    WHERE ((v.status IN ('pending', 'retry_wait') AND v.next_attempt_at <= p_now)
        OR (v.status = 'running' AND v.lease_expires_at IS NOT NULL AND v.lease_expires_at <= p_now))
    ORDER BY v.created_at, v.task_id LIMIT 1
$fn$;

CREATE FUNCTION trpc_binding_resolve(
    p_channel text,
    p_external_app_id text
) RETURNS TABLE (
    tenant_id text,
    channel text,
    binding_id text,
    external_app_id text,
    secret_ref text,
    verify_token_ref text,
    status text,
    enabled boolean,
    version bigint,
    created_at timestamptz,
    updated_at timestamptz,
    expires_at timestamptz,
    disabled_at timestamptz,
    external_target_type text,
    external_target_id text
) LANGUAGE sql SECURITY DEFINER SET search_path FROM CURRENT AS $fn$
    SELECT b.tenant_id, b.channel, b.binding_id, b.external_app_id,
           b.secret_ref, b.verify_token_ref, b.status, b.enabled,
           b.version, b.created_at, b.updated_at, b.expires_at,
           b.disabled_at, b.external_target_type, b.external_target_id
    FROM channel_binding b
    WHERE b.channel = p_channel AND b.external_app_id = p_external_app_id
$fn$;

-- Transfer ownership to the dedicated NOLOGIN role so the SECURITY DEFINER
-- functions never execute with migrator/superuser identity. Non-superuser
-- schema owners must be granted membership in trpc_claim_owner before
-- running this migration (GRANT trpc_claim_owner TO <schema_owner>).
DO $mig$
BEGIN
    IF NOT (
        EXISTS (SELECT 1 FROM pg_roles WHERE rolname = current_user AND rolsuper)
        OR pg_has_role(current_user, 'trpc_claim_owner', 'member')
    ) THEN
        RAISE EXCEPTION 'current user must be superuser or a member of trpc_claim_owner to own the claim functions';
    END IF;
END
$mig$;
ALTER FUNCTION trpc_queue_claim_next(bigint, text) OWNER TO trpc_claim_owner;
ALTER FUNCTION trpc_vector_task_candidate(timestamptz) OWNER TO trpc_claim_owner;
ALTER FUNCTION trpc_binding_resolve(text, text) OWNER TO trpc_claim_owner;

-- The claim owner executes the three fixed functions under SECURITY DEFINER;
-- it needs schema usage and exactly the table privileges those functions use,
-- and nothing else. BYPASSRLS removes the RLS layer, the grants bound the
-- surface to these three tables.
DO $mig$
DECLARE
    schema_name text := current_schema();
BEGIN
    IF schema_name IS NULL OR schema_name = '' THEN
        RAISE EXCEPTION 'current_schema is empty';
    END IF;
    EXECUTE format('GRANT USAGE ON SCHEMA %I TO trpc_claim_owner', schema_name);
    EXECUTE format('GRANT SELECT, UPDATE ON TABLE %I.job_queue TO trpc_claim_owner', schema_name);
    EXECUTE format('GRANT SELECT ON TABLE %I.vector_projection_task TO trpc_claim_owner', schema_name);
    EXECUTE format('GRANT SELECT ON TABLE %I.channel_binding TO trpc_claim_owner', schema_name);
END
$mig$;

REVOKE ALL ON FUNCTION trpc_queue_claim_next(bigint, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION trpc_vector_task_candidate(timestamptz) FROM PUBLIC;
REVOKE ALL ON FUNCTION trpc_binding_resolve(text, text) FROM PUBLIC;
DO $mig$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_runtime') THEN
        GRANT EXECUTE ON FUNCTION trpc_queue_claim_next(bigint, text) TO trpc_runtime;
        GRANT EXECUTE ON FUNCTION trpc_vector_task_candidate(timestamptz) TO trpc_runtime;
        GRANT EXECUTE ON FUNCTION trpc_binding_resolve(text, text) TO trpc_runtime;
    END IF;
END
$mig$;
