BEGIN;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'compliance_purger') THEN
    CREATE ROLE compliance_purger NOLOGIN;
  END IF;
END
$$;
--
-- PostgreSQL database dump
--


-- Dumped from database version 16.15 (Debian 16.15-1.pgdg13+2)
-- Dumped by pg_dump version 16.15 (Debian 16.15-1.pgdg13+2)

SET LOCAL check_function_bodies = false;

--
-- Name: compliance; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA compliance;


--
-- Name: approve_audit_purge_batch(text, text, text, text); Type: FUNCTION; Schema: compliance; Owner: -
--

CREATE FUNCTION compliance.approve_audit_purge_batch(p_tenant text, p_batch text, p_approver text, p_reason text) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'compliance'
    AS $$
BEGIN
  IF NOT pg_has_role(session_user, 'compliance_purger', 'MEMBER') THEN
    RAISE EXCEPTION 'not authorized to approve purge' USING ERRCODE = '42501';
  END IF;
  UPDATE compliance.audit_purge_batch SET
    state = 'approved',
    approved_by = p_approver,
    approved_at = clock_timestamp(),
    reason = CASE WHEN btrim(p_reason) <> '' THEN p_reason ELSE reason END,
    version = version + 1,
    updated_at = clock_timestamp()
    WHERE tenant_id = p_tenant AND batch_id = p_batch AND state = 'planned';
  IF NOT FOUND THEN
    RAISE EXCEPTION 'purge batch is not approvable' USING ERRCODE = '23514';
  END IF;
END;
$$;


--
-- Name: audit_effective_retention(text, text); Type: FUNCTION; Schema: compliance; Owner: -
--

CREATE FUNCTION compliance.audit_effective_retention(p_tenant text, p_class text) RETURNS bigint
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog'
    AS $$
  SELECT GREATEST(
    COALESCE((SELECT retention_seconds FROM compliance.audit_retention_policy
              WHERE tenant_id = p_tenant ORDER BY version DESC LIMIT 1), 0),
    COALESCE((SELECT min_retention_seconds FROM compliance.audit_retention_floor
              WHERE class = p_class), 0))
$$;


--
-- Name: audit_event_on_hold(text, timestamp with time zone); Type: FUNCTION; Schema: compliance; Owner: -
--

CREATE FUNCTION compliance.audit_event_on_hold(p_tenant text, p_at timestamp with time zone) RETURNS boolean
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog'
    AS $$
  SELECT EXISTS (
    SELECT 1 FROM compliance.audit_legal_hold h
    WHERE h.tenant_id = p_tenant AND h.event = 'placed'
      AND h.scope_start <= p_at AND p_at < h.scope_end
      AND NOT EXISTS (
        SELECT 1 FROM compliance.audit_legal_hold r
        WHERE r.tenant_id = h.tenant_id AND r.hold_id = h.hold_id AND r.event = 'released'
      )
  )
$$;


--
-- Name: audit_retention_class(text); Type: FUNCTION; Schema: compliance; Owner: -
--

CREATE FUNCTION compliance.audit_retention_class(action text) RETURNS text
    LANGUAGE sql STABLE
    SET search_path TO 'pg_catalog'
    AS $$
  SELECT COALESCE(
    (SELECT class FROM compliance.audit_retention_class_rule
     WHERE action LIKE action_prefix || '%'
     ORDER BY length(action_prefix) DESC LIMIT 1),
    'default')
$$;


--
-- Name: execute_audit_purge_batch(text, text, text); Type: FUNCTION; Schema: compliance; Owner: -
--

CREATE FUNCTION compliance.execute_audit_purge_batch(p_tenant text, p_batch text, p_owner text) RETURNS text
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'compliance'
    AS $$
DECLARE
  b compliance.audit_purge_batch%ROWTYPE;
  v_count bigint;
  v_digest text;
  v_from timestamptz;
  v_to timestamptz;
  v_alert_count bigint := 0;
  v_deleted bigint;
  v_chunk int := 1000;
  v_policy bigint;
  v_floor bigint;
  v_ids text[];
BEGIN
  IF NOT pg_has_role(session_user, 'compliance_purger', 'MEMBER') THEN
    RAISE EXCEPTION 'not authorized to execute purge' USING ERRCODE = '42501';
  END IF;

  SELECT * INTO b FROM compliance.audit_purge_batch
    WHERE tenant_id = p_tenant AND batch_id = p_batch FOR UPDATE;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'purge batch not found' USING ERRCODE = '02000';
  END IF;
  IF b.state = 'completed' THEN
    RETURN 'completed';
  END IF;
  IF b.state NOT IN ('approved','executing','failed') THEN
    RETURN 'not_executable';
  END IF;
  IF b.state = 'executing'
     AND b.claim_owner IS DISTINCT FROM p_owner
     AND (b.claim_until IS NULL OR b.claim_until >= clock_timestamp()) THEN
    RETURN 'claimed_by_another';
  END IF;
  IF b.state = 'failed' AND b.not_before IS NOT NULL AND b.not_before > clock_timestamp() THEN
    RETURN 'backoff';
  END IF;

  SELECT COALESCE((SELECT version FROM compliance.audit_retention_policy
                   WHERE tenant_id = b.tenant_id ORDER BY version DESC LIMIT 1), 0) INTO v_policy;
  SELECT floor_version FROM compliance.audit_retention_floor WHERE class = b.class INTO v_floor;
  IF v_policy IS DISTINCT FROM b.policy_version OR v_floor IS DISTINCT FROM b.floor_version
     OR b.cutoff_at > clock_timestamp() - make_interval(
          secs => compliance.audit_effective_retention(b.tenant_id, b.class)) THEN
    IF b.state = 'approved' THEN
      UPDATE compliance.audit_purge_batch SET state = 'executing',
        claim_owner = p_owner, claim_until = clock_timestamp() + interval '5 minutes',
        version = version + 1, updated_at = clock_timestamp()
        WHERE tenant_id = p_tenant AND batch_id = p_batch;
    END IF;
    UPDATE compliance.audit_purge_batch SET state = 'failed', last_error_class = 'retention_changed',
      delete_attempt = delete_attempt + 1,
      not_before = clock_timestamp() + make_interval(secs => LEAST(30 * power(2, delete_attempt), 86400)::int),
      version = version + 1, updated_at = clock_timestamp()
      WHERE tenant_id = p_tenant AND batch_id = p_batch;
    RETURN 'retention_changed';
  END IF;

  -- Advance to executing before any validation branch may set failed, keeping
  -- the state machine transitions legal (approved -> executing -> ...). The
  -- deterministic failure branches below persist their state and RETURN a
  -- result code instead of RAISE so the transition is not rolled back.
  UPDATE compliance.audit_purge_batch SET
    state = 'executing',
    claim_owner = p_owner,
    claim_until = clock_timestamp() + interval '5 minutes',
    version = version + 1,
    updated_at = clock_timestamp()
    WHERE tenant_id = p_tenant AND batch_id = p_batch;

  IF b.ttl_until IS NOT NULL AND b.ttl_until < clock_timestamp() THEN
    UPDATE compliance.audit_purge_batch SET state = 'failed', last_error_class = 'preview_expired',
      delete_attempt = delete_attempt + 1,
      not_before = clock_timestamp() + make_interval(secs => LEAST(30 * power(2, delete_attempt), 86400)::int),
      version = version + 1, updated_at = clock_timestamp()
      WHERE tenant_id = p_tenant AND batch_id = p_batch;
    RETURN 'preview_expired';
  END IF;

  -- Recompute the candidate set; any divergence from the plan is fail closed.
  -- Freeze and row-lock the exact candidate IDs before computing the digest.
  -- Delayed relay inserts with historical occurred_at are left for a later
  -- batch instead of being deleted without appearing in this certificate.
  SELECT ARRAY(
    SELECT e.audit_id FROM compliance.audit_event e
    WHERE e.tenant_id = p_tenant
      AND e.occurred_at < b.cutoff_at
      AND compliance.audit_retention_class(e.event_json->>'action') = b.class
      AND NOT compliance.audit_event_on_hold(e.tenant_id, e.occurred_at)
    ORDER BY e.occurred_at, e.audit_id
    FOR UPDATE) INTO v_ids;

  SELECT count(*), COALESCE(min(occurred_at), b.cutoff_at), COALESCE(max(occurred_at), b.cutoff_at),
         COALESCE(encode(sha256(convert_to(string_agg(e.tenant_id || ':' || e.audit_id, E'\n'
                 ORDER BY e.occurred_at, e.audit_id), 'UTF8')), 'hex'),
                  encode(sha256(convert_to('', 'UTF8')), 'hex'))
    INTO v_count, v_from, v_to, v_digest
    FROM compliance.audit_event e
    WHERE e.tenant_id = p_tenant
      AND e.audit_id = ANY(v_ids);

  IF v_count <> b.planned_count OR v_digest <> b.planned_digest THEN
    UPDATE compliance.audit_purge_batch SET state = 'failed', last_error_class = 'divergence',
      verified_digest = v_digest, delete_attempt = delete_attempt + 1,
      not_before = clock_timestamp() + make_interval(secs => LEAST(30 * power(2, delete_attempt), 86400)::int),
      version = version + 1, updated_at = clock_timestamp()
      WHERE tenant_id = p_tenant AND batch_id = p_batch;
    RETURN 'divergence';
  END IF;

  -- Refuse if any candidate backs an open quarantine alert without resolution.
  IF EXISTS (
    SELECT 1 FROM compliance.quarantine_alert q
    WHERE q.tenant_id = p_tenant
      AND EXISTS (
        SELECT 1 FROM compliance.audit_event e
        WHERE e.tenant_id = q.tenant_id AND e.audit_id = q.audit_id
          AND e.occurred_at < b.cutoff_at
          AND compliance.audit_retention_class(e.event_json->>'action') = b.class
          AND NOT compliance.audit_event_on_hold(e.tenant_id, e.occurred_at))
      AND NOT EXISTS (
        SELECT 1 FROM compliance.audit_quarantine_resolution r
        WHERE r.tenant_id = q.tenant_id AND r.audit_id = q.audit_id)
  ) THEN
    UPDATE compliance.audit_purge_batch SET state = 'failed', last_error_class = 'unresolved_quarantine',
      delete_attempt = delete_attempt + 1,
      not_before = clock_timestamp() + make_interval(secs => LEAST(30 * power(2, delete_attempt), 86400)::int),
      version = version + 1, updated_at = clock_timestamp()
      WHERE tenant_id = p_tenant AND batch_id = p_batch;
    RETURN 'unresolved_quarantine';
  END IF;

  -- Guarded deletion: children (resolution + alert) first, then the event.
  -- Every statement re-applies the hold predicate so legal-hold events keep
  -- their quarantine evidence while the parent is retained.
  PERFORM set_config('compliance.purge_authorized', 'on', true);

  DELETE FROM compliance.audit_quarantine_resolution r
    USING compliance.audit_event e
    WHERE r.tenant_id = p_tenant AND e.tenant_id = p_tenant AND r.audit_id = e.audit_id
      AND e.audit_id = ANY(v_ids)
      AND e.occurred_at < b.cutoff_at
      AND compliance.audit_retention_class(e.event_json->>'action') = b.class
      AND NOT compliance.audit_event_on_hold(e.tenant_id, e.occurred_at);

  DELETE FROM compliance.quarantine_alert q
    USING compliance.audit_event e
    WHERE q.tenant_id = p_tenant AND e.tenant_id = p_tenant AND q.audit_id = e.audit_id
      AND e.audit_id = ANY(v_ids)
      AND e.occurred_at < b.cutoff_at
      AND compliance.audit_retention_class(e.event_json->>'action') = b.class
      AND NOT compliance.audit_event_on_hold(e.tenant_id, e.occurred_at);
  GET DIAGNOSTICS v_alert_count = ROW_COUNT;

  -- Chunked event deletion keeps any single DELETE statement bounded.
  v_count := 0;
  LOOP
    DELETE FROM compliance.audit_event e
      WHERE e.tenant_id = p_tenant
        AND e.occurred_at < b.cutoff_at
        AND e.audit_id = ANY(v_ids)
        AND compliance.audit_retention_class(e.event_json->>'action') = b.class
        AND NOT compliance.audit_event_on_hold(e.tenant_id, e.occurred_at)
        AND e.ctid IN (
          SELECT e2.ctid FROM compliance.audit_event e2
          WHERE e2.tenant_id = p_tenant
            AND e2.occurred_at < b.cutoff_at
            AND e2.audit_id = ANY(v_ids)
            AND compliance.audit_retention_class(e2.event_json->>'action') = b.class
            AND NOT compliance.audit_event_on_hold(e2.tenant_id, e2.occurred_at)
          LIMIT v_chunk
        );
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    v_count := v_count + v_deleted;
    EXIT WHEN v_deleted = 0;
  END LOOP;

  INSERT INTO compliance.audit_purge_certificate(
    tenant_id,batch_id,from_occurred_at,to_occurred_at,count,alert_count,event_digest,
    policy_version,floor_version,class,approved_by,reason)
  VALUES (p_tenant, b.batch_id, v_from, v_to, v_count, v_alert_count, v_digest,
          b.policy_version, b.floor_version, b.class, COALESCE(b.approved_by, ''), b.reason)
  ON CONFLICT (tenant_id,batch_id) DO NOTHING;

  UPDATE compliance.audit_purge_batch SET
    state = 'completed', deleted_count = v_count, alert_count = v_alert_count,
    version = version + 1, updated_at = clock_timestamp()
    WHERE tenant_id = p_tenant AND batch_id = p_batch;

  RETURN 'completed';
END;
$$;


--
-- Name: guard_audit_purge_batch_update(); Type: FUNCTION; Schema: compliance; Owner: -
--

CREATE FUNCTION compliance.guard_audit_purge_batch_update() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $$
DECLARE ok boolean;
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'purge batch is immutable' USING ERRCODE = '55000';
  END IF;
  IF (NEW.tenant_id,NEW.batch_id,NEW.cutoff_at,NEW.class,NEW.created_at)
     IS DISTINCT FROM (OLD.tenant_id,OLD.batch_id,OLD.cutoff_at,OLD.class,OLD.created_at) THEN
    RAISE EXCEPTION 'purge batch identity is immutable' USING ERRCODE = '23000';
  END IF;
  IF NEW.version <> OLD.version + 1 OR NEW.updated_at < OLD.updated_at THEN
    RAISE EXCEPTION 'purge batch version must advance exactly once' USING ERRCODE = '40001';
  END IF;
  IF (NEW.planned_count,NEW.planned_digest) IS DISTINCT FROM (OLD.planned_count,OLD.planned_digest) THEN
    RAISE EXCEPTION 'purge plan is immutable' USING ERRCODE = '23000';
  END IF;
  ok := false;
  IF NEW.state = OLD.state THEN
    ok := NEW.state IN ('planned','approved','executing','failed');
  ELSIF OLD.state = 'planned' AND NEW.state = 'approved' THEN ok := true;
  ELSIF OLD.state = 'approved' AND NEW.state = 'executing' THEN ok := true;
  ELSIF OLD.state = 'executing' AND NEW.state IN ('completed','failed','quarantined') THEN ok := true;
  ELSIF OLD.state = 'failed' AND NEW.state IN ('executing','quarantined') THEN ok := true;
  END IF;
  IF NOT ok THEN
    RAISE EXCEPTION 'illegal purge batch transition' USING ERRCODE = '23514';
  END IF;
  IF NEW.state = 'executing' AND (NEW.claim_owner IS NULL OR NEW.claim_until IS NULL) THEN
    RAISE EXCEPTION 'executing purge batch requires a claim' USING ERRCODE = '23514';
  END IF;
  RETURN NEW;
END;
$$;


--
-- Name: guard_audit_retention_floor(); Type: FUNCTION; Schema: compliance; Owner: -
--

CREATE FUNCTION compliance.guard_audit_retention_floor() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'retention floor is immutable' USING ERRCODE = '55000';
  END IF;
  IF TG_OP = 'UPDATE' THEN
    IF NEW.class <> OLD.class THEN
      RAISE EXCEPTION 'retention floor class is immutable' USING ERRCODE = '23000';
    END IF;
    IF NEW.floor_version <> OLD.floor_version + 1 THEN
      RAISE EXCEPTION 'retention floor version must advance exactly once' USING ERRCODE = '40001';
    END IF;
    IF NEW.min_retention_seconds < OLD.min_retention_seconds THEN
      RAISE EXCEPTION 'retention floor may only increase' USING ERRCODE = '23514';
    END IF;
  END IF;
  RETURN NEW;
END;
$$;


--
-- Name: guard_audit_retention_policy(); Type: FUNCTION; Schema: compliance; Owner: -
--

CREATE FUNCTION compliance.guard_audit_retention_policy() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $$
BEGIN
  IF TG_OP <> 'INSERT' THEN
    RAISE EXCEPTION 'retention policy is append-only' USING ERRCODE = '55000';
  END IF;
  IF NEW.version <> 1 AND NEW.version <>
     (SELECT COALESCE(MAX(version),0)+1 FROM compliance.audit_retention_policy WHERE tenant_id = NEW.tenant_id) THEN
    RAISE EXCEPTION 'retention policy version must be contiguous' USING ERRCODE = '40001';
  END IF;
  RETURN NEW;
END;
$$;


--
-- Name: plan_audit_purge_batch(text, text, timestamp with time zone, text, text, interval, bigint); Type: FUNCTION; Schema: compliance; Owner: -
--

CREATE FUNCTION compliance.plan_audit_purge_batch(p_tenant text, p_class text, p_cutoff timestamp with time zone, p_actor text, p_reason text, p_ttl interval, p_max bigint) RETURNS text
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'compliance'
    AS $$
DECLARE
  v_batch text;
  v_count bigint;
  v_digest text;
  v_policy bigint;
  v_floor bigint;
BEGIN
  IF NOT pg_has_role(session_user, 'compliance_purger', 'MEMBER') THEN
    RAISE EXCEPTION 'not authorized to plan purge' USING ERRCODE = '42501';
  END IF;
  IF p_tenant IS NULL OR btrim(p_tenant) = '' OR p_class NOT IN ('default','security','billing')
     OR p_actor IS NULL OR btrim(p_actor) = '' OR p_ttl <= interval '0 seconds' OR p_max < 1 THEN
    RAISE EXCEPTION 'invalid purge plan input' USING ERRCODE = '22023';
  END IF;
  IF p_cutoff > clock_timestamp() - make_interval(
       secs => compliance.audit_effective_retention(p_tenant, p_class)) THEN
    RAISE EXCEPTION 'purge cutoff violates effective retention' USING ERRCODE = '23514';
  END IF;
  v_batch := 'apb_' || encode(sha256(convert_to(
    'audit-purge-v1' || chr(31) || p_tenant || chr(31) || p_class || chr(31) || (EXTRACT(EPOCH FROM p_cutoff))::bigint::text,
    'UTF8')), 'hex');
  IF EXISTS (SELECT 1 FROM compliance.audit_purge_batch WHERE tenant_id = p_tenant AND batch_id = v_batch) THEN
    RETURN v_batch;
  END IF;

  SELECT count(*),
         COALESCE(encode(sha256(convert_to(string_agg(e.tenant_id || ':' || e.audit_id, E'\n'
                 ORDER BY e.occurred_at, e.audit_id), 'UTF8')), 'hex'),
                  encode(sha256(convert_to('', 'UTF8')), 'hex'))
    INTO v_count, v_digest
    FROM compliance.audit_event e
    WHERE e.tenant_id = p_tenant
      AND e.occurred_at < p_cutoff
      AND compliance.audit_retention_class(e.event_json->>'action') = p_class
      AND NOT compliance.audit_event_on_hold(e.tenant_id, e.occurred_at);

  -- Refuse to plan a batch too large to execute in one bounded transaction;
  -- the operator narrows the cutoff window instead.
  IF v_count > p_max THEN
    RAISE EXCEPTION 'purge candidate set exceeds max batch size' USING ERRCODE = '23514';
  END IF;

  SELECT COALESCE((SELECT version FROM compliance.audit_retention_policy
                   WHERE tenant_id = p_tenant ORDER BY version DESC LIMIT 1), 0) INTO v_policy;
  SELECT floor_version FROM compliance.audit_retention_floor WHERE class = p_class INTO v_floor;

  INSERT INTO compliance.audit_purge_batch(
    tenant_id,batch_id,state,cutoff_at,class,planned_count,planned_digest,
    policy_version,floor_version,previewed_at,ttl_until,reason)
  VALUES (p_tenant, v_batch, 'planned', p_cutoff, p_class, v_count, v_digest,
          v_policy, v_floor, clock_timestamp(), clock_timestamp() + p_ttl, p_reason);
  RETURN v_batch;
END;
$$;


--
-- Name: quarantine_audit_purge_batch(text, text, text, text); Type: FUNCTION; Schema: compliance; Owner: -
--

CREATE FUNCTION compliance.quarantine_audit_purge_batch(p_tenant text, p_batch text, p_owner text, p_error text) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path TO 'pg_catalog', 'compliance'
    AS $$
BEGIN
  IF NOT pg_has_role(session_user, 'compliance_purger', 'MEMBER') THEN
    RAISE EXCEPTION 'not authorized to quarantine purge' USING ERRCODE = '42501';
  END IF;
  UPDATE compliance.audit_purge_batch SET
    state = 'quarantined', last_error_class = p_error,
    version = version + 1, updated_at = clock_timestamp()
    WHERE tenant_id = p_tenant AND batch_id = p_batch AND state IN ('executing','failed');
  IF NOT FOUND THEN
    RAISE EXCEPTION 'purge batch is not quarantinable' USING ERRCODE = '23514';
  END IF;
END;
$$;


--
-- Name: reject_any_change(); Type: FUNCTION; Schema: compliance; Owner: -
--

CREATE FUNCTION compliance.reject_any_change() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $$
BEGIN
  RAISE EXCEPTION 'compliance fact is immutable' USING ERRCODE = '55000';
END;
$$;


--
-- Name: reject_immutable_change(); Type: FUNCTION; Schema: compliance; Owner: -
--

CREATE FUNCTION compliance.reject_immutable_change() RETURNS trigger
    LANGUAGE plpgsql
    SET search_path TO 'pg_catalog'
    AS $$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    RAISE EXCEPTION 'compliance fact is immutable' USING ERRCODE = '55000';
  END IF;
  IF current_setting('compliance.purge_authorized', true) IS DISTINCT FROM 'on'
     OR NOT pg_has_role(session_user, 'compliance_purger', 'MEMBER') THEN
    RAISE EXCEPTION 'compliance fact is immutable' USING ERRCODE = '55000';
  END IF;
  RETURN OLD;
END;
$$;


SET LOCAL default_tablespace = '';

SET LOCAL default_table_access_method = heap;

--
-- Name: audit_event; Type: TABLE; Schema: compliance; Owner: -
--

CREATE TABLE compliance.audit_event (
    tenant_id text NOT NULL,
    audit_id text NOT NULL,
    schema_version integer NOT NULL,
    event_json jsonb NOT NULL,
    event_digest text NOT NULL,
    occurred_at timestamp with time zone NOT NULL,
    received_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT audit_event_audit_id_check CHECK (((length(btrim(audit_id)) >= 1) AND (length(btrim(audit_id)) <= 256))),
    CONSTRAINT audit_event_check CHECK (((event_json ->> 'tenant_id'::text) = tenant_id)),
    CONSTRAINT audit_event_check1 CHECK (((event_json ->> 'audit_id'::text) = audit_id)),
    CONSTRAINT audit_event_check2 CHECK ((((event_json ->> 'schema_version'::text))::integer = schema_version)),
    CONSTRAINT audit_event_event_digest_check CHECK ((event_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT audit_event_event_json_check CHECK ((jsonb_typeof(event_json) = 'object'::text)),
    CONSTRAINT audit_event_schema_version_check CHECK ((schema_version = 1)),
    CONSTRAINT audit_event_tenant_id_check CHECK (((length(btrim(tenant_id)) >= 1) AND (length(btrim(tenant_id)) <= 128)))
);


--
-- Name: audit_legal_hold; Type: TABLE; Schema: compliance; Owner: -
--

CREATE TABLE compliance.audit_legal_hold (
    tenant_id text NOT NULL,
    hold_id text NOT NULL,
    event text NOT NULL,
    scope_start timestamp with time zone,
    scope_end timestamp with time zone,
    actor text NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT audit_legal_hold_actor_check CHECK (((length(btrim(actor)) >= 1) AND (length(btrim(actor)) <= 128))),
    CONSTRAINT audit_legal_hold_check CHECK (((event = 'released'::text) OR ((scope_start IS NOT NULL) AND (scope_end IS NOT NULL) AND (scope_start < scope_end)))),
    CONSTRAINT audit_legal_hold_event_check CHECK ((event = ANY (ARRAY['placed'::text, 'released'::text]))),
    CONSTRAINT audit_legal_hold_hold_id_check CHECK (((length(btrim(hold_id)) >= 1) AND (length(btrim(hold_id)) <= 128))),
    CONSTRAINT audit_legal_hold_reason_check CHECK ((length(reason) <= 1024)),
    CONSTRAINT audit_legal_hold_tenant_id_check CHECK (((length(btrim(tenant_id)) >= 1) AND (length(btrim(tenant_id)) <= 128)))
);


--
-- Name: audit_purge_batch; Type: TABLE; Schema: compliance; Owner: -
--

CREATE TABLE compliance.audit_purge_batch (
    tenant_id text NOT NULL,
    batch_id text NOT NULL,
    state text NOT NULL,
    cutoff_at timestamp with time zone NOT NULL,
    class text NOT NULL,
    planned_count bigint DEFAULT 0 NOT NULL,
    planned_digest text DEFAULT ''::text NOT NULL,
    verified_digest text DEFAULT ''::text NOT NULL,
    deleted_count bigint DEFAULT 0 NOT NULL,
    alert_count bigint DEFAULT 0 NOT NULL,
    claim_owner text,
    claim_until timestamp with time zone,
    delete_attempt integer DEFAULT 0 NOT NULL,
    last_error_class text,
    not_before timestamp with time zone,
    previewed_at timestamp with time zone,
    ttl_until timestamp with time zone,
    policy_version bigint,
    floor_version bigint,
    approved_by text,
    approved_at timestamp with time zone,
    reason text DEFAULT ''::text NOT NULL,
    version bigint DEFAULT 1 NOT NULL,
    created_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT audit_purge_batch_alert_count_check CHECK ((alert_count >= 0)),
    CONSTRAINT audit_purge_batch_batch_id_check CHECK (((length(btrim(batch_id)) >= 1) AND (length(btrim(batch_id)) <= 128))),
    CONSTRAINT audit_purge_batch_class_check CHECK ((class = ANY (ARRAY['default'::text, 'security'::text, 'billing'::text]))),
    CONSTRAINT audit_purge_batch_delete_attempt_check CHECK ((delete_attempt >= 0)),
    CONSTRAINT audit_purge_batch_deleted_count_check CHECK ((deleted_count >= 0)),
    CONSTRAINT audit_purge_batch_planned_count_check CHECK ((planned_count >= 0)),
    CONSTRAINT audit_purge_batch_planned_digest_check CHECK (((planned_digest = ''::text) OR (planned_digest ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT audit_purge_batch_reason_check CHECK ((length(reason) <= 1024)),
    CONSTRAINT audit_purge_batch_state_check CHECK ((state = ANY (ARRAY['planned'::text, 'approved'::text, 'executing'::text, 'completed'::text, 'failed'::text, 'quarantined'::text]))),
    CONSTRAINT audit_purge_batch_tenant_id_check CHECK (((length(btrim(tenant_id)) >= 1) AND (length(btrim(tenant_id)) <= 128))),
    CONSTRAINT audit_purge_batch_verified_digest_check CHECK (((verified_digest = ''::text) OR (verified_digest ~ '^[0-9a-f]{64}$'::text))),
    CONSTRAINT audit_purge_batch_version_check CHECK ((version >= 1))
);


--
-- Name: audit_purge_certificate; Type: TABLE; Schema: compliance; Owner: -
--

CREATE TABLE compliance.audit_purge_certificate (
    tenant_id text NOT NULL,
    batch_id text NOT NULL,
    from_occurred_at timestamp with time zone NOT NULL,
    to_occurred_at timestamp with time zone NOT NULL,
    count bigint NOT NULL,
    alert_count bigint NOT NULL,
    event_digest text NOT NULL,
    policy_version bigint NOT NULL,
    floor_version bigint NOT NULL,
    class text NOT NULL,
    approved_by text NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    executed_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT audit_purge_certificate_alert_count_check CHECK ((alert_count >= 0)),
    CONSTRAINT audit_purge_certificate_class_check CHECK ((class = ANY (ARRAY['default'::text, 'security'::text, 'billing'::text]))),
    CONSTRAINT audit_purge_certificate_count_check CHECK ((count >= 0)),
    CONSTRAINT audit_purge_certificate_event_digest_check CHECK ((event_digest ~ '^[0-9a-f]{64}$'::text))
);


--
-- Name: audit_quarantine_resolution; Type: TABLE; Schema: compliance; Owner: -
--

CREATE TABLE compliance.audit_quarantine_resolution (
    tenant_id text NOT NULL,
    audit_id text NOT NULL,
    resolved_by text NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    resolved_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT audit_quarantine_resolution_reason_check CHECK ((length(reason) <= 1024)),
    CONSTRAINT audit_quarantine_resolution_resolved_by_check CHECK (((length(btrim(resolved_by)) >= 1) AND (length(btrim(resolved_by)) <= 128)))
);


--
-- Name: audit_query_record; Type: TABLE; Schema: compliance; Owner: -
--

CREATE TABLE compliance.audit_query_record (
    query_id text NOT NULL,
    tenant_id text NOT NULL,
    subject text NOT NULL,
    cross_tenant boolean NOT NULL,
    from_occurred_at timestamp with time zone NOT NULL,
    to_occurred_at timestamp with time zone NOT NULL,
    filter_digest text NOT NULL,
    result_count bigint NOT NULL,
    result_digest text NOT NULL,
    decision text NOT NULL,
    reason_code text DEFAULT ''::text NOT NULL,
    trace_id text DEFAULT ''::text NOT NULL,
    occurred_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT audit_query_record_decision_check CHECK ((decision = ANY (ARRAY['allowed'::text, 'denied'::text]))),
    CONSTRAINT audit_query_record_filter_digest_check CHECK ((filter_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT audit_query_record_query_id_check CHECK (((length(btrim(query_id)) >= 1) AND (length(btrim(query_id)) <= 128))),
    CONSTRAINT audit_query_record_result_count_check CHECK ((result_count >= 0)),
    CONSTRAINT audit_query_record_result_digest_check CHECK ((result_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT audit_query_record_subject_check CHECK (((length(btrim(subject)) >= 1) AND (length(btrim(subject)) <= 256))),
    CONSTRAINT audit_query_record_tenant_id_check CHECK (((length(btrim(tenant_id)) >= 1) AND (length(btrim(tenant_id)) <= 128)))
);


--
-- Name: audit_retention_class_rule; Type: TABLE; Schema: compliance; Owner: -
--

CREATE TABLE compliance.audit_retention_class_rule (
    action_prefix text NOT NULL,
    class text NOT NULL,
    CONSTRAINT audit_retention_class_rule_action_prefix_check CHECK (((length(btrim(action_prefix)) >= 1) AND (length(btrim(action_prefix)) <= 128))),
    CONSTRAINT audit_retention_class_rule_class_check CHECK ((class = ANY (ARRAY['default'::text, 'security'::text, 'billing'::text])))
);


--
-- Name: audit_retention_floor; Type: TABLE; Schema: compliance; Owner: -
--

CREATE TABLE compliance.audit_retention_floor (
    class text NOT NULL,
    min_retention_seconds bigint NOT NULL,
    floor_version bigint DEFAULT 1 NOT NULL,
    updated_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT audit_retention_floor_class_check CHECK ((class = ANY (ARRAY['default'::text, 'security'::text, 'billing'::text]))),
    CONSTRAINT audit_retention_floor_floor_version_check CHECK ((floor_version >= 1)),
    CONSTRAINT audit_retention_floor_min_retention_seconds_check CHECK ((min_retention_seconds > 0))
);


--
-- Name: audit_retention_policy; Type: TABLE; Schema: compliance; Owner: -
--

CREATE TABLE compliance.audit_retention_policy (
    tenant_id text NOT NULL,
    version bigint NOT NULL,
    retention_seconds bigint NOT NULL,
    actor text NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    effective_from timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    CONSTRAINT audit_retention_policy_actor_check CHECK (((length(btrim(actor)) >= 1) AND (length(btrim(actor)) <= 128))),
    CONSTRAINT audit_retention_policy_reason_check CHECK ((length(reason) <= 1024)),
    CONSTRAINT audit_retention_policy_retention_seconds_check CHECK ((retention_seconds > 0)),
    CONSTRAINT audit_retention_policy_tenant_id_check CHECK (((length(btrim(tenant_id)) >= 1) AND (length(btrim(tenant_id)) <= 128))),
    CONSTRAINT audit_retention_policy_version_check CHECK ((version >= 1))
);


--
-- Name: quarantine_alert; Type: TABLE; Schema: compliance; Owner: -
--

CREATE TABLE compliance.quarantine_alert (
    tenant_id text NOT NULL,
    audit_id text NOT NULL,
    resource_kind text NOT NULL,
    artifact_id text NOT NULL,
    resource_version bigint NOT NULL,
    request_id text DEFAULT ''::text NOT NULL,
    error_type text NOT NULL,
    resource_ref text NOT NULL,
    event_digest text NOT NULL,
    occurred_at timestamp with time zone NOT NULL,
    received_at timestamp with time zone DEFAULT clock_timestamp() NOT NULL,
    state text DEFAULT 'open'::text NOT NULL,
    CONSTRAINT quarantine_alert_artifact_id_check CHECK (((length(btrim(artifact_id)) >= 1) AND (length(btrim(artifact_id)) <= 256))),
    CONSTRAINT quarantine_alert_error_type_check CHECK (((length(btrim(error_type)) >= 1) AND (length(btrim(error_type)) <= 128))),
    CONSTRAINT quarantine_alert_event_digest_check CHECK ((event_digest ~ '^[0-9a-f]{64}$'::text)),
    CONSTRAINT quarantine_alert_resource_kind_check CHECK ((resource_kind = ANY (ARRAY['upload'::text, 'retention'::text]))),
    CONSTRAINT quarantine_alert_resource_ref_check CHECK (((length(btrim(resource_ref)) >= 1) AND (length(btrim(resource_ref)) <= 1024))),
    CONSTRAINT quarantine_alert_resource_version_check CHECK ((resource_version >= 1)),
    CONSTRAINT quarantine_alert_state_check CHECK ((state = 'open'::text))
);


--
-- Name: audit_event audit_event_pkey; Type: CONSTRAINT; Schema: compliance; Owner: -
--

ALTER TABLE ONLY compliance.audit_event
    ADD CONSTRAINT audit_event_pkey PRIMARY KEY (tenant_id, audit_id);


--
-- Name: audit_legal_hold audit_legal_hold_pkey; Type: CONSTRAINT; Schema: compliance; Owner: -
--

ALTER TABLE ONLY compliance.audit_legal_hold
    ADD CONSTRAINT audit_legal_hold_pkey PRIMARY KEY (tenant_id, hold_id, event);


--
-- Name: audit_purge_batch audit_purge_batch_pkey; Type: CONSTRAINT; Schema: compliance; Owner: -
--

ALTER TABLE ONLY compliance.audit_purge_batch
    ADD CONSTRAINT audit_purge_batch_pkey PRIMARY KEY (tenant_id, batch_id);


--
-- Name: audit_purge_certificate audit_purge_certificate_pkey; Type: CONSTRAINT; Schema: compliance; Owner: -
--

ALTER TABLE ONLY compliance.audit_purge_certificate
    ADD CONSTRAINT audit_purge_certificate_pkey PRIMARY KEY (tenant_id, batch_id);


--
-- Name: audit_quarantine_resolution audit_quarantine_resolution_pkey; Type: CONSTRAINT; Schema: compliance; Owner: -
--

ALTER TABLE ONLY compliance.audit_quarantine_resolution
    ADD CONSTRAINT audit_quarantine_resolution_pkey PRIMARY KEY (tenant_id, audit_id);


--
-- Name: audit_query_record audit_query_record_pkey; Type: CONSTRAINT; Schema: compliance; Owner: -
--

ALTER TABLE ONLY compliance.audit_query_record
    ADD CONSTRAINT audit_query_record_pkey PRIMARY KEY (query_id);


--
-- Name: audit_retention_class_rule audit_retention_class_rule_pkey; Type: CONSTRAINT; Schema: compliance; Owner: -
--

ALTER TABLE ONLY compliance.audit_retention_class_rule
    ADD CONSTRAINT audit_retention_class_rule_pkey PRIMARY KEY (action_prefix);


--
-- Name: audit_retention_floor audit_retention_floor_pkey; Type: CONSTRAINT; Schema: compliance; Owner: -
--

ALTER TABLE ONLY compliance.audit_retention_floor
    ADD CONSTRAINT audit_retention_floor_pkey PRIMARY KEY (class);


--
-- Name: audit_retention_policy audit_retention_policy_pkey; Type: CONSTRAINT; Schema: compliance; Owner: -
--

ALTER TABLE ONLY compliance.audit_retention_policy
    ADD CONSTRAINT audit_retention_policy_pkey PRIMARY KEY (tenant_id, version);


--
-- Name: quarantine_alert quarantine_alert_pkey; Type: CONSTRAINT; Schema: compliance; Owner: -
--

ALTER TABLE ONLY compliance.quarantine_alert
    ADD CONSTRAINT quarantine_alert_pkey PRIMARY KEY (tenant_id, audit_id);


--
-- Name: audit_purge_batch_state_idx; Type: INDEX; Schema: compliance; Owner: -
--

CREATE INDEX audit_purge_batch_state_idx ON compliance.audit_purge_batch USING btree (state, tenant_id, batch_id);


--
-- Name: compliance_audit_event_global_time_idx; Type: INDEX; Schema: compliance; Owner: -
--

CREATE INDEX compliance_audit_event_global_time_idx ON compliance.audit_event USING btree (occurred_at DESC, tenant_id DESC, audit_id DESC);


--
-- Name: compliance_audit_event_tenant_time_idx; Type: INDEX; Schema: compliance; Owner: -
--

CREATE INDEX compliance_audit_event_tenant_time_idx ON compliance.audit_event USING btree (tenant_id, occurred_at DESC, audit_id);


--
-- Name: compliance_quarantine_alert_open_idx; Type: INDEX; Schema: compliance; Owner: -
--

CREATE INDEX compliance_quarantine_alert_open_idx ON compliance.quarantine_alert USING btree (received_at, tenant_id, audit_id) WHERE (state = 'open'::text);


--
-- Name: audit_legal_hold audit_legal_hold_immutable; Type: TRIGGER; Schema: compliance; Owner: -
--

CREATE TRIGGER audit_legal_hold_immutable BEFORE DELETE OR UPDATE ON compliance.audit_legal_hold FOR EACH ROW EXECUTE FUNCTION compliance.reject_any_change();


--
-- Name: audit_purge_batch audit_purge_batch_guard; Type: TRIGGER; Schema: compliance; Owner: -
--

CREATE TRIGGER audit_purge_batch_guard BEFORE DELETE OR UPDATE ON compliance.audit_purge_batch FOR EACH ROW EXECUTE FUNCTION compliance.guard_audit_purge_batch_update();


--
-- Name: audit_purge_certificate audit_purge_certificate_immutable; Type: TRIGGER; Schema: compliance; Owner: -
--

CREATE TRIGGER audit_purge_certificate_immutable BEFORE DELETE OR UPDATE ON compliance.audit_purge_certificate FOR EACH ROW EXECUTE FUNCTION compliance.reject_any_change();


--
-- Name: audit_quarantine_resolution audit_quarantine_resolution_immutable; Type: TRIGGER; Schema: compliance; Owner: -
--

CREATE TRIGGER audit_quarantine_resolution_immutable BEFORE DELETE OR UPDATE ON compliance.audit_quarantine_resolution FOR EACH ROW EXECUTE FUNCTION compliance.reject_immutable_change();


--
-- Name: audit_query_record audit_query_record_immutable; Type: TRIGGER; Schema: compliance; Owner: -
--

CREATE TRIGGER audit_query_record_immutable BEFORE DELETE OR UPDATE ON compliance.audit_query_record FOR EACH ROW EXECUTE FUNCTION compliance.reject_any_change();


--
-- Name: audit_retention_floor audit_retention_floor_guard; Type: TRIGGER; Schema: compliance; Owner: -
--

CREATE TRIGGER audit_retention_floor_guard BEFORE DELETE OR UPDATE ON compliance.audit_retention_floor FOR EACH ROW EXECUTE FUNCTION compliance.guard_audit_retention_floor();


--
-- Name: audit_retention_policy audit_retention_policy_guard; Type: TRIGGER; Schema: compliance; Owner: -
--

CREATE TRIGGER audit_retention_policy_guard BEFORE INSERT OR DELETE OR UPDATE ON compliance.audit_retention_policy FOR EACH ROW EXECUTE FUNCTION compliance.guard_audit_retention_policy();


--
-- Name: audit_event compliance_audit_event_immutable; Type: TRIGGER; Schema: compliance; Owner: -
--

CREATE TRIGGER compliance_audit_event_immutable BEFORE DELETE OR UPDATE ON compliance.audit_event FOR EACH ROW EXECUTE FUNCTION compliance.reject_immutable_change();


--
-- Name: quarantine_alert compliance_quarantine_alert_immutable; Type: TRIGGER; Schema: compliance; Owner: -
--

CREATE TRIGGER compliance_quarantine_alert_immutable BEFORE DELETE OR UPDATE ON compliance.quarantine_alert FOR EACH ROW EXECUTE FUNCTION compliance.reject_immutable_change();


--
-- Name: audit_quarantine_resolution audit_quarantine_resolution_tenant_id_audit_id_fkey; Type: FK CONSTRAINT; Schema: compliance; Owner: -
--

ALTER TABLE ONLY compliance.audit_quarantine_resolution
    ADD CONSTRAINT audit_quarantine_resolution_tenant_id_audit_id_fkey FOREIGN KEY (tenant_id, audit_id) REFERENCES compliance.audit_event(tenant_id, audit_id);


--
-- Name: quarantine_alert quarantine_alert_tenant_id_audit_id_fkey; Type: FK CONSTRAINT; Schema: compliance; Owner: -
--

ALTER TABLE ONLY compliance.quarantine_alert
    ADD CONSTRAINT quarantine_alert_tenant_id_audit_id_fkey FOREIGN KEY (tenant_id, audit_id) REFERENCES compliance.audit_event(tenant_id, audit_id);


--
-- Name: FUNCTION approve_audit_purge_batch(p_tenant text, p_batch text, p_approver text, p_reason text); Type: ACL; Schema: compliance; Owner: -
--

REVOKE ALL ON FUNCTION compliance.approve_audit_purge_batch(p_tenant text, p_batch text, p_approver text, p_reason text) FROM PUBLIC;
GRANT ALL ON FUNCTION compliance.approve_audit_purge_batch(p_tenant text, p_batch text, p_approver text, p_reason text) TO compliance_purger;


--
-- Name: FUNCTION execute_audit_purge_batch(p_tenant text, p_batch text, p_owner text); Type: ACL; Schema: compliance; Owner: -
--

REVOKE ALL ON FUNCTION compliance.execute_audit_purge_batch(p_tenant text, p_batch text, p_owner text) FROM PUBLIC;
GRANT ALL ON FUNCTION compliance.execute_audit_purge_batch(p_tenant text, p_batch text, p_owner text) TO compliance_purger;


--
-- Name: FUNCTION plan_audit_purge_batch(p_tenant text, p_class text, p_cutoff timestamp with time zone, p_actor text, p_reason text, p_ttl interval, p_max bigint); Type: ACL; Schema: compliance; Owner: -
--

REVOKE ALL ON FUNCTION compliance.plan_audit_purge_batch(p_tenant text, p_class text, p_cutoff timestamp with time zone, p_actor text, p_reason text, p_ttl interval, p_max bigint) FROM PUBLIC;
GRANT ALL ON FUNCTION compliance.plan_audit_purge_batch(p_tenant text, p_class text, p_cutoff timestamp with time zone, p_actor text, p_reason text, p_ttl interval, p_max bigint) TO compliance_purger;


--
-- Name: FUNCTION quarantine_audit_purge_batch(p_tenant text, p_batch text, p_owner text, p_error text); Type: ACL; Schema: compliance; Owner: -
--

REVOKE ALL ON FUNCTION compliance.quarantine_audit_purge_batch(p_tenant text, p_batch text, p_owner text, p_error text) FROM PUBLIC;
GRANT ALL ON FUNCTION compliance.quarantine_audit_purge_batch(p_tenant text, p_batch text, p_owner text, p_error text) TO compliance_purger;


--
-- PostgreSQL database dump complete
--



INSERT INTO compliance.audit_retention_floor(class, min_retention_seconds) VALUES
  ('default', 15552000), ('security', 31536000), ('billing', 315360000);
INSERT INTO compliance.audit_retention_class_rule(action_prefix, class) VALUES
  ('governance.', 'security'), ('artifact.quarantine', 'security'), ('tool_confirmation', 'security'), ('usage.', 'billing');

COMMIT;
