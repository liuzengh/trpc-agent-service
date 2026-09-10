ALTER TABLE public.agent_app
    ADD COLUMN canary_revision BIGINT;

ALTER TABLE public.agent_app
    ADD CONSTRAINT fk_agent_app_canary_revision
    FOREIGN KEY (tenant_id, app_id, canary_revision)
    REFERENCES public.agent_app_revision(tenant_id, app_id, revision);

ALTER TABLE public.agent_app_change_outbox
    DROP CONSTRAINT agent_app_change_outbox_event_type_check,
    ADD CONSTRAINT agent_app_change_outbox_event_type_check
    CHECK (event_type IN (
        'published', 'rolled_back', 'suspended', 'resumed', 'disabled',
        'canary_started', 'canary_stopped'
    ));

ALTER TABLE public.audit_event
    DROP CONSTRAINT audit_event_event_type_check,
    ADD CONSTRAINT audit_event_event_type_check
    CHECK (event_type IN (
        'control_plane.changed', 'execution.started', 'execution.completed',
        'execution.failed', 'execution.canceled', 'execution.timed_out',
        'execution.fallback', 'execution.canary_selected', 'tool.allowed',
        'tool.denied', 'tool.approval_required', 'im.authorization_allowed',
        'im.authorization_denied', 'im.ingress_accepted', 'im.ingress_duplicate',
        'im.delivery_sent', 'im.delivery_retry_scheduled',
        'im.delivery_dead_lettered', 'im.delivery_reconciled',
        'budget.rejected', 'content.redacted', 'audit_incomplete'
    ));

CREATE OR REPLACE FUNCTION public.agent_app_canary_revision_guard()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_state TEXT;
BEGIN
    IF TG_OP = 'UPDATE' AND NEW.current_revision IS DISTINCT FROM OLD.current_revision THEN
        NEW.canary_revision := NULL;
    END IF;
    IF NEW.canary_revision IS NULL THEN
        RETURN NEW;
    END IF;
    IF NEW.current_revision IS NULL OR NEW.canary_revision = NEW.current_revision THEN
        RAISE EXCEPTION 'agent app canary revision must differ from current revision';
    END IF;
    SELECT state INTO v_state FROM public.agent_app_revision
      WHERE tenant_id = NEW.tenant_id AND app_id = NEW.app_id AND revision = NEW.canary_revision;
    IF NOT FOUND OR v_state <> 'published' THEN
        RAISE EXCEPTION 'agent app canary revision must be published';
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER agent_app_canary_revision_change_guard
BEFORE UPDATE OF current_revision, canary_revision ON public.agent_app
FOR EACH ROW EXECUTE FUNCTION public.agent_app_canary_revision_guard();

CREATE CONSTRAINT TRIGGER agent_app_canary_revision_published_guard
AFTER INSERT OR UPDATE OF current_revision, canary_revision ON public.agent_app
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.agent_app_canary_revision_guard();

CREATE OR REPLACE FUNCTION public.control_plane_set_agent_app_canary(
    p_tenant_id TEXT, p_app_id TEXT, p_candidate_revision BIGINT,
    p_stable_revision BIGINT, p_expected_app_version BIGINT, p_app_version BIGINT, p_app_updated_at TIMESTAMPTZ,
    p_content_digest TEXT, p_previous_canary_revision BIGINT, p_next_canary_revision BIGINT,
    p_event_type TEXT, p_actor_type TEXT, p_actor_id TEXT, p_reason TEXT, p_correlation_id TEXT
) RETURNS BIGINT
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_current BIGINT; v_version BIGINT; v_status TEXT; v_tenant_status TEXT; v_state TEXT; v_event_id BIGINT;
BEGIN
    -- Lock rows in the same tenant-then-app order used by publication.
    SELECT status INTO v_tenant_status
      FROM public.tenant
      WHERE tenant_id = p_tenant_id
      FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist'; END IF;
    SELECT current_revision, version, status
      INTO v_current, v_version, v_status
      FROM public.agent_app
      WHERE tenant_id = p_tenant_id AND app_id = p_app_id
      FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'agent app does not exist'; END IF;
    IF v_version <> p_expected_app_version THEN RAISE EXCEPTION 'agent app version conflict'; END IF;
    IF v_tenant_status <> 'active' THEN RAISE EXCEPTION 'tenant must be active'; END IF;
    IF v_status <> 'active' THEN RAISE EXCEPTION 'canary requires an active app'; END IF;
    IF p_stable_revision IS DISTINCT FROM v_current THEN RAISE EXCEPTION 'agent app current revision mismatch'; END IF;
    IF p_previous_canary_revision IS DISTINCT FROM (SELECT canary_revision FROM public.agent_app WHERE tenant_id = p_tenant_id AND app_id = p_app_id) THEN RAISE EXCEPTION 'agent app canary revision mismatch'; END IF;
    IF p_next_canary_revision IS DISTINCT FROM p_candidate_revision THEN RAISE EXCEPTION 'agent app candidate revision mismatch'; END IF;
    IF p_candidate_revision IS NOT NULL THEN
        IF v_current IS NULL OR p_candidate_revision = v_current THEN RAISE EXCEPTION 'invalid canary revision'; END IF;
        SELECT state INTO v_state FROM public.agent_app_revision WHERE tenant_id = p_tenant_id AND app_id = p_app_id AND revision = p_candidate_revision FOR SHARE;
        IF NOT FOUND OR v_state <> 'published' THEN RAISE EXCEPTION 'canary revision must be published'; END IF;
    END IF;
    UPDATE public.agent_app SET canary_revision = p_candidate_revision, version = p_app_version, updated_at = p_app_updated_at
      WHERE tenant_id = p_tenant_id AND app_id = p_app_id AND version = p_expected_app_version;
    IF NOT FOUND THEN RAISE EXCEPTION 'agent app version conflict'; END IF;
    INSERT INTO public.agent_app_change_outbox(event_type, tenant_id, app_id, previous_status, current_status, previous_revision, current_revision, content_digest, actor_type, actor_id, reason, correlation_id, previous_version, next_version, occurred_at)
      VALUES (p_event_type, p_tenant_id, p_app_id, v_status, v_status, p_previous_canary_revision, p_next_canary_revision, NULLIF(p_content_digest,''), public.trim_control_plane_text(p_actor_type), public.trim_control_plane_text(p_actor_id), public.trim_control_plane_text(p_reason), public.trim_control_plane_text(p_correlation_id), p_expected_app_version, p_app_version, p_app_updated_at)
      RETURNING event_id INTO v_event_id;
    RETURN v_event_id;
END;
$$;

REVOKE EXECUTE ON FUNCTION public.control_plane_set_agent_app_canary(TEXT,TEXT,BIGINT,BIGINT,BIGINT,BIGINT,TIMESTAMPTZ,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.control_plane_set_agent_app_canary(TEXT,TEXT,BIGINT,BIGINT,BIGINT,BIGINT,TIMESTAMPTZ,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT) TO tenant_admin_writer;
ALTER FUNCTION public.control_plane_set_agent_app_canary(TEXT,TEXT,BIGINT,BIGINT,BIGINT,BIGINT,TIMESTAMPTZ,TEXT,BIGINT,BIGINT,TEXT,TEXT,TEXT,TEXT,TEXT) OWNER TO migration_owner;
