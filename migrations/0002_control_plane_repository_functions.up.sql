BEGIN;

SET LOCAL search_path = pg_catalog, public, pg_temp;

-- Repository mutation entry points. Runtime roles receive EXECUTE below, but
-- never receive direct INSERT/UPDATE/DELETE on the control-plane tables.

CREATE OR REPLACE FUNCTION public.control_plane_create_tenant(
    p_tenant_id TEXT,
    p_tenant_key TEXT,
    p_display_name TEXT,
    p_status TEXT,
    p_rate_limit_rpm BIGINT,
    p_max_concurrent_executions BIGINT,
    p_monthly_token_budget BIGINT,
    p_monthly_spend_limit_minor BIGINT,
    p_billing_currency TEXT,
    p_audit_retention_days INT,
    p_log_masking_level TEXT,
    p_trace_sampling_rate REAL,
    p_default_agent_app_id TEXT,
    p_default_backend_profile_id TEXT,
    p_version BIGINT,
    p_created_at TIMESTAMPTZ,
    p_updated_at TIMESTAMPTZ
) RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    INSERT INTO public.tenant (
        tenant_id, tenant_key, display_name, status,
        rate_limit_rpm, max_concurrent_executions, monthly_token_budget,
        monthly_spend_limit_minor, billing_currency, audit_retention_days,
        log_masking_level, trace_sampling_rate, default_agent_app_id,
        default_backend_profile_id, version, created_at, updated_at
    ) VALUES (
        p_tenant_id, p_tenant_key, p_display_name, p_status,
        p_rate_limit_rpm, p_max_concurrent_executions, p_monthly_token_budget,
        p_monthly_spend_limit_minor, NULLIF(p_billing_currency, '')::CHAR(3),
        p_audit_retention_days, p_log_masking_level, p_trace_sampling_rate,
        p_default_agent_app_id, p_default_backend_profile_id, p_version,
        p_created_at, p_updated_at
    );
END;
$$;

CREATE OR REPLACE FUNCTION public.control_plane_create_model_profile(
    p_tenant_id TEXT,
    p_profile_id TEXT,
    p_profile_key TEXT,
    p_display_name TEXT,
    p_description TEXT,
    p_status TEXT,
    p_schema_version INT,
    p_provider TEXT,
    p_model TEXT,
    p_endpoint TEXT,
    p_options JSONB,
    p_secret_ref TEXT,
    p_generation JSONB,
    p_content_digest TEXT,
    p_version BIGINT,
    p_created_at TIMESTAMPTZ,
    p_updated_at TIMESTAMPTZ,
    p_actor_type TEXT,
    p_actor_id TEXT,
    p_reason TEXT,
    p_correlation_id TEXT
) RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_event_id BIGINT;
BEGIN
    INSERT INTO public.model_profile (
        tenant_id, profile_id, profile_key, display_name, description, status,
        schema_version, provider, model, endpoint, options, secret_ref,
        generation, content_digest, version, created_at, updated_at
    ) VALUES (
        p_tenant_id, p_profile_id, p_profile_key, p_display_name, p_description,
        p_status, p_schema_version, p_provider, p_model, p_endpoint,
        COALESCE(p_options, '{}'::JSONB), COALESCE(p_secret_ref, ''),
        COALESCE(p_generation, '{}'::JSONB), p_content_digest, p_version,
        p_created_at, p_updated_at
    );
    INSERT INTO public.model_profile_change_outbox (
        event_type, tenant_id, profile_id, previous_status, current_status,
        previous_digest, current_digest, actor_type, actor_id, reason,
        correlation_id, previous_version, next_version, occurred_at
    ) VALUES (
        'created', p_tenant_id, p_profile_id, NULL, p_status, NULL,
        p_content_digest, public.trim_control_plane_text(p_actor_type),
        public.trim_control_plane_text(p_actor_id), public.trim_control_plane_text(p_reason),
        public.trim_control_plane_text(p_correlation_id), 0, p_version, p_created_at
    ) RETURNING event_id INTO v_event_id;
    RETURN v_event_id;
END;
$$;

CREATE OR REPLACE FUNCTION public.control_plane_update_model_profile(
    p_tenant_id TEXT,
    p_profile_id TEXT,
    p_expected_version BIGINT,
    p_display_name TEXT,
    p_description TEXT,
    p_schema_version INT,
    p_provider TEXT,
    p_model TEXT,
    p_endpoint TEXT,
    p_options JSONB,
    p_secret_ref TEXT,
    p_generation JSONB,
    p_content_digest TEXT,
    p_updated_at TIMESTAMPTZ,
    p_event_type TEXT,
    p_previous_status TEXT,
    p_current_status TEXT,
    p_previous_digest TEXT,
    p_current_digest TEXT,
    p_actor_type TEXT,
    p_actor_id TEXT,
    p_reason TEXT,
    p_correlation_id TEXT
) RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_previous_status TEXT;
    v_previous_digest TEXT;
    v_previous_version BIGINT;
    v_now TIMESTAMPTZ;
    v_event_id BIGINT;
BEGIN
    SELECT status, content_digest, version, updated_at
    INTO v_previous_status, v_previous_digest, v_previous_version, v_now
    FROM public.model_profile
    WHERE tenant_id = p_tenant_id AND profile_id = p_profile_id
    FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'model profile does not exist'; END IF;
    IF v_previous_status = 'disabled' THEN RAISE EXCEPTION 'model profile is disabled'; END IF;
    IF v_previous_version <> p_expected_version THEN RAISE EXCEPTION 'model profile version conflict'; END IF;
    v_now := GREATEST(p_updated_at, v_now);
    UPDATE public.model_profile
    SET display_name = p_display_name, description = p_description,
        schema_version = p_schema_version, provider = p_provider, model = p_model,
        endpoint = p_endpoint, options = COALESCE(p_options, '{}'::JSONB),
        secret_ref = COALESCE(p_secret_ref, ''), generation = COALESCE(p_generation, '{}'::JSONB),
        content_digest = p_content_digest, version = version + 1, updated_at = v_now
    WHERE tenant_id = p_tenant_id AND profile_id = p_profile_id
      AND version = p_expected_version
    RETURNING version INTO p_expected_version;
    IF NOT FOUND THEN RAISE EXCEPTION 'model profile version conflict'; END IF;
    INSERT INTO public.model_profile_change_outbox (
        event_type, tenant_id, profile_id, previous_status, current_status,
        previous_digest, current_digest, actor_type, actor_id, reason,
        correlation_id, previous_version, next_version, occurred_at
    ) VALUES (
        p_event_type, p_tenant_id, p_profile_id, v_previous_status, v_previous_status,
        v_previous_digest, p_content_digest, public.trim_control_plane_text(p_actor_type),
        public.trim_control_plane_text(p_actor_id), public.trim_control_plane_text(p_reason),
        public.trim_control_plane_text(p_correlation_id), p_expected_version - 1,
        p_expected_version, v_now
    ) RETURNING event_id INTO v_event_id;
    RETURN v_event_id;
END;
$$;

CREATE OR REPLACE FUNCTION public.control_plane_transition_model_profile_status(
    p_tenant_id TEXT,
    p_profile_id TEXT,
    p_expected_version BIGINT,
    p_next_status TEXT,
    p_actor_type TEXT,
    p_actor_id TEXT,
    p_reason TEXT,
    p_correlation_id TEXT
) RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_previous_status TEXT;
    v_previous_digest TEXT;
    v_previous_version BIGINT;
    v_updated_at TIMESTAMPTZ;
    v_now TIMESTAMPTZ;
    v_event_id BIGINT;
BEGIN
    SELECT status, content_digest, version, updated_at
    INTO v_previous_status, v_previous_digest, v_previous_version, v_updated_at
    FROM public.model_profile
    WHERE tenant_id = p_tenant_id AND profile_id = p_profile_id
    FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'model profile does not exist'; END IF;
    IF v_previous_status = 'disabled' THEN RAISE EXCEPTION 'model profile is disabled'; END IF;
    IF v_previous_version <> p_expected_version THEN RAISE EXCEPTION 'model profile version conflict'; END IF;
    IF (v_previous_status, p_next_status) NOT IN (
        ('active', 'suspended'), ('active', 'disabled'),
        ('suspended', 'active'), ('suspended', 'disabled')
    ) THEN RAISE EXCEPTION 'invalid model profile status transition'; END IF;
    v_now := GREATEST(clock_timestamp(), v_updated_at);
    UPDATE public.model_profile
    SET status = p_next_status, version = version + 1, updated_at = v_now
    WHERE tenant_id = p_tenant_id AND profile_id = p_profile_id
      AND version = p_expected_version
    RETURNING version INTO p_expected_version;
    IF NOT FOUND THEN RAISE EXCEPTION 'model profile version conflict'; END IF;
    INSERT INTO public.model_profile_change_outbox (
        event_type, tenant_id, profile_id, previous_status, current_status,
        previous_digest, current_digest, actor_type, actor_id, reason,
        correlation_id, previous_version, next_version, occurred_at
    ) VALUES (
        CASE p_next_status WHEN 'suspended' THEN 'suspended'
             WHEN 'active' THEN 'resumed' ELSE 'disabled' END,
        p_tenant_id, p_profile_id, v_previous_status, p_next_status,
        v_previous_digest, v_previous_digest,
        public.trim_control_plane_text(p_actor_type), public.trim_control_plane_text(p_actor_id),
        public.trim_control_plane_text(p_reason), public.trim_control_plane_text(p_correlation_id),
        p_expected_version - 1, p_expected_version, v_now
    ) RETURNING event_id INTO v_event_id;
    RETURN v_event_id;
END;
$$;

CREATE OR REPLACE FUNCTION public.control_plane_create_backend_profile(
    p_tenant_id TEXT,
    p_profile_id TEXT,
    p_profile_key TEXT,
    p_display_name TEXT,
    p_description TEXT,
    p_status TEXT,
    p_schema_version INT,
    p_content_digest TEXT,
    p_version BIGINT,
    p_created_at TIMESTAMPTZ,
    p_updated_at TIMESTAMPTZ,
    p_bindings JSONB,
    p_event_type TEXT,
    p_previous_status TEXT,
    p_current_status TEXT,
    p_previous_digest TEXT,
    p_current_digest TEXT,
    p_actor_type TEXT,
    p_actor_id TEXT,
    p_reason TEXT,
    p_correlation_id TEXT
) RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_event_id BIGINT;
BEGIN
    INSERT INTO public.backend_profile (
        tenant_id, profile_id, profile_key, display_name, description, status,
        schema_version, content_digest, version, created_at, updated_at
    ) VALUES (
        p_tenant_id, p_profile_id, p_profile_key, p_display_name, p_description,
        p_status, p_schema_version, p_content_digest, p_version, p_created_at, p_updated_at
    );
    INSERT INTO public.backend_profile_binding (
        tenant_id, profile_id, capability, provider, endpoint, options, secret_ref
    )
    SELECT p_tenant_id, p_profile_id, item.capability, item.provider, COALESCE(item.endpoint, ''),
           COALESCE(item.options, '{}'::JSONB), COALESCE(item.secret_ref, '')
    FROM pg_catalog.jsonb_to_recordset(COALESCE(p_bindings, '[]'::JSONB)) AS item(
        capability TEXT, provider TEXT, endpoint TEXT, options JSONB, secret_ref TEXT
    );
    INSERT INTO public.backend_profile_change_outbox (
        event_type, tenant_id, profile_id, previous_status, current_status,
        previous_digest, current_digest, actor_type, actor_id, reason,
        correlation_id, previous_version, next_version, occurred_at
    ) VALUES (
        p_event_type, p_tenant_id, p_profile_id, NULL, p_status, NULL,
        p_content_digest, public.trim_control_plane_text(p_actor_type),
        public.trim_control_plane_text(p_actor_id), public.trim_control_plane_text(p_reason),
        public.trim_control_plane_text(p_correlation_id), 0, p_version, p_created_at
    ) RETURNING event_id INTO v_event_id;
    RETURN v_event_id;
END;
$$;

CREATE OR REPLACE FUNCTION public.control_plane_update_backend_profile(
    p_tenant_id TEXT,
    p_profile_id TEXT,
    p_expected_version BIGINT,
    p_display_name TEXT,
    p_description TEXT,
    p_schema_version INT,
    p_content_digest TEXT,
    p_updated_at TIMESTAMPTZ,
    p_bindings JSONB,
    p_event_type TEXT,
    p_previous_status TEXT,
    p_current_status TEXT,
    p_previous_digest TEXT,
    p_current_digest TEXT,
    p_actor_type TEXT,
    p_actor_id TEXT,
    p_reason TEXT,
    p_correlation_id TEXT
) RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_previous_status TEXT;
    v_previous_digest TEXT;
    v_previous_version BIGINT;
    v_previous_updated_at TIMESTAMPTZ;
    v_now TIMESTAMPTZ;
    v_event_id BIGINT;
BEGIN
    SELECT status, content_digest, version, updated_at
    INTO v_previous_status, v_previous_digest, v_previous_version, v_previous_updated_at
    FROM public.backend_profile
    WHERE tenant_id = p_tenant_id AND profile_id = p_profile_id
    FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'backend profile does not exist'; END IF;
    IF v_previous_status = 'disabled' THEN RAISE EXCEPTION 'backend profile is disabled'; END IF;
    IF v_previous_version <> p_expected_version THEN RAISE EXCEPTION 'backend profile version conflict'; END IF;
    v_now := GREATEST(p_updated_at, v_previous_updated_at);
    UPDATE public.backend_profile
    SET display_name = p_display_name, description = p_description,
        schema_version = p_schema_version, content_digest = p_content_digest,
        version = version + 1, updated_at = v_now
    WHERE tenant_id = p_tenant_id AND profile_id = p_profile_id
      AND version = p_expected_version;
    IF NOT FOUND THEN RAISE EXCEPTION 'backend profile version conflict'; END IF;
    DELETE FROM public.backend_profile_binding
    WHERE tenant_id = p_tenant_id AND profile_id = p_profile_id;
    INSERT INTO public.backend_profile_binding (
        tenant_id, profile_id, capability, provider, endpoint, options, secret_ref
    )
    SELECT p_tenant_id, p_profile_id, item.capability, item.provider, COALESCE(item.endpoint, ''),
           COALESCE(item.options, '{}'::JSONB), COALESCE(item.secret_ref, '')
    FROM pg_catalog.jsonb_to_recordset(COALESCE(p_bindings, '[]'::JSONB)) AS item(
        capability TEXT, provider TEXT, endpoint TEXT, options JSONB, secret_ref TEXT
    );
    INSERT INTO public.backend_profile_change_outbox (
        event_type, tenant_id, profile_id, previous_status, current_status,
        previous_digest, current_digest, actor_type, actor_id, reason,
        correlation_id, previous_version, next_version, occurred_at
    ) VALUES (
        p_event_type, p_tenant_id, p_profile_id, v_previous_status, v_previous_status,
        v_previous_digest, p_content_digest, public.trim_control_plane_text(p_actor_type),
        public.trim_control_plane_text(p_actor_id), public.trim_control_plane_text(p_reason),
        public.trim_control_plane_text(p_correlation_id), v_previous_version,
        v_previous_version + 1, v_now
    ) RETURNING event_id INTO v_event_id;
    RETURN v_event_id;
END;
$$;

CREATE OR REPLACE FUNCTION public.control_plane_create_agent_app(
    p_tenant_id TEXT,
    p_app_id TEXT,
    p_app_key TEXT,
    p_display_name TEXT,
    p_description TEXT,
    p_status TEXT,
    p_version BIGINT,
    p_created_at TIMESTAMPTZ,
    p_updated_at TIMESTAMPTZ
) RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    INSERT INTO public.agent_app (
        tenant_id, app_id, app_key, display_name, description, status,
        current_revision, version, created_at, updated_at
    ) VALUES (
        p_tenant_id, p_app_id, p_app_key, p_display_name, p_description,
        p_status, NULL, p_version, p_created_at, p_updated_at
    );
END;
$$;

CREATE OR REPLACE FUNCTION public.control_plane_update_agent_metadata(
    p_tenant_id TEXT,
    p_app_id TEXT,
    p_expected_version BIGINT,
    p_display_name TEXT,
    p_description TEXT,
    p_updated_at TIMESTAMPTZ
) RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_status TEXT;
    v_updated_at TIMESTAMPTZ;
BEGIN
    SELECT status, updated_at INTO v_status, v_updated_at
    FROM public.agent_app
    WHERE tenant_id = p_tenant_id AND app_id = p_app_id
    FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'agent app does not exist'; END IF;
    IF v_status = 'disabled' THEN RAISE EXCEPTION 'agent app is disabled'; END IF;
    UPDATE public.agent_app
    SET display_name = p_display_name, description = p_description,
        version = version + 1, updated_at = GREATEST(p_updated_at, v_updated_at)
    WHERE tenant_id = p_tenant_id AND app_id = p_app_id
      AND version = p_expected_version;
    IF NOT FOUND THEN RAISE EXCEPTION 'agent app version conflict'; END IF;
END;
$$;

CREATE OR REPLACE FUNCTION public.control_plane_create_agent_revision(
    p_tenant_id TEXT,
    p_app_id TEXT,
    p_revision BIGINT,
    p_draft_version BIGINT,
    p_agent_kind TEXT,
    p_schema_version INT,
    p_description TEXT,
    p_instruction TEXT,
    p_global_instruction TEXT,
    p_model_profile_id TEXT,
    p_generation_config JSONB,
    p_runtime_policy JSONB,
    p_tools JSONB,
    p_created_at TIMESTAMPTZ,
    p_updated_at TIMESTAMPTZ,
    p_state TEXT
) RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
BEGIN
    INSERT INTO public.agent_app_revision (
        tenant_id, app_id, revision, state, draft_version, agent_kind,
        schema_version, description, instruction, global_instruction,
        model_profile_id, generation_config, runtime_policy, content_digest,
        published_at, created_at, updated_at
    ) VALUES (
        p_tenant_id, p_app_id, p_revision, p_state, p_draft_version, p_agent_kind,
        p_schema_version, p_description, p_instruction, p_global_instruction,
        p_model_profile_id, COALESCE(p_generation_config, '{}'::JSONB),
        COALESCE(p_runtime_policy, '{}'::JSONB), NULL, NULL, p_created_at, p_updated_at
    );
    INSERT INTO public.agent_app_revision_tool (tenant_id, app_id, revision, tool_id, required)
    SELECT p_tenant_id, p_app_id, p_revision, item.tool_id, item.required
    FROM pg_catalog.jsonb_to_recordset(COALESCE(p_tools, '[]'::JSONB)) AS item(
        tool_id TEXT, required BOOLEAN
    );
END;
$$;

CREATE OR REPLACE FUNCTION public.control_plane_update_agent_draft(
    p_tenant_id TEXT,
    p_app_id TEXT,
    p_revision BIGINT,
    p_expected_draft_version BIGINT,
    p_draft_version BIGINT,
    p_description TEXT,
    p_instruction TEXT,
    p_global_instruction TEXT,
    p_model_profile_id TEXT,
    p_generation_config JSONB,
    p_runtime_policy JSONB,
    p_tools JSONB,
    p_updated_at TIMESTAMPTZ,
    p_state TEXT,
    p_agent_kind TEXT,
    p_schema_version INT
) RETURNS VOID
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_state TEXT;
    v_created_at TIMESTAMPTZ;
    v_current_updated_at TIMESTAMPTZ;
BEGIN
    SELECT state, created_at, updated_at
    INTO v_state, v_created_at, v_current_updated_at
    FROM public.agent_app_revision
    WHERE tenant_id = p_tenant_id AND app_id = p_app_id AND revision = p_revision
    FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'agent app revision does not exist'; END IF;
    IF v_state <> 'draft' THEN RAISE EXCEPTION 'published agent app revision is immutable'; END IF;
    IF p_expected_draft_version <> (SELECT draft_version FROM public.agent_app_revision
                                    WHERE tenant_id = p_tenant_id AND app_id = p_app_id AND revision = p_revision) THEN
        RAISE EXCEPTION 'agent app draft version conflict';
    END IF;
    UPDATE public.agent_app_revision
    SET draft_version = p_draft_version, agent_kind = p_agent_kind,
        schema_version = p_schema_version, description = p_description,
        instruction = p_instruction, global_instruction = p_global_instruction,
        model_profile_id = p_model_profile_id,
        generation_config = COALESCE(p_generation_config, '{}'::JSONB),
        runtime_policy = COALESCE(p_runtime_policy, '{}'::JSONB),
        content_digest = NULL, published_at = NULL,
        updated_at = GREATEST(p_updated_at, v_current_updated_at)
    WHERE tenant_id = p_tenant_id AND app_id = p_app_id AND revision = p_revision
      AND state = 'draft' AND draft_version = p_expected_draft_version;
    IF NOT FOUND THEN RAISE EXCEPTION 'agent app draft version conflict'; END IF;
    DELETE FROM public.agent_app_revision_tool
    WHERE tenant_id = p_tenant_id AND app_id = p_app_id AND revision = p_revision;
    INSERT INTO public.agent_app_revision_tool (tenant_id, app_id, revision, tool_id, required)
    SELECT p_tenant_id, p_app_id, p_revision, item.tool_id, item.required
    FROM pg_catalog.jsonb_to_recordset(COALESCE(p_tools, '[]'::JSONB)) AS item(
        tool_id TEXT, required BOOLEAN
    );
END;
$$;

CREATE OR REPLACE FUNCTION public.control_plane_publish_agent_app(
    p_tenant_id TEXT,
    p_app_id TEXT,
    p_revision BIGINT,
    p_expected_app_version BIGINT,
    p_expected_draft_version BIGINT,
    p_content_digest TEXT,
    p_published_at TIMESTAMPTZ,
    p_revision_updated_at TIMESTAMPTZ,
    p_app_status TEXT,
    p_current_revision BIGINT,
    p_app_version BIGINT,
    p_app_updated_at TIMESTAMPTZ,
    p_previous_status TEXT,
    p_current_status TEXT,
    p_previous_revision BIGINT,
    p_event_current_revision BIGINT,
    p_actor_type TEXT,
    p_actor_id TEXT,
    p_reason TEXT,
    p_correlation_id TEXT
) RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_tenant_status TEXT;
    v_app_status TEXT;
    v_app_version BIGINT;
    v_revision_state TEXT;
    v_draft_version BIGINT;
    v_event_id BIGINT;
BEGIN
    SELECT status INTO v_tenant_status FROM public.tenant
    WHERE tenant_id = p_tenant_id FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'tenant does not exist'; END IF;
    IF v_tenant_status <> 'active' THEN RAISE EXCEPTION 'tenant must be active'; END IF;
    SELECT status, version INTO v_app_status, v_app_version
    FROM public.agent_app
    WHERE tenant_id = p_tenant_id AND app_id = p_app_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'agent app does not exist'; END IF;
    IF v_app_status = 'disabled' THEN RAISE EXCEPTION 'agent app is disabled'; END IF;
    IF v_app_version <> p_expected_app_version THEN RAISE EXCEPTION 'agent app version conflict'; END IF;
    SELECT state, draft_version INTO v_revision_state, v_draft_version
    FROM public.agent_app_revision
    WHERE tenant_id = p_tenant_id AND app_id = p_app_id AND revision = p_revision
    FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'agent app revision does not exist'; END IF;
    IF v_revision_state <> 'draft' THEN RAISE EXCEPTION 'published agent app revision is immutable'; END IF;
    IF v_draft_version <> p_expected_draft_version THEN RAISE EXCEPTION 'agent app draft version conflict'; END IF;
    IF p_current_revision IS NULL OR p_current_revision <> p_revision
       OR p_event_current_revision IS NULL OR p_event_current_revision <> p_revision THEN
        RAISE EXCEPTION 'published agent app current revision must match published revision';
    END IF;
    UPDATE public.agent_app_revision
    SET state = 'published', content_digest = p_content_digest,
        published_at = p_published_at, updated_at = p_revision_updated_at
    WHERE tenant_id = p_tenant_id AND app_id = p_app_id AND revision = p_revision
      AND state = 'draft' AND draft_version = p_expected_draft_version;
    IF NOT FOUND THEN RAISE EXCEPTION 'agent app revision publication conflict'; END IF;
    UPDATE public.agent_app
    SET status = p_app_status, current_revision = p_current_revision,
        version = p_app_version, updated_at = p_app_updated_at
    WHERE tenant_id = p_tenant_id AND app_id = p_app_id
      AND version = p_expected_app_version;
    IF NOT FOUND THEN RAISE EXCEPTION 'agent app version conflict'; END IF;
    INSERT INTO public.agent_app_change_outbox (
        event_type, tenant_id, app_id, previous_status, current_status,
        previous_revision, current_revision, content_digest, actor_type, actor_id,
        reason, correlation_id, previous_version, next_version, occurred_at
    ) VALUES (
        'published', p_tenant_id, p_app_id, p_previous_status, p_current_status,
        p_previous_revision, p_event_current_revision, p_content_digest,
        public.trim_control_plane_text(p_actor_type), public.trim_control_plane_text(p_actor_id),
        public.trim_control_plane_text(p_reason), public.trim_control_plane_text(p_correlation_id),
        p_expected_app_version, p_app_version, p_app_updated_at
    ) RETURNING event_id INTO v_event_id;
    RETURN v_event_id;
END;
$$;

CREATE OR REPLACE FUNCTION public.control_plane_rollback_agent_app(
    p_tenant_id TEXT,
    p_app_id TEXT,
    p_target_revision BIGINT,
    p_expected_app_version BIGINT,
    p_current_revision BIGINT,
    p_app_version BIGINT,
    p_app_updated_at TIMESTAMPTZ,
    p_content_digest TEXT,
    p_previous_revision BIGINT,
    p_event_current_revision BIGINT,
    p_previous_status TEXT,
    p_current_status TEXT,
    p_actor_type TEXT,
    p_actor_id TEXT,
    p_reason TEXT,
    p_correlation_id TEXT
) RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_status TEXT;
    v_current_revision BIGINT;
    v_version BIGINT;
    v_target_state TEXT;
    v_event_id BIGINT;
BEGIN
    SELECT status, current_revision, version INTO v_status, v_current_revision, v_version
    FROM public.agent_app
    WHERE tenant_id = p_tenant_id AND app_id = p_app_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'agent app does not exist'; END IF;
    IF v_status = 'disabled' THEN RAISE EXCEPTION 'agent app is disabled'; END IF;
    IF v_version <> p_expected_app_version THEN RAISE EXCEPTION 'agent app version conflict'; END IF;
    SELECT state INTO v_target_state
    FROM public.agent_app_revision
    WHERE tenant_id = p_tenant_id AND app_id = p_app_id AND revision = p_target_revision
    FOR SHARE;
    IF NOT FOUND THEN RAISE EXCEPTION 'agent app revision does not exist'; END IF;
    IF v_target_state <> 'published' THEN RAISE EXCEPTION 'rollback target must be published'; END IF;
    IF v_current_revision IS NULL OR p_previous_revision IS NULL
       OR p_previous_revision <> v_current_revision OR p_previous_revision = p_target_revision THEN
        RAISE EXCEPTION 'rollback must change the current revision';
    END IF;
    IF p_current_revision IS NULL OR p_current_revision <> p_target_revision
       OR p_event_current_revision IS NULL OR p_event_current_revision <> p_target_revision THEN
        RAISE EXCEPTION 'rollback current revision must match rollback target';
    END IF;
    UPDATE public.agent_app
    SET current_revision = p_target_revision, version = p_app_version,
        updated_at = p_app_updated_at
    WHERE tenant_id = p_tenant_id AND app_id = p_app_id
      AND version = p_expected_app_version;
    IF NOT FOUND THEN RAISE EXCEPTION 'agent app version conflict'; END IF;
    INSERT INTO public.agent_app_change_outbox (
        event_type, tenant_id, app_id, previous_status, current_status,
        previous_revision, current_revision, content_digest, actor_type, actor_id,
        reason, correlation_id, previous_version, next_version, occurred_at
    ) VALUES (
        'rolled_back', p_tenant_id, p_app_id, p_previous_status, p_current_status,
        p_previous_revision, p_target_revision, p_content_digest,
        public.trim_control_plane_text(p_actor_type), public.trim_control_plane_text(p_actor_id),
        public.trim_control_plane_text(p_reason), public.trim_control_plane_text(p_correlation_id),
        p_expected_app_version, p_app_version, p_app_updated_at
    ) RETURNING event_id INTO v_event_id;
    RETURN v_event_id;
END;
$$;

CREATE OR REPLACE FUNCTION public.control_plane_transition_agent_app_status(
    p_tenant_id TEXT,
    p_app_id TEXT,
    p_expected_version BIGINT,
    p_next_status TEXT,
    p_updated_at TIMESTAMPTZ,
    p_previous_revision BIGINT,
    p_current_revision BIGINT,
    p_content_digest TEXT,
    p_actor_type TEXT,
    p_actor_id TEXT,
    p_reason TEXT,
    p_correlation_id TEXT
) RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_status TEXT;
    v_version BIGINT;
    v_previous_updated_at TIMESTAMPTZ;
    v_now TIMESTAMPTZ;
    v_event_id BIGINT;
BEGIN
    SELECT status, version, updated_at INTO v_status, v_version, v_previous_updated_at
    FROM public.agent_app
    WHERE tenant_id = p_tenant_id AND app_id = p_app_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'agent app does not exist'; END IF;
    IF v_status = 'disabled' THEN RAISE EXCEPTION 'agent app is disabled'; END IF;
    IF v_version <> p_expected_version THEN RAISE EXCEPTION 'agent app version conflict'; END IF;
    IF (v_status, p_next_status) NOT IN (
        ('draft', 'disabled'), ('active', 'suspended'), ('active', 'disabled'),
        ('suspended', 'active'), ('suspended', 'disabled')
    ) THEN RAISE EXCEPTION 'invalid agent app status transition'; END IF;
    v_now := GREATEST(p_updated_at, v_previous_updated_at);
    UPDATE public.agent_app
    SET status = p_next_status, version = version + 1, updated_at = v_now
    WHERE tenant_id = p_tenant_id AND app_id = p_app_id
      AND version = p_expected_version;
    IF NOT FOUND THEN RAISE EXCEPTION 'agent app version conflict'; END IF;
    INSERT INTO public.agent_app_change_outbox (
        event_type, tenant_id, app_id, previous_status, current_status,
        previous_revision, current_revision, content_digest, actor_type, actor_id,
        reason, correlation_id, previous_version, next_version, occurred_at
    ) VALUES (
        CASE p_next_status WHEN 'suspended' THEN 'suspended'
             WHEN 'active' THEN 'resumed' ELSE 'disabled' END,
        p_tenant_id, p_app_id, v_status, p_next_status, p_previous_revision,
        p_current_revision, NULLIF(p_content_digest, ''),
        public.trim_control_plane_text(p_actor_type), public.trim_control_plane_text(p_actor_id),
        public.trim_control_plane_text(p_reason), public.trim_control_plane_text(p_correlation_id),
        p_expected_version, p_expected_version + 1, v_now
    ) RETURNING event_id INTO v_event_id;
    RETURN v_event_id;
END;
$$;

CREATE OR REPLACE FUNCTION public.control_plane_create_channel_binding(
    p_tenant_id TEXT,
    p_binding_id TEXT,
    p_binding_key TEXT,
    p_channel TEXT,
    p_provider_account_id TEXT,
    p_public_route_key_digest TEXT,
    p_app_id TEXT,
    p_secret_ref TEXT,
    p_protocol_config JSONB,
    p_schema_version INT,
    p_status TEXT,
    p_version BIGINT,
    p_config_digest TEXT,
    p_created_at TIMESTAMPTZ,
    p_updated_at TIMESTAMPTZ,
    p_event_type TEXT,
    p_previous_status TEXT,
    p_current_status TEXT,
    p_previous_digest TEXT,
    p_current_digest TEXT,
    p_actor_type TEXT,
    p_actor_id TEXT,
    p_reason TEXT,
    p_correlation_id TEXT
) RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_event_id BIGINT;
BEGIN
    INSERT INTO public.channel_binding (
        tenant_id, binding_id, binding_key, channel, provider_account_id,
        public_route_key_digest, app_id, secret_ref, protocol_config,
        schema_version, status, version, config_digest, created_at, updated_at
    ) VALUES (
        p_tenant_id, p_binding_id, p_binding_key, p_channel, p_provider_account_id,
        p_public_route_key_digest, p_app_id, p_secret_ref,
        COALESCE(p_protocol_config, '{}'::JSONB), p_schema_version, p_status,
        p_version, p_config_digest, p_created_at, p_updated_at
    );
    INSERT INTO public.channel_binding_change_outbox (
        event_type, tenant_id, binding_id, previous_status, current_status,
        previous_digest, current_digest, actor_type, actor_id, reason,
        correlation_id, previous_version, next_version, occurred_at
    ) VALUES (
        p_event_type, p_tenant_id, p_binding_id, NULLIF(p_previous_status, ''),
        p_current_status, NULLIF(p_previous_digest, ''), p_current_digest,
        public.trim_control_plane_text(p_actor_type), public.trim_control_plane_text(p_actor_id),
        public.trim_control_plane_text(p_reason), public.trim_control_plane_text(p_correlation_id),
        0, p_version, p_created_at
    ) RETURNING event_id INTO v_event_id;
    RETURN v_event_id;
END;
$$;

CREATE OR REPLACE FUNCTION public.control_plane_update_channel_binding(
    p_tenant_id TEXT,
    p_binding_id TEXT,
    p_expected_version BIGINT,
    p_provider_account_id TEXT,
    p_public_route_key_digest TEXT,
    p_app_id TEXT,
    p_secret_ref TEXT,
    p_protocol_config JSONB,
    p_schema_version INT,
    p_config_digest TEXT,
    p_updated_at TIMESTAMPTZ,
    p_event_type TEXT,
    p_previous_status TEXT,
    p_current_status TEXT,
    p_previous_digest TEXT,
    p_current_digest TEXT,
    p_actor_type TEXT,
    p_actor_id TEXT,
    p_reason TEXT,
    p_correlation_id TEXT,
    p_channel TEXT
) RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_status TEXT;
    v_previous_digest TEXT;
    v_previous_version BIGINT;
    v_previous_updated_at TIMESTAMPTZ;
    v_now TIMESTAMPTZ;
    v_event_id BIGINT;
BEGIN
    SELECT status, config_digest, version, updated_at INTO v_status, v_previous_digest,
           v_previous_version, v_previous_updated_at
    FROM public.channel_binding
    WHERE tenant_id = p_tenant_id AND binding_id = p_binding_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'channel binding does not exist'; END IF;
    IF v_status = 'disabled' THEN RAISE EXCEPTION 'channel binding is disabled'; END IF;
    IF v_previous_version <> p_expected_version THEN RAISE EXCEPTION 'channel binding version conflict'; END IF;
    v_now := GREATEST(p_updated_at, v_previous_updated_at);
    UPDATE public.channel_binding
    SET provider_account_id = p_provider_account_id,
        public_route_key_digest = p_public_route_key_digest, app_id = p_app_id,
        secret_ref = p_secret_ref, protocol_config = COALESCE(p_protocol_config, '{}'::JSONB),
        schema_version = p_schema_version, config_digest = p_config_digest,
        version = version + 1, updated_at = v_now
    WHERE tenant_id = p_tenant_id AND binding_id = p_binding_id
      AND channel = p_channel AND version = p_expected_version;
    IF NOT FOUND THEN RAISE EXCEPTION 'channel binding version conflict'; END IF;
    INSERT INTO public.channel_binding_change_outbox (
        event_type, tenant_id, binding_id, previous_status, current_status,
        previous_digest, current_digest, actor_type, actor_id, reason,
        correlation_id, previous_version, next_version, occurred_at
    ) VALUES (
        p_event_type, p_tenant_id, p_binding_id, v_status, v_status,
        v_previous_digest, p_config_digest, public.trim_control_plane_text(p_actor_type),
        public.trim_control_plane_text(p_actor_id), public.trim_control_plane_text(p_reason),
        public.trim_control_plane_text(p_correlation_id), v_previous_version,
        v_previous_version + 1, v_now
    ) RETURNING event_id INTO v_event_id;
    RETURN v_event_id;
END;
$$;

CREATE OR REPLACE FUNCTION public.control_plane_transition_channel_binding_status(
    p_tenant_id TEXT,
    p_binding_id TEXT,
    p_expected_version BIGINT,
    p_next_status TEXT,
    p_actor_type TEXT,
    p_actor_id TEXT,
    p_reason TEXT,
    p_correlation_id TEXT
) RETURNS BIGINT
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_status TEXT;
    v_digest TEXT;
    v_version BIGINT;
    v_updated_at TIMESTAMPTZ;
    v_now TIMESTAMPTZ;
    v_event_id BIGINT;
BEGIN
    SELECT status, config_digest, version, updated_at INTO v_status, v_digest,
           v_version, v_updated_at
    FROM public.channel_binding
    WHERE tenant_id = p_tenant_id AND binding_id = p_binding_id FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'channel binding does not exist'; END IF;
    IF v_status = 'disabled' THEN RAISE EXCEPTION 'channel binding is disabled'; END IF;
    IF v_version <> p_expected_version THEN RAISE EXCEPTION 'channel binding version conflict'; END IF;
    IF (v_status, p_next_status) NOT IN (
        ('draft', 'active'), ('draft', 'disabled'),
        ('active', 'suspended'), ('active', 'disabled'),
        ('suspended', 'active'), ('suspended', 'disabled')
    ) THEN RAISE EXCEPTION 'invalid channel binding status transition'; END IF;
    v_now := GREATEST(clock_timestamp(), v_updated_at);
    UPDATE public.channel_binding
    SET status = p_next_status, version = version + 1, updated_at = v_now
    WHERE tenant_id = p_tenant_id AND binding_id = p_binding_id
      AND version = p_expected_version;
    IF NOT FOUND THEN RAISE EXCEPTION 'channel binding version conflict'; END IF;
    INSERT INTO public.channel_binding_change_outbox (
        event_type, tenant_id, binding_id, previous_status, current_status,
        previous_digest, current_digest, actor_type, actor_id, reason,
        correlation_id, previous_version, next_version, occurred_at
    ) VALUES (
        CASE WHEN p_next_status = 'active' AND v_status = 'draft' THEN 'activated'
             WHEN p_next_status = 'active' THEN 'resumed'
             WHEN p_next_status = 'suspended' THEN 'suspended'
             ELSE 'disabled' END,
        p_tenant_id, p_binding_id, v_status, p_next_status, v_digest, v_digest,
        public.trim_control_plane_text(p_actor_type), public.trim_control_plane_text(p_actor_id),
        public.trim_control_plane_text(p_reason), public.trim_control_plane_text(p_correlation_id),
        p_expected_version, p_expected_version + 1, v_now
    ) RETURNING event_id INTO v_event_id;
    RETURN v_event_id;
END;
$$;

GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO migration_owner;

DO $$
DECLARE
    function_oid OID;
    function_name TEXT;
BEGIN
    FOR function_oid, function_name IN
        SELECT p.oid, p.oid::REGPROCEDURE::TEXT
        FROM pg_catalog.pg_proc AS p
        JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public' AND p.proname LIKE 'control_plane_%'
    LOOP
        EXECUTE pg_catalog.format('REVOKE ALL ON FUNCTION %s FROM PUBLIC', function_name);
        EXECUTE pg_catalog.format('GRANT EXECUTE ON FUNCTION %s TO tenant_admin_writer', function_name);
        EXECUTE pg_catalog.format('ALTER FUNCTION %s OWNER TO migration_owner', function_name);
    END LOOP;
END;
$$;

COMMIT;
