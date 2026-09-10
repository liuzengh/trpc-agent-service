BEGIN;

SET LOCAL search_path = pg_catalog, public, pg_temp;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'tenant_auditor') THEN
        EXECUTE 'CREATE ROLE tenant_auditor NOLOGIN NOINHERIT';
    END IF;
END;
$$;

-- Issue #54: durable, secret-free business audit and usage facts. The table is
-- append-only for runtime identities; cleanup remains a migration-owner job.
CREATE TABLE public.audit_event (
    tenant_id            TEXT NOT NULL CHECK (length(btrim(tenant_id)) BETWEEN 1 AND 256),
    event_id             TEXT NOT NULL CHECK (length(btrim(event_id)) BETWEEN 1 AND 256),
    schema_version       INTEGER NOT NULL CHECK (schema_version = 1),
    event_type           TEXT NOT NULL CHECK (event_type IN (
        'control_plane.changed', 'execution.started', 'execution.completed',
        'execution.failed', 'execution.canceled', 'execution.timed_out',
        'execution.fallback', 'tool.allowed', 'tool.denied',
        'tool.approval_required', 'im.authorization_allowed',
        'im.authorization_denied', 'im.ingress_accepted', 'im.ingress_duplicate',
        'im.delivery_sent', 'im.delivery_retry_scheduled',
        'im.delivery_dead_lettered', 'im.delivery_reconciled', 'budget.rejected',
        'content.redacted', 'audit_incomplete'
    )),
    channel              TEXT,
    user_id              TEXT,
    session_id           TEXT,
    agent_app_id         TEXT,
    revision             BIGINT CHECK (revision IS NULL OR revision >= 0),
    model_profile_id     TEXT,
    tool_name            TEXT,
    decision              TEXT CHECK (decision IS NULL OR decision IN (
        'allow', 'deny', 'approval_required', 'accepted', 'duplicate', 'rejected'
    )),
    latency_ms            BIGINT CHECK (latency_ms IS NULL OR latency_ms >= 0),
    error_type            TEXT CHECK (error_type IS NULL OR error_type IN (
        'canceled', 'timeout', 'invalid', 'unauthenticated', 'rate_limited',
        'duplicate', 'unavailable', 'storage', 'model', 'tool', 'provider_error',
        'budget', 'redacted', 'conflict'
    )),
    input_tokens          BIGINT CHECK (input_tokens IS NULL OR input_tokens >= 0),
    output_tokens         BIGINT CHECK (output_tokens IS NULL OR output_tokens >= 0),
    model_cost_minor      BIGINT CHECK (model_cost_minor IS NULL OR model_cost_minor >= 0),
    tool_cost_minor       BIGINT CHECK (tool_cost_minor IS NULL OR tool_cost_minor >= 0),
    currency              CHAR(3),
    budget_used_tokens    BIGINT CHECK (budget_used_tokens IS NULL OR budget_used_tokens >= 0),
    budget_used_minor     BIGINT CHECK (budget_used_minor IS NULL OR budget_used_minor >= 0),
    execution_result      TEXT CHECK (execution_result IS NULL OR execution_result IN (
        'success', 'failure', 'canceled', 'timeout', 'rejected'
    )),
    provider              TEXT,
    model                 TEXT,
    request_id            TEXT,
    trace_id              TEXT,
    correlation_id        TEXT,
    actor_type            TEXT,
    actor_id              TEXT,
    reason                TEXT CHECK (reason IS NULL OR length(reason) <= 4000),
    previous_version     BIGINT,
    next_version          BIGINT,
    occurred_at           TIMESTAMPTZ NOT NULL,
    digest                CHAR(64) NOT NULL CHECK (digest ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (tenant_id, event_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id),
    CHECK ((model_cost_minor IS NULL AND tool_cost_minor IS NULL AND budget_used_minor IS NULL)
        OR (currency IS NOT NULL AND currency ~ '^[A-Z]{3}$')),
    CHECK ((previous_version IS NULL) = (next_version IS NULL)
        AND (previous_version IS NULL
             OR (previous_version >= 0 AND previous_version < 9223372036854775807
                 AND next_version = previous_version + 1)))
);

CREATE INDEX audit_event_timeline_idx
    ON public.audit_event (tenant_id, occurred_at, event_id);
CREATE INDEX audit_event_app_idx
    ON public.audit_event (tenant_id, agent_app_id, occurred_at);
CREATE INDEX audit_event_channel_idx
    ON public.audit_event (tenant_id, channel, occurred_at);
CREATE INDEX audit_event_model_profile_idx
    ON public.audit_event (tenant_id, model_profile_id, occurred_at);

-- A trusted connection establishes app.tenant_id before invoking the runtime
-- entry point. Direct table access is unavailable to runtime roles.
ALTER TABLE public.audit_event ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.audit_event FORCE ROW LEVEL SECURITY;
CREATE POLICY audit_event_tenant_scope ON public.audit_event
    USING (tenant_id = current_setting('app.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('app.tenant_id', true));

CREATE OR REPLACE FUNCTION public.audit_append_event(
    p_tenant_id TEXT, p_event_id TEXT, p_schema_version INTEGER, p_event_type TEXT,
    p_channel TEXT, p_user_id TEXT, p_session_id TEXT, p_agent_app_id TEXT,
    p_revision BIGINT, p_model_profile_id TEXT, p_tool_name TEXT, p_decision TEXT,
    p_latency_ms BIGINT, p_error_type TEXT, p_input_tokens BIGINT,
    p_output_tokens BIGINT, p_model_cost_minor BIGINT, p_tool_cost_minor BIGINT,
    p_currency TEXT, p_budget_used_tokens BIGINT, p_budget_used_minor BIGINT,
    p_execution_result TEXT, p_provider TEXT, p_model TEXT, p_request_id TEXT,
    p_trace_id TEXT, p_correlation_id TEXT, p_actor_type TEXT, p_actor_id TEXT,
    p_reason TEXT, p_previous_version BIGINT, p_next_version BIGINT,
    p_occurred_at TIMESTAMPTZ, p_digest TEXT
) RETURNS TABLE(stored_digest TEXT, duplicate BOOLEAN, conflict BOOLEAN)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, pg_temp
AS $$
DECLARE
    v_existing TEXT;
BEGIN
    IF current_setting('app.tenant_id', true) IS NULL
       OR current_setting('app.tenant_id', true) <> p_tenant_id THEN
        RAISE EXCEPTION 'audit tenant scope denied' USING ERRCODE = '42501';
    END IF;
    IF p_tenant_id <> btrim(p_tenant_id)
       OR p_event_id <> btrim(p_event_id)
       OR p_tenant_id ~ '[[:cntrl:]]'
       OR p_event_id ~ '[[:cntrl:]]'
       OR length(coalesce(p_reason, '')) > 1000
       OR concat_ws('|', p_channel, p_user_id, p_session_id, p_agent_app_id,
                    p_model_profile_id, p_tool_name, p_error_type, p_request_id,
                    p_trace_id, p_correlation_id, p_actor_type, p_actor_id,
                    p_reason, p_provider, p_model) ~ '[[:cntrl:]]'
       OR concat_ws('|', p_channel, p_user_id, p_session_id, p_agent_app_id,
                    p_model_profile_id, p_tool_name, p_error_type, p_request_id,
                    p_trace_id, p_correlation_id, p_actor_type, p_actor_id,
                    p_reason, p_provider, p_model) ~* '(://|authorization|bearer[[:space:]]|api([_-]|[[:space:]])key|token[=[:space:]]|secret([=:_[:space:]]|ref)|password[=[:space:]]|dsn[=[:space:]]|provider[[:space:]]+error)'
       OR ((p_model_cost_minor IS NOT NULL OR p_tool_cost_minor IS NOT NULL OR p_budget_used_minor IS NOT NULL)
           AND (p_currency IS NULL OR p_currency <> ALL(ARRAY['AED','AFN','ALL','AMD','ANG','AOA','ARS','AUD','AWG','AZN','BAM','BBD','BDT','BGN','BHD','BIF','BMD','BND','BOB','BOV','BRL','BSD','BTN','BWP','BYN','BZD','CAD','CDF','CHE','CHF','CHW','CLF','CLP','CNY','COP','COU','CRC','CUC','CUP','CVE','CZK','DJF','DKK','DOP','DZD','EGP','ERN','ETB','EUR','FJD','FKP','GBP','GEL','GHS','GIP','GMD','GNF','GTQ','GYD','HKD','HNL','HTG','HUF','IDR','ILS','INR','IQD','IRR','ISK','JMD','JOD','JPY','KES','KGS','KHR','KMF','KPW','KRW','KWD','KYD','KZT','LAK','LBP','LKR','LRD','LSL','LYD','MAD','MDL','MGA','MKD','MMK','MNT','MOP','MRU','MUR','MVR','MWK','MXN','MXV','MYR','MZN','NAD','NGN','NIO','NOK','NPR','NZD','OMR','PAB','PEN','PGK','PHP','PKR','PLN','PYG','QAR','RON','RSD','RUB','RWF','SAR','SBD','SCR','SDG','SEK','SGD','SHP','SLE','SLL','SOS','SRD','SSP','STN','SVC','SYP','SZL','THB','TJS','TMT','TND','TOP','TRY','TTD','TWD','TZS','UAH','UGX','USD','USN','UYI','UYU','UYW','UZS','VED','VES','VND','VUV','WST','XAF','XAG','XAU','XBA','XBB','XBC','XBD','XCD','XDR','XOF','XPD','XPF','XPT','XSU','XTS','XUA','XXX','YER','ZAR','ZMW','ZWG'])))
       OR ((p_previous_version IS NULL) <> (p_next_version IS NULL))
       OR (p_event_type = 'control_plane.changed' AND
           (nullif(p_actor_type, '') IS NULL OR nullif(p_actor_id, '') IS NULL OR
            nullif(p_reason, '') IS NULL OR nullif(p_correlation_id, '') IS NULL OR
            p_previous_version IS NULL OR p_previous_version = 9223372036854775807 OR
            p_next_version <> p_previous_version + 1)) THEN
        RAISE EXCEPTION 'audit payload rejected' USING ERRCODE = '22023';
    END IF;
    INSERT INTO public.audit_event (
        tenant_id, event_id, schema_version, event_type, channel, user_id, session_id,
        agent_app_id, revision, model_profile_id, tool_name, decision, latency_ms,
        error_type, input_tokens, output_tokens, model_cost_minor, tool_cost_minor,
        currency, budget_used_tokens, budget_used_minor, execution_result, provider,
        model, request_id, trace_id, correlation_id, actor_type, actor_id, reason,
        previous_version, next_version, occurred_at, digest
    ) VALUES (
        p_tenant_id, p_event_id, p_schema_version, p_event_type, NULLIF(p_channel, ''),
        NULLIF(p_user_id, ''), NULLIF(p_session_id, ''), NULLIF(p_agent_app_id, ''),
        p_revision, NULLIF(p_model_profile_id, ''), NULLIF(p_tool_name, ''), NULLIF(p_decision, ''),
        p_latency_ms, NULLIF(p_error_type, ''), p_input_tokens, p_output_tokens,
        p_model_cost_minor, p_tool_cost_minor, NULLIF(p_currency, ''), p_budget_used_tokens,
        p_budget_used_minor, NULLIF(p_execution_result, ''), NULLIF(p_provider, ''),
        NULLIF(p_model, ''), NULLIF(p_request_id, ''), NULLIF(p_trace_id, ''),
        NULLIF(p_correlation_id, ''), NULLIF(p_actor_type, ''), NULLIF(p_actor_id, ''),
        NULLIF(p_reason, ''), p_previous_version, p_next_version, p_occurred_at, p_digest
    ) ON CONFLICT (tenant_id, event_id) DO NOTHING;
    IF FOUND THEN
        stored_digest := p_digest; duplicate := false; conflict := false; RETURN NEXT; RETURN;
    END IF;
    SELECT a.digest INTO v_existing
      FROM public.audit_event AS a
     WHERE a.tenant_id = p_tenant_id AND a.event_id = p_event_id;
    IF v_existing = p_digest THEN
        stored_digest := v_existing; duplicate := true; conflict := false; RETURN NEXT; RETURN;
    END IF;
    stored_digest := v_existing; duplicate := false; conflict := true; RETURN NEXT;
END;
$$;

REVOKE ALL ON TABLE public.audit_event FROM PUBLIC, tenant_app_writer;
GRANT USAGE ON SCHEMA public TO tenant_auditor;
GRANT SELECT ON TABLE public.audit_event TO tenant_auditor;
REVOKE ALL ON FUNCTION public.audit_append_event(
    TEXT, TEXT, INTEGER, TEXT, TEXT, TEXT, TEXT, TEXT, BIGINT, TEXT, TEXT, TEXT,
    BIGINT, TEXT, BIGINT, BIGINT, BIGINT, BIGINT, TEXT, BIGINT, BIGINT, TEXT, TEXT,
    TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TIMESTAMPTZ, TEXT
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.audit_append_event(
    TEXT, TEXT, INTEGER, TEXT, TEXT, TEXT, TEXT, TEXT, BIGINT, TEXT, TEXT, TEXT,
    BIGINT, TEXT, BIGINT, BIGINT, BIGINT, BIGINT, TEXT, BIGINT, BIGINT, TEXT, TEXT,
    TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TIMESTAMPTZ, TEXT
) TO tenant_app_writer;
GRANT ALL PRIVILEGES ON TABLE public.audit_event TO migration_owner;
ALTER FUNCTION public.audit_append_event(
    TEXT, TEXT, INTEGER, TEXT, TEXT, TEXT, TEXT, TEXT, BIGINT, TEXT, TEXT, TEXT,
    BIGINT, TEXT, BIGINT, BIGINT, BIGINT, BIGINT, TEXT, BIGINT, BIGINT, TEXT, TEXT,
    TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TIMESTAMPTZ, TEXT
) OWNER TO migration_owner;

COMMIT;
