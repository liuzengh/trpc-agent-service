-- Durable execution audit handoff. It is mutable only through reserve/finalize
-- functions; the projected audit_event remains append-only.
CREATE TABLE public.execution_audit_handoff (
    tenant_id TEXT NOT NULL REFERENCES public.tenant(tenant_id),
    handoff_id TEXT NOT NULL CHECK (length(btrim(handoff_id)) BETWEEN 1 AND 256),
    request_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending','finalized','repairable')),
    result TEXT,
    error_type TEXT,
    latency_ms BIGINT CHECK (latency_ms IS NULL OR latency_ms >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, handoff_id)
);
ALTER TABLE public.execution_audit_handoff ENABLE ROW LEVEL SECURITY;
CREATE POLICY execution_audit_handoff_scope ON public.execution_audit_handoff
USING (current_setting('app.tenant_id', true) = tenant_id)
WITH CHECK (current_setting('app.tenant_id', true) = tenant_id);
CREATE INDEX execution_audit_handoff_state_idx ON public.execution_audit_handoff (tenant_id, state, updated_at);

-- The SECURITY DEFINER boundary is the only mutable SQL surface. Callers still
-- inherit tenant scope through the explicit tenant_id argument and stable
-- request/handoff idempotency fence.
CREATE OR REPLACE FUNCTION public.reserve_execution_audit_handoff(
    p_tenant_id TEXT, p_handoff_id TEXT, p_request_id TEXT, p_trace_id TEXT,
    p_event_id TEXT, p_created_at TIMESTAMPTZ DEFAULT now()
) RETURNS public.execution_audit_handoff
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE v public.execution_audit_handoff;
BEGIN
    IF p_tenant_id IS NULL OR btrim(p_tenant_id) = '' OR p_handoff_id IS NULL OR btrim(p_handoff_id) = '' OR p_request_id IS NULL OR btrim(p_request_id) = '' THEN
        RAISE EXCEPTION 'invalid execution audit handoff';
    END IF;
    INSERT INTO public.execution_audit_handoff(tenant_id,handoff_id,request_id,trace_id,event_id,state,created_at,updated_at)
    VALUES(p_tenant_id,p_handoff_id,p_request_id,coalesce(p_trace_id,''),coalesce(p_event_id,''),'pending',coalesce(p_created_at,now()),coalesce(p_created_at,now()))
    ON CONFLICT (tenant_id,handoff_id) DO NOTHING
    RETURNING * INTO v;
    IF NOT FOUND THEN
        SELECT * INTO v FROM public.execution_audit_handoff WHERE tenant_id=p_tenant_id AND handoff_id=p_handoff_id;
        IF v.request_id <> p_request_id OR v.event_id <> coalesce(p_event_id,'') THEN
            RAISE EXCEPTION 'execution audit handoff conflict' USING ERRCODE='23505';
        END IF;
    END IF;
    RETURN v;
END $$;

CREATE OR REPLACE FUNCTION public.finalize_execution_audit_handoff(
    p_tenant_id TEXT, p_handoff_id TEXT, p_result TEXT, p_error_type TEXT DEFAULT NULL,
    p_latency_ms BIGINT DEFAULT NULL, p_updated_at TIMESTAMPTZ DEFAULT now()
) RETURNS public.execution_audit_handoff
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE v public.execution_audit_handoff;
BEGIN
    UPDATE public.execution_audit_handoff SET state='finalized', result=p_result, error_type=p_error_type, latency_ms=p_latency_ms, updated_at=coalesce(p_updated_at,now())
    WHERE tenant_id=p_tenant_id AND handoff_id=p_handoff_id AND state IN ('pending','repairable')
    RETURNING * INTO v;
    IF NOT FOUND THEN
        SELECT * INTO v FROM public.execution_audit_handoff WHERE tenant_id=p_tenant_id AND handoff_id=p_handoff_id;
        IF NOT FOUND THEN RAISE EXCEPTION 'execution audit handoff not found' USING ERRCODE='P0002'; END IF;
    END IF;
    RETURN v;
END $$;

CREATE OR REPLACE FUNCTION public.repair_execution_audit_handoff(
    p_tenant_id TEXT, p_handoff_id TEXT, p_updated_at TIMESTAMPTZ DEFAULT now()
) RETURNS public.execution_audit_handoff
LANGUAGE plpgsql SECURITY DEFINER SET search_path = public AS $$
DECLARE v public.execution_audit_handoff;
BEGIN
    UPDATE public.execution_audit_handoff SET state='repairable', updated_at=coalesce(p_updated_at,now())
    WHERE tenant_id=p_tenant_id AND handoff_id=p_handoff_id AND state='pending'
    RETURNING * INTO v;
    IF NOT FOUND THEN
        SELECT * INTO v FROM public.execution_audit_handoff WHERE tenant_id=p_tenant_id AND handoff_id=p_handoff_id;
        IF NOT FOUND THEN RAISE EXCEPTION 'execution audit handoff not found' USING ERRCODE='P0002'; END IF;
    END IF;
    RETURN v;
END $$;
