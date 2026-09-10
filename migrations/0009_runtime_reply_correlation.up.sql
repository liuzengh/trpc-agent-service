-- Durable request/trace correlation for asynchronous reply delivery audits.
CREATE TABLE public.runtime_reply_correlation (
    tenant_id  TEXT NOT NULL,
    event_id   TEXT NOT NULL,
    request_id TEXT NOT NULL CHECK (length(btrim(request_id)) BETWEEN 1 AND 256),
    trace_id   TEXT NOT NULL DEFAULT '' CHECK (length(trace_id) <= 256),
    PRIMARY KEY (tenant_id, event_id),
    FOREIGN KEY (tenant_id, event_id) REFERENCES public.message_event(tenant_id, event_id) ON DELETE CASCADE
);

REVOKE ALL ON TABLE public.runtime_reply_correlation FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.runtime_reply_correlation TO tenant_app_writer;
GRANT ALL PRIVILEGES ON public.runtime_reply_correlation TO migration_owner;
