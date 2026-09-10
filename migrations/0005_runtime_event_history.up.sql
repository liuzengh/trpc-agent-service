-- Issue #48: append-only upstream Runner event history for durable session recovery.
SET LOCAL search_path = pg_catalog, public, pg_temp;

CREATE TABLE public.runtime_event_history (
    tenant_id   TEXT NOT NULL,
    session_id  TEXT NOT NULL,
    event_id    TEXT NOT NULL CHECK (length(btrim(event_id)) BETWEEN 1 AND 256),
    payload     JSONB NOT NULL,
    history_seq BIGINT GENERATED ALWAYS AS IDENTITY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, session_id, event_id),
    UNIQUE (tenant_id, session_id, history_seq),
    FOREIGN KEY (tenant_id, session_id)
        REFERENCES public.runtime_session(tenant_id, session_id) ON DELETE CASCADE
);

REVOKE ALL ON TABLE public.runtime_event_history FROM PUBLIC;
GRANT SELECT, INSERT ON public.runtime_event_history TO tenant_app_writer;
-- The idempotent append uses ON CONFLICT DO UPDATE to return an existing row.
-- Limit the runtime role to that no-op key-column update; payload and delete
-- mutations remain unavailable.
GRANT UPDATE (event_id) ON public.runtime_event_history TO tenant_app_writer;
GRANT ALL PRIVILEGES ON TABLE public.runtime_event_history TO migration_owner;
GRANT USAGE, SELECT ON SEQUENCE public.runtime_event_history_history_seq_seq TO tenant_app_writer;
GRANT ALL PRIVILEGES ON SEQUENCE public.runtime_event_history_history_seq_seq TO migration_owner;
