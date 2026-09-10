-- Issue #48: deleting a runtime Session also removes its inbound facts and pending replies.
SET LOCAL search_path = pg_catalog, public, pg_temp;

ALTER TABLE public.message_event
    DROP CONSTRAINT message_event_tenant_id_session_id_fkey,
    ADD CONSTRAINT message_event_tenant_id_session_id_fkey
        FOREIGN KEY (tenant_id, session_id)
        REFERENCES public.runtime_session(tenant_id, session_id) ON DELETE CASCADE;

ALTER TABLE public.reply_outbox
    DROP CONSTRAINT reply_outbox_tenant_id_event_id_fkey,
    ADD CONSTRAINT reply_outbox_tenant_id_event_id_fkey
        FOREIGN KEY (tenant_id, event_id)
        REFERENCES public.message_event(tenant_id, event_id) ON DELETE CASCADE;
