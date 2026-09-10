-- Durable, trusted channel reply destinations. Empty values represent rows
-- written before per-message routing; partial destinations are never valid.
ALTER TABLE public.message_event
    ADD COLUMN reply_conversation_kind TEXT NOT NULL DEFAULT ''
        CHECK (reply_conversation_kind IN ('', 'direct', 'group')),
    ADD COLUMN reply_receiver_id TEXT NOT NULL DEFAULT ''
        CHECK (length(reply_receiver_id) <= 1024),
    ADD COLUMN reply_thread_id TEXT NOT NULL DEFAULT ''
        CHECK (length(reply_thread_id) <= 1024),
    ADD CONSTRAINT message_event_reply_target_complete CHECK (
        (reply_conversation_kind = '' AND reply_receiver_id = '' AND reply_thread_id = '')
        OR (reply_conversation_kind IN ('direct', 'group') AND btrim(reply_receiver_id) <> '')
    );

ALTER TABLE public.reply_outbox
    ADD COLUMN reply_binding_id TEXT NOT NULL DEFAULT ''
        CHECK (length(reply_binding_id) <= 256),
    ADD COLUMN reply_conversation_kind TEXT NOT NULL DEFAULT ''
        CHECK (reply_conversation_kind IN ('', 'direct', 'group')),
    ADD COLUMN reply_receiver_id TEXT NOT NULL DEFAULT ''
        CHECK (length(reply_receiver_id) <= 1024),
    ADD COLUMN reply_thread_id TEXT NOT NULL DEFAULT ''
        CHECK (length(reply_thread_id) <= 1024),
    ADD CONSTRAINT reply_outbox_reply_target_complete CHECK (
        (reply_binding_id = '' AND reply_conversation_kind = '' AND reply_receiver_id = '' AND reply_thread_id = '')
        OR (btrim(reply_binding_id) <> '' AND reply_conversation_kind IN ('direct', 'group') AND btrim(reply_receiver_id) <> '')
    );
