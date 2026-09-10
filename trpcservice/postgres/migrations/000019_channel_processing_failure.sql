ALTER TABLE platform.channel_inbox
    DROP CONSTRAINT channel_inbox_reject_reason_check;

ALTER TABLE platform.channel_inbox
    ADD CONSTRAINT channel_inbox_reject_reason_check CHECK (
        (status = 'ADMITTED' AND reject_reason IS NULL)
        OR (status = 'REJECTED' AND reject_reason IN (
            'UNSUPPORTED_MESSAGE_TYPE', 'ATTACHMENT_REJECTED',
            'IM_ACCESS_DENIED', 'CHANNEL_PROCESSING_FAILED'
        ))
    );

ALTER TABLE platform.reply_outbox
    DROP CONSTRAINT reply_outbox_source_kind_check;

ALTER TABLE platform.reply_outbox
    ADD CONSTRAINT reply_outbox_source_kind_check
    CHECK (source_kind IN ('execution', 'channel_command', 'channel_failure'));

CREATE OR REPLACE FUNCTION platform.validate_reply_outbox_source()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.source_kind = 'execution' THEN
        IF NOT EXISTS (
            SELECT 1
            FROM platform.execution e
            WHERE e.tenant_id = NEW.tenant_id
              AND e.app_id = NEW.app_id
              AND e.request_id = NEW.request_id
        ) THEN
            RAISE EXCEPTION 'reply outbox execution source is missing'
                USING ERRCODE = '23503';
        END IF;
    ELSIF NEW.source_kind IN ('channel_command', 'channel_failure') THEN
        IF NOT EXISTS (
            SELECT 1
            FROM platform.channel_inbox i
            WHERE i.tenant_id = NEW.tenant_id
              AND i.app_id = NEW.app_id
              AND i.binding_id = NEW.binding_id
              AND i.request_id = NEW.request_id
        ) THEN
            RAISE EXCEPTION 'reply outbox channel source is missing'
                USING ERRCODE = '23503';
        END IF;
    ELSE
        RAISE EXCEPTION 'reply outbox source kind is invalid';
    END IF;
    RETURN NEW;
END;
$$;
