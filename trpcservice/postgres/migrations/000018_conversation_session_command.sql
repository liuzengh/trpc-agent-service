CREATE TABLE platform.conversation_session (
    tenant_id TEXT NOT NULL,
    app_id TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    session_principal_id TEXT NOT NULL CHECK (session_principal_id <> ''),
    active_session_id TEXT NOT NULL CHECK (active_session_id <> ''),
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, binding_id, session_principal_id),
    FOREIGN KEY (tenant_id, app_id, binding_id)
        REFERENCES platform.channel_binding (tenant_id, app_id, binding_id)
);

CREATE INDEX conversation_session_active_idx
    ON platform.conversation_session (
        tenant_id, app_id, binding_id, session_principal_id, active_session_id
    );

ALTER TABLE platform.reply_outbox
    DROP CONSTRAINT IF EXISTS reply_outbox_tenant_id_app_id_request_id_fkey;

ALTER TABLE platform.reply_outbox
    ADD COLUMN source_kind TEXT NOT NULL DEFAULT 'execution';

ALTER TABLE platform.reply_outbox
    ADD CONSTRAINT reply_outbox_source_kind_check
    CHECK (source_kind IN ('execution', 'channel_command'));

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
    ELSIF NEW.source_kind = 'channel_command' THEN
        IF NOT EXISTS (
            SELECT 1
            FROM platform.channel_inbox i
            WHERE i.tenant_id = NEW.tenant_id
              AND i.app_id = NEW.app_id
              AND i.binding_id = NEW.binding_id
              AND i.request_id = NEW.request_id
        ) THEN
            RAISE EXCEPTION 'reply outbox channel command source is missing'
                USING ERRCODE = '23503';
        END IF;
    ELSE
        RAISE EXCEPTION 'reply outbox source kind is invalid';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS reply_outbox_source_integrity_trg
    ON platform.reply_outbox;

CREATE CONSTRAINT TRIGGER reply_outbox_source_integrity_trg
AFTER INSERT OR UPDATE ON platform.reply_outbox
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION platform.validate_reply_outbox_source();
