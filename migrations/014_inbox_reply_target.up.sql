-- Capture the provider-native reply/thread target at trusted ingress. The
-- Worker completion seam is then unable to redirect an Outbox message by
-- supplying its own tenant, account, conversation or reply target.
ALTER TABLE inbox_messages
    ADD COLUMN reply_to_id VARCHAR(256);

-- Historical Telegram Inbox rows used update_id as external_message_id but
-- stored the chat-local message_id in canonical payload metadata. Using the
-- update_id as reply_to_message_id is incorrect. Invalid or legacy payloads
-- safely fall back to an empty target, which sends an unthreaded reply to the
-- authoritative conversation instead of targeting the wrong message.
CREATE FUNCTION trpc_legacy_inbox_reply_target(
    channel TEXT,
    canonical_payload BYTEA,
    external_id TEXT
)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    document JSONB;
    candidate TEXT;
BEGIN
    IF channel = 'telegram' THEN
        BEGIN
            document := convert_from(canonical_payload, 'UTF8')::jsonb;
            candidate := COALESCE(
                document->>'replyToId',
                document #>> '{metadata,provider_message_id}'
            );
        EXCEPTION WHEN OTHERS THEN
            candidate := '';
        END;
    ELSE
        candidate := external_id;
    END IF;

    IF candidate IS NULL OR octet_length(candidate) > 256 THEN
        RETURN '';
    END IF;
    RETURN candidate;
END
$$;

UPDATE inbox_messages
SET reply_to_id = trpc_legacy_inbox_reply_target(
    channel_type,
    payload,
    external_message_id
);

-- Old Gateway binaries omit reply_to_id. Preserve a rolling-upgrade overlap
-- window by deriving the trusted value from fields already written by those
-- binaries. New binaries provide the normalized value explicitly.
CREATE FUNCTION trpc_assign_inbox_reply_target()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.reply_to_id IS NULL OR NEW.reply_to_id = '' THEN
        NEW.reply_to_id := trpc_legacy_inbox_reply_target(
            NEW.channel_type,
            NEW.payload,
            NEW.external_message_id
        );
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER inbox_reply_target_compat
BEFORE INSERT ON inbox_messages
FOR EACH ROW
EXECUTE FUNCTION trpc_assign_inbox_reply_target();

ALTER TABLE inbox_messages
    ALTER COLUMN reply_to_id SET NOT NULL,
    ALTER COLUMN reply_to_id SET DEFAULT '',
    ADD CONSTRAINT inbox_reply_to_id_byte_limit
        CHECK (octet_length(reply_to_id) <= 256);
