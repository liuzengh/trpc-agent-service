DROP TRIGGER IF EXISTS inbox_reply_target_compat ON inbox_messages;
DROP FUNCTION IF EXISTS trpc_assign_inbox_reply_target();
DROP FUNCTION IF EXISTS trpc_legacy_inbox_reply_target(TEXT, BYTEA, TEXT);

ALTER TABLE inbox_messages
    DROP CONSTRAINT IF EXISTS inbox_reply_to_id_byte_limit,
    DROP COLUMN IF EXISTS reply_to_id;
