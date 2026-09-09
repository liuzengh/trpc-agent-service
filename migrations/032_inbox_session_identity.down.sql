ALTER TABLE inbox_messages
    DROP CONSTRAINT IF EXISTS inbox_routing_version_check,
    DROP CONSTRAINT IF EXISTS inbox_session_owner_required,
    DROP CONSTRAINT IF EXISTS inbox_session_owner_byte_limit;

ALTER TABLE inbox_messages
    DROP COLUMN IF EXISTS routing_version,
    DROP COLUMN IF EXISTS session_owner_id,
    DROP COLUMN IF EXISTS is_group_chat;
