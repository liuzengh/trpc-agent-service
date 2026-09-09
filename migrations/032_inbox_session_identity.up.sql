-- Persist the routing decision made by the authenticated Gateway.  Version
-- zero rows are historical records written before these columns existed and
-- must be proven from their original payload by the Consumer; they are never
-- guessed as direct messages.
ALTER TABLE inbox_messages
    ADD COLUMN IF NOT EXISTS is_group_chat BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS session_owner_id VARCHAR(255) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS routing_version INTEGER NOT NULL DEFAULT 0;

ALTER TABLE inbox_messages
    DROP CONSTRAINT IF EXISTS inbox_routing_version_check,
    DROP CONSTRAINT IF EXISTS inbox_session_owner_required,
    DROP CONSTRAINT IF EXISTS inbox_session_owner_byte_limit;

ALTER TABLE inbox_messages
    ADD CONSTRAINT inbox_routing_version_check
        CHECK (routing_version >= 0 AND routing_version <= 1),
    ADD CONSTRAINT inbox_session_owner_required
        CHECK (routing_version = 0 OR octet_length(session_owner_id) > 0),
    ADD CONSTRAINT inbox_session_owner_byte_limit
        CHECK (octet_length(session_owner_id) <= 255);

COMMENT ON COLUMN inbox_messages.is_group_chat IS
    'Authenticated Gateway group/direct routing decision; authoritative when routing_version >= 1';
COMMENT ON COLUMN inbox_messages.session_owner_id IS
    'Runner Session owner identity; shared group owner or direct actor, authoritative when routing_version >= 1';
COMMENT ON COLUMN inbox_messages.routing_version IS
    'Routing schema version; zero denotes a legacy row requiring payload proof';
