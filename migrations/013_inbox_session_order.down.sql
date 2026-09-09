DROP INDEX IF EXISTS idx_inbox_unfinished_session_order;
DROP INDEX IF EXISTS idx_inbox_session_order;
DROP INDEX IF EXISTS uq_inbox_session_sequence;

DROP TRIGGER IF EXISTS inbox_session_sequence_compat ON inbox_messages;
DROP FUNCTION IF EXISTS trpc_assign_inbox_session_sequence();

ALTER TABLE inbox_messages
    DROP CONSTRAINT IF EXISTS inbox_stream_key_byte_limits,
    DROP CONSTRAINT IF EXISTS inbox_session_sequence_positive;

DROP TABLE IF EXISTS inbox_session_sequences;

ALTER TABLE inbox_messages
    DROP COLUMN IF EXISTS session_sequence;
