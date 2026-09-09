DROP INDEX IF EXISTS idx_outbox_unfinished_session_order;
DROP INDEX IF EXISTS uq_outbox_session_sequence;

DROP TRIGGER IF EXISTS outbox_session_order_compat ON outbox_messages;
DROP FUNCTION IF EXISTS trpc_assign_outbox_session_order();

ALTER TABLE outbox_messages
    DROP CONSTRAINT IF EXISTS outbox_stream_key_byte_limits,
    DROP CONSTRAINT IF EXISTS outbox_session_sequence_positive,
    DROP COLUMN IF EXISTS session_sequence,
    DROP COLUMN IF EXISTS session_id,
    DROP COLUMN IF EXISTS agent_app_name;
