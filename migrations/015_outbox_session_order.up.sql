-- Preserve user-visible reply order for each tenant/agent/session stream. The
-- order key is copied from the authoritative Inbox row in the same completion
-- transaction so Delivery can enforce FIFO without scanning Inbox history.
ALTER TABLE outbox_messages
    ADD COLUMN agent_app_name VARCHAR(128),
    ADD COLUMN session_id VARCHAR(512),
    ADD COLUMN session_sequence BIGINT;

UPDATE outbox_messages AS outbox
SET agent_app_name = inbox.agent_app_name,
    session_id = inbox.session_id,
    session_sequence = inbox.session_sequence
FROM inbox_messages AS inbox
WHERE inbox.id = outbox.inbox_id;

-- Old Consumer binaries omit the new order columns. This trigger supplies a
-- rolling-upgrade overlap window; new binaries provide them explicitly.
CREATE FUNCTION trpc_assign_outbox_session_order()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.agent_app_name IS NULL
       OR NEW.session_id IS NULL
       OR NEW.session_sequence IS NULL THEN
        SELECT agent_app_name, session_id, session_sequence
        INTO NEW.agent_app_name, NEW.session_id, NEW.session_sequence
        FROM inbox_messages
        WHERE id = NEW.inbox_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'outbox inbox_id % does not exist', NEW.inbox_id;
        END IF;
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER outbox_session_order_compat
BEFORE INSERT ON outbox_messages
FOR EACH ROW
EXECUTE FUNCTION trpc_assign_outbox_session_order();

ALTER TABLE outbox_messages
    ALTER COLUMN agent_app_name SET NOT NULL,
    ALTER COLUMN session_id SET NOT NULL,
    ALTER COLUMN session_sequence SET NOT NULL,
    ADD CONSTRAINT outbox_session_sequence_positive
        CHECK (session_sequence > 0),
    ADD CONSTRAINT outbox_stream_key_byte_limits
        CHECK (
            octet_length(agent_app_name) <= 128
            AND octet_length(session_id) <= 512
        );

CREATE UNIQUE INDEX uq_outbox_session_sequence
    ON outbox_messages (tenant_id, agent_app_name, session_id, session_sequence);

CREATE INDEX idx_outbox_unfinished_session_order
    ON outbox_messages (tenant_id, agent_app_name, session_id, session_sequence)
    WHERE status <> 'REPLIED';
