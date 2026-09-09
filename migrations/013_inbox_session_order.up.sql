-- Durable FIFO for each tenant/agent-app/session stream.
--
-- The byte preflight runs before the new indexes are created. PostgreSQL
-- VARCHAR limits characters, while btree index tuple limits are byte based;
-- accepting a historical multi-byte value that cannot be indexed would make
-- this migration fail halfway through an upgrade.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM inbox_messages
        WHERE octet_length(tenant_id) > 64
           OR octet_length(agent_app_name) > 128
           OR octet_length(session_id) > 512
    ) THEN
        RAISE EXCEPTION
            'inbox stream key exceeds byte limit (tenant=64, agent_app=128, session=512)';
    END IF;
END
$$;

ALTER TABLE inbox_messages
    ADD COLUMN IF NOT EXISTS session_sequence BIGINT;

CREATE TABLE IF NOT EXISTS inbox_session_sequences (
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    agent_app_name VARCHAR(128) NOT NULL,
    session_id VARCHAR(512) NOT NULL,
    last_sequence BIGINT NOT NULL DEFAULT 0 CHECK (last_sequence >= 0),
    PRIMARY KEY (tenant_id, agent_app_name, session_id),
    CHECK (octet_length(tenant_id) <= 64),
    CHECK (octet_length(agent_app_name) <= 128),
    CHECK (octet_length(session_id) <= 512)
);

-- Existing rows receive a deterministic sequence. created_at is the logical
-- arrival order and id is the stable tie breaker for equal timestamps.
WITH ranked AS (
    SELECT id,
           row_number() OVER (
               PARTITION BY tenant_id, agent_app_name, session_id
               ORDER BY created_at, id
           ) AS sequence
    FROM inbox_messages
)
UPDATE inbox_messages AS messages
SET session_sequence = ranked.sequence
FROM ranked
WHERE messages.id = ranked.id;

INSERT INTO inbox_session_sequences (tenant_id, agent_app_name, session_id, last_sequence)
SELECT tenant_id, agent_app_name, session_id, MAX(session_sequence)
FROM inbox_messages
GROUP BY tenant_id, agent_app_name, session_id
ON CONFLICT (tenant_id, agent_app_name, session_id)
DO UPDATE SET last_sequence = GREATEST(
    inbox_session_sequences.last_sequence,
    EXCLUDED.last_sequence
);

-- Expand/contract compatibility for a rolling deployment. Gateways from the
-- previous binary omit session_sequence; after this migration commits, the
-- trigger allocates it atomically until every old replica has drained. New
-- binaries provide an explicit value after updating the same counter row.
CREATE FUNCTION trpc_assign_inbox_session_sequence()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.session_sequence IS NULL THEN
        INSERT INTO inbox_session_sequences (
            tenant_id, agent_app_name, session_id, last_sequence
        ) VALUES (
            NEW.tenant_id, NEW.agent_app_name, NEW.session_id, 1
        )
        ON CONFLICT (tenant_id, agent_app_name, session_id)
        DO UPDATE SET last_sequence = inbox_session_sequences.last_sequence + 1
        RETURNING last_sequence INTO NEW.session_sequence;
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER inbox_session_sequence_compat
BEFORE INSERT ON inbox_messages
FOR EACH ROW
WHEN (NEW.session_sequence IS NULL)
EXECUTE FUNCTION trpc_assign_inbox_session_sequence();

ALTER TABLE inbox_messages
    ALTER COLUMN session_sequence SET NOT NULL;

ALTER TABLE inbox_messages
    ADD CONSTRAINT inbox_session_sequence_positive
        CHECK (session_sequence > 0),
    ADD CONSTRAINT inbox_stream_key_byte_limits
        CHECK (
            octet_length(tenant_id) <= 64
            AND octet_length(agent_app_name) <= 128
            AND octet_length(session_id) <= 512
        );

CREATE UNIQUE INDEX uq_inbox_session_sequence
    ON inbox_messages (tenant_id, agent_app_name, session_id, session_sequence);

CREATE INDEX idx_inbox_session_order
    ON inbox_messages (tenant_id, agent_app_name, session_id, session_sequence, status);

-- The claim predicate asks whether any unfinished predecessor exists. Keeping
-- only unfinished rows in this index makes the negative lookup independent of
-- the completed history length of a long-running session.
CREATE INDEX idx_inbox_unfinished_session_order
    ON inbox_messages (tenant_id, agent_app_name, session_id, session_sequence)
    WHERE status <> 'COMPLETED';
