ALTER TABLE platform.reply_outbox
    ADD COLUMN reply_kind TEXT NOT NULL DEFAULT 'text',
    ADD COLUMN stream_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN stream_phase TEXT NOT NULL DEFAULT '',
    ADD COLUMN stream_sequence BIGINT NOT NULL DEFAULT 0;

ALTER TABLE platform.reply_outbox
    DROP CONSTRAINT IF EXISTS reply_outbox_status_check;

ALTER TABLE platform.reply_outbox
    ADD CONSTRAINT reply_outbox_status_check CHECK (
        status IN ('PENDING', 'SENDING', 'SENT', 'UNCERTAIN', 'PERMANENTLY_FAILED', 'SUPERSEDED')
    );

ALTER TABLE platform.reply_outbox
    ADD CONSTRAINT reply_outbox_kind_check CHECK (
        reply_kind IN ('text', 'stream', 'card')
    ),
    ADD CONSTRAINT reply_outbox_stream_check CHECK (
        (reply_kind IN ('stream', 'card') AND stream_id <> '' AND stream_phase IN ('start', 'update', 'end', 'abort') AND stream_sequence > 0)
        OR (reply_kind = 'text' AND stream_id = '' AND stream_phase = '' AND stream_sequence = 0)
    );

DROP INDEX IF EXISTS reply_outbox_stream_identity_idx;

CREATE UNIQUE INDEX reply_outbox_stream_identity_idx
    ON platform.reply_outbox (
        tenant_id, app_id, binding_id, request_id, reply_kind, stream_id, stream_sequence
    )
    WHERE reply_kind IN ('stream', 'card') AND stream_id <> '';

ALTER TABLE platform.execution_event
    ADD COLUMN event_id TEXT NOT NULL DEFAULT '';

CREATE UNIQUE INDEX execution_event_identity_idx
    ON platform.execution_event (tenant_id, app_id, request_id, event_id)
    WHERE event_id <> '';
