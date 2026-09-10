ALTER TABLE platform.channel_binding
    ADD COLUMN connection_status TEXT NOT NULL DEFAULT 'NOT_READY',
    ADD COLUMN last_connected_at TIMESTAMPTZ,
    ADD COLUMN last_error TEXT NOT NULL DEFAULT '';

ALTER TABLE platform.channel_binding
    ADD CONSTRAINT channel_binding_connection_status_ck
        CHECK (connection_status IN ('READY', 'NOT_READY', 'DEGRADED'));
