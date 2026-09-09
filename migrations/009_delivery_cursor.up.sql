ALTER TABLE outbox_messages
    ADD COLUMN IF NOT EXISTS delivery_cursor INTEGER NOT NULL DEFAULT 0
        CHECK (delivery_cursor >= 0);
