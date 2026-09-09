ALTER TABLE message_replay_audit
    ADD COLUMN replay_mode VARCHAR(16) NOT NULL DEFAULT 'restart'
        CHECK (replay_mode IN ('resume', 'restart'));
