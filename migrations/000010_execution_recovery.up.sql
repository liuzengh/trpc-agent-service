-- Durable Runner progress used to resume reclaimed Inbox claims without
-- invoking the model or side-effecting tools a second time.
ALTER TABLE inbox_messages ADD COLUMN IF NOT EXISTS execution_stage TEXT NOT NULL DEFAULT 'none';
ALTER TABLE inbox_messages ADD COLUMN IF NOT EXISTS execution_reply TEXT;
ALTER TABLE inbox_messages ADD COLUMN IF NOT EXISTS execution_event_id TEXT;
DO $$ BEGIN
  ALTER TABLE inbox_messages ADD CONSTRAINT inbox_execution_stage_check
    CHECK (execution_stage IN ('none','runner_committed','derived_committed','outbox_committed'));
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
CREATE INDEX IF NOT EXISTS idx_inbox_execution_stage ON inbox_messages (tenant_id, execution_stage);
