DROP INDEX IF EXISTS idx_inbox_execution_stage;
ALTER TABLE inbox_messages DROP CONSTRAINT IF EXISTS inbox_execution_stage_check;
ALTER TABLE inbox_messages DROP COLUMN IF EXISTS execution_event_id;
ALTER TABLE inbox_messages DROP COLUMN IF EXISTS execution_reply;
ALTER TABLE inbox_messages DROP COLUMN IF EXISTS execution_stage;
