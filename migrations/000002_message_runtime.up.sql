-- Upgrade PR2 databases without mutating the already-published 000001 schema.
ALTER TABLE message_events ADD COLUMN IF NOT EXISTS inbox_id TEXT;
UPDATE message_events SET inbox_id = 'legacy:' || event_id WHERE inbox_id IS NULL;
ALTER TABLE message_events ALTER COLUMN inbox_id SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_message_events_tenant_inbox ON message_events (tenant_id, inbox_id);

ALTER TABLE inbox_messages ADD COLUMN IF NOT EXISTS inbox_id TEXT;
ALTER TABLE inbox_messages ADD COLUMN IF NOT EXISTS config_version BIGINT;
ALTER TABLE inbox_messages ADD COLUMN IF NOT EXISTS inbox_seq BIGINT;
ALTER TABLE inbox_messages ADD COLUMN IF NOT EXISTS claim_owner TEXT;
ALTER TABLE inbox_messages ADD COLUMN IF NOT EXISTS claim_token TEXT;
ALTER TABLE inbox_messages ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;
ALTER TABLE inbox_messages ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;
UPDATE inbox_messages
SET inbox_id = tenant_id || '/' || binding_id || '/' || external_message_id
WHERE inbox_id IS NULL;
UPDATE inbox_messages AS inbox
SET config_version = binding.config_version
FROM channel_bindings AS binding
WHERE inbox.tenant_id = binding.tenant_id AND inbox.binding_id = binding.binding_id
  AND inbox.config_version IS NULL;
WITH ordered AS (
  SELECT tenant_id, binding_id, external_message_id,
         ROW_NUMBER() OVER (PARTITION BY tenant_id, app_id, user_id, session_id ORDER BY created_at, external_message_id) AS seq
  FROM inbox_messages
)
UPDATE inbox_messages AS inbox
SET inbox_seq = ordered.seq
FROM ordered
WHERE inbox.tenant_id = ordered.tenant_id AND inbox.binding_id = ordered.binding_id
  AND inbox.external_message_id = ordered.external_message_id AND inbox.inbox_seq IS NULL;
ALTER TABLE inbox_messages ALTER COLUMN inbox_id SET NOT NULL;
ALTER TABLE inbox_messages ALTER COLUMN config_version SET NOT NULL;
ALTER TABLE inbox_messages ALTER COLUMN inbox_seq SET NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_inbox_tenant_id ON inbox_messages (tenant_id, inbox_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_inbox_session_seq ON inbox_messages (tenant_id, app_id, user_id, session_id, inbox_seq);
DO $$ BEGIN
  ALTER TABLE inbox_messages ADD CONSTRAINT fk_inbox_config_version
    FOREIGN KEY (tenant_id, config_version) REFERENCES config_versions(tenant_id, version);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Old Outbox rows do not contain enough provenance to invent a safe source event.
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = current_schema() AND table_name = 'outbox_messages' AND column_name = 'source_inbox_id'
  ) AND EXISTS (SELECT 1 FROM outbox_messages) THEN
    RAISE EXCEPTION '000002 requires draining legacy outbox_messages before upgrade';
  END IF;
END $$;
ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS app_id TEXT;
ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS source_inbox_id TEXT;
ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS source_event_id TEXT;
ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS fence BIGINT;
ALTER TABLE outbox_messages ALTER COLUMN app_id SET NOT NULL;
ALTER TABLE outbox_messages ALTER COLUMN source_inbox_id SET NOT NULL;
ALTER TABLE outbox_messages ALTER COLUMN source_event_id SET NOT NULL;
ALTER TABLE outbox_messages ALTER COLUMN fence SET NOT NULL;
DO $$ BEGIN
  ALTER TABLE outbox_messages ADD CONSTRAINT fk_outbox_inbox
    FOREIGN KEY (tenant_id, source_inbox_id) REFERENCES inbox_messages(tenant_id, inbox_id);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
DO $$ BEGIN
  ALTER TABLE outbox_messages ADD CONSTRAINT fk_outbox_event
    FOREIGN KEY (tenant_id, source_event_id) REFERENCES message_events(tenant_id, event_id);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE TABLE IF NOT EXISTS derived_jobs (
  tenant_id TEXT NOT NULL, app_id TEXT NOT NULL, user_id TEXT NOT NULL, session_id TEXT NOT NULL,
  job_id TEXT NOT NULL, job_type TEXT NOT NULL, source_event_id TEXT NOT NULL, source_event_seq BIGINT NOT NULL,
  status TEXT NOT NULL, attempts INT NOT NULL DEFAULT 0, claim_owner TEXT, claim_token TEXT, lease_until TIMESTAMPTZ,
  next_attempt_at TIMESTAMPTZ, last_error TEXT, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, job_id), UNIQUE (tenant_id, job_type, source_event_id),
  FOREIGN KEY (tenant_id, source_event_id) REFERENCES message_events(tenant_id, event_id)
);
CREATE INDEX IF NOT EXISTS idx_derived_jobs_ready ON derived_jobs (tenant_id, status, next_attempt_at);
