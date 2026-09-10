ALTER TABLE platform.audit_event
    ADD COLUMN IF NOT EXISTS actor_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS actor_role TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS requested_tenant_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS requested_app_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS query_digest TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS result_count INTEGER NOT NULL DEFAULT 0;

ALTER TABLE platform.audit_event
    DROP CONSTRAINT IF EXISTS audit_event_result_count_check;

ALTER TABLE platform.audit_event
    ADD CONSTRAINT audit_event_result_count_check CHECK (result_count >= 0);

CREATE INDEX IF NOT EXISTS audit_event_actor_idx
    ON platform.audit_event (tenant_id, app_id, actor_role, created_at, audit_event_id);
