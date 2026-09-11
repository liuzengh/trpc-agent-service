-- Retire a route without deleting conversations, deliveries or audit records.
ALTER TABLE channel_binding ADD COLUMN retired_at TIMESTAMPTZ;
ALTER TABLE channel_binding ADD CONSTRAINT channel_binding_retired_disabled
    CHECK (retired_at IS NULL OR status='disabled');
ALTER TABLE channel_binding DROP CONSTRAINT channel_binding_tenant_id_channel_type_account_id_key;
CREATE UNIQUE INDEX channel_binding_current_account
    ON channel_binding(tenant_id,channel_type,account_id) WHERE retired_at IS NULL;

ALTER TABLE channel_connection DROP CONSTRAINT channel_connection_status_check;
ALTER TABLE channel_connection ADD CONSTRAINT channel_connection_status_check
    CHECK(status IN ('draft','needs_confirmation','connecting','connected','paused','error','unknown','removed'));
ALTER TABLE channel_connection ADD COLUMN operation TEXT NOT NULL DEFAULT ''
    CHECK(operation IN ('','activate','remove'));
ALTER TABLE channel_connection DROP CONSTRAINT channel_connection_channel_type_account_key_key;
CREATE UNIQUE INDEX channel_connection_current_account
    ON channel_connection(channel_type,account_key) WHERE status<>'removed';
