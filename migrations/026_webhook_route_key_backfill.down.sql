-- Rollback deliberately disables callback lookup: the runtime only accepts
-- webhook_key, so dropping this column cannot accidentally reactivate a
-- legacy/provider token. The forward migration must be reapplied before
-- callbacks are accepted again.
DROP INDEX IF EXISTS uq_tenant_channels_webhook_key;
ALTER TABLE tenant_channels
    DROP COLUMN IF EXISTS webhook_key,
    DROP COLUMN IF EXISTS channel_index;
