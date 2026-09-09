-- Tenant-scoped operational history uses time/ID keyset pagination. Keep the
-- complete ordering in the index so a bounded page does not sort the tenant's
-- full Inbox or execution history. Migrations run in a transaction; schedule
-- index construction within the deployment maintenance window.
CREATE INDEX IF NOT EXISTS idx_inbox_operations_tenant_time_id
    ON inbox_messages (tenant_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_execution_operations_tenant_time_id
    ON execution_records (tenant_id, started_at DESC, id DESC);
