-- Queue admission counts active Inbox states by tenant. Keep a narrow partial
-- index for this hot ingress query instead of repeatedly scanning all tenant
-- history and terminal rows.
CREATE INDEX IF NOT EXISTS idx_inbox_queue_admission_tenant_status
    ON inbox_messages (tenant_id, status)
    WHERE status IN ('RECEIVED','PROCESSING','RETRY_WAIT','WAITING_APPROVAL');
