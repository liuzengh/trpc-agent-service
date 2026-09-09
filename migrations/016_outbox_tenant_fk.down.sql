ALTER TABLE outbox_messages
    DROP CONSTRAINT IF EXISTS outbox_inbox_tenant_fk;

DROP INDEX IF EXISTS uq_inbox_id_tenant;
