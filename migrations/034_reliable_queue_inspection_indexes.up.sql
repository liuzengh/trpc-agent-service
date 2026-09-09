-- QueueInspector reads automatic work by created_at. These partial indexes
-- keep oldest-message alerts and queue-depth snapshots bounded as terminal and
-- reconciliation rows accumulate in the durable tables.
-- The migration runner applies a short lock_timeout preflight for this
-- transactional DDL. It must run in a maintenance window on large tables;
-- CREATE INDEX CONCURRENTLY is intentionally not used because every migration
-- is committed atomically with its schema_migrations checksum row.
CREATE INDEX IF NOT EXISTS idx_inbox_automatic_queue_created
    ON inbox_messages (created_at, id)
    WHERE status IN ('RECEIVED','PROCESSING','RETRY_WAIT','WAITING_APPROVAL');

CREATE INDEX IF NOT EXISTS idx_outbox_automatic_queue_created
    ON outbox_messages (created_at, id)
    WHERE status IN ('REPLY_PENDING','DELIVERING','DISPATCH_STARTED','RETRY_WAIT');
