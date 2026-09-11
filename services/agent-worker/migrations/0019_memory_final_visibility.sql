-- Memory is applied only after the immutable execution result is accepted.
-- This local visibility gate is not a recovery queue: the owning Processor
-- synchronously decides APPLIED/FAILED and releases a single corresponding Final.
ALTER TABLE execution_completions ADD COLUMN memory_digest text NOT NULL DEFAULT '';
ALTER TABLE execution_completions ADD COLUMN memory_status text NOT NULL DEFAULT '';
ALTER TABLE execution_completions ADD CONSTRAINT execution_memory_status_valid CHECK (
 (memory_digest='' AND memory_status='') OR
 (memory_digest ~ '^sha256:[0-9a-f]{64}$' AND memory_status IN ('PENDING','APPLIED','FAILED') AND kind='ATTEMPT' AND status='SUCCEEDED')
);
ALTER TABLE execution_reply_outbox ADD COLUMN ready boolean NOT NULL DEFAULT true;
CREATE INDEX execution_memory_pending ON execution_completions(tenant_id,run_id) WHERE memory_status='PENDING';
