DROP INDEX IF EXISTS idx_execution_request_lookup;
DROP INDEX IF EXISTS uq_execution_request;
ALTER TABLE execution_records
    DROP COLUMN IF EXISTS payload_hash,
    DROP COLUMN IF EXISTS idempotency_key;
