-- Bind execution audit rows to the same request identity used by the result
-- cache. This enables a cached retry to finish a RUNNING audit row after a
-- replica crashed between result persistence and Finish.
ALTER TABLE execution_records
    ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(256),
    ADD COLUMN IF NOT EXISTS payload_hash CHAR(64);

CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_request
    ON execution_records (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL AND idempotency_key <> '';

CREATE INDEX IF NOT EXISTS idx_execution_request_lookup
    ON execution_records (tenant_id, idempotency_key, status)
    WHERE idempotency_key IS NOT NULL AND idempotency_key <> '';
