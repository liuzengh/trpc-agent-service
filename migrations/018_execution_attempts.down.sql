-- Rollback is intentionally refused when append-only attempts have produced
-- more than one row for a request. Recreating the old unique index by deleting
-- audit history would be destructive and is never an implicit migration step.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM execution_records
        WHERE idempotency_key IS NOT NULL AND idempotency_key <> ''
        GROUP BY tenant_id, idempotency_key
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION
            'cannot roll back execution attempts while multiple attempts exist; archive history explicitly first';
    END IF;
END
$$;

DROP INDEX IF EXISTS idx_execution_request_latest;
DROP INDEX IF EXISTS uq_invocation_result_execution;

ALTER TABLE invocation_results
    DROP CONSTRAINT IF EXISTS invocation_result_execution_tenant_fk,
    DROP CONSTRAINT IF EXISTS invocation_result_execution_positive,
    DROP COLUMN IF EXISTS execution_id;

DROP INDEX IF EXISTS uq_execution_record_id_tenant;
DROP INDEX IF EXISTS uq_execution_token;
DROP INDEX IF EXISTS uq_execution_request_running;
DROP INDEX IF EXISTS uq_execution_request_attempt;

ALTER TABLE execution_records
    DROP CONSTRAINT IF EXISTS execution_token_byte_limit,
    DROP CONSTRAINT IF EXISTS execution_lease_order,
    DROP CONSTRAINT IF EXISTS execution_retry_safe_status,
    DROP CONSTRAINT IF EXISTS execution_attempt_number_positive,
    DROP COLUMN IF EXISTS lease_until,
    DROP COLUMN IF EXISTS heartbeat_at,
    DROP COLUMN IF EXISTS retry_safe,
    DROP COLUMN IF EXISTS execution_token,
    DROP COLUMN IF EXISTS attempt_number;

ALTER TABLE invocation_bindings
    DROP CONSTRAINT IF EXISTS invocation_binding_session_format,
    DROP CONSTRAINT IF EXISTS invocation_binding_payload_hash_format,
    DROP COLUMN IF EXISTS session_id,
    DROP COLUMN IF EXISTS payload_hash;

CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_request
    ON execution_records (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL AND idempotency_key <> '';
