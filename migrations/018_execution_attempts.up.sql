-- Keep request identity separate from append-only execution attempts.
-- The binding row is the serialization seam for one tenant/idempotency key;
-- execution rows are never reused, so a late worker cannot finish a newer
-- attempt after a retry.
ALTER TABLE invocation_bindings
    ADD COLUMN IF NOT EXISTS session_id VARCHAR(512),
    ADD COLUMN IF NOT EXISTS payload_hash CHAR(64);

ALTER TABLE invocation_bindings
    ADD CONSTRAINT invocation_binding_payload_hash_format
    CHECK (
        payload_hash IS NULL
        OR payload_hash = ''
        OR payload_hash ~ '^[0-9a-fA-F]{64}$'
    );

ALTER TABLE invocation_bindings
    ADD CONSTRAINT invocation_binding_session_format
    CHECK (
        session_id IS NULL
        OR (
            session_id <> ''
            AND octet_length(session_id) <= 512
            AND session_id !~ E'[\\x00\\r\\n]'
        )
    );

ALTER TABLE execution_records
    ADD COLUMN IF NOT EXISTS attempt_number INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS execution_token VARCHAR(64) NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS retry_safe BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ NOT NULL DEFAULT (now() + INTERVAL '15 minutes');

-- Rows written by migration 017 predate fencing. Give them stable audit-only
-- tokens before enforcing the non-empty invariant. In-flight legacy Workers
-- must be drained before this migration; they cannot know these tokens.
UPDATE execution_records
SET execution_token='legacy:' || id::text
WHERE idempotency_key IS NOT NULL
  AND idempotency_key <> ''
  AND execution_token='';

ALTER TABLE execution_records
    ADD CONSTRAINT execution_attempt_number_positive
    CHECK (attempt_number > 0),
    ADD CONSTRAINT execution_retry_safe_status
    CHECK (status='FAILED' OR retry_safe=FALSE),
    ADD CONSTRAINT execution_lease_order
    CHECK (lease_until >= heartbeat_at);

ALTER TABLE execution_records
    ADD CONSTRAINT execution_token_byte_limit
    CHECK (
        octet_length(execution_token) <= 64
        AND (
            idempotency_key IS NULL
            OR idempotency_key=''
            OR execution_token <> ''
        )
    );

-- Migration 017 used a request-wide unique index. It is incompatible with
-- append-only recovery because FAILED/ABANDONED attempts must remain auditable.
DROP INDEX IF EXISTS uq_execution_request;
CREATE INDEX IF NOT EXISTS idx_execution_request_latest
    ON execution_records (tenant_id, idempotency_key, id DESC)
    WHERE idempotency_key IS NOT NULL AND idempotency_key <> '';

CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_request_attempt
    ON execution_records (tenant_id, idempotency_key, attempt_number)
    WHERE idempotency_key IS NOT NULL AND idempotency_key <> '';

CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_request_running
    ON execution_records (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL
      AND idempotency_key <> ''
      AND status='RUNNING';

CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_token
    ON execution_records (execution_token)
    WHERE execution_token <> '';

-- A cache row is evidence only when it names the exact execution that produced
-- it. The composite reference prevents cross-tenant producer substitution.
CREATE UNIQUE INDEX IF NOT EXISTS uq_execution_record_id_tenant
    ON execution_records (id, tenant_id);

ALTER TABLE invocation_results
    ADD COLUMN IF NOT EXISTS execution_id BIGINT;

UPDATE invocation_results r
SET execution_id=(
    SELECT e.id
    FROM execution_records e
    WHERE e.tenant_id=r.tenant_id
      AND e.idempotency_key=r.idempotency_key
      AND LOWER(COALESCE(e.payload_hash,''))=LOWER(r.payload_hash)
    ORDER BY e.id DESC
    LIMIT 1
)
WHERE r.execution_id IS NULL;

ALTER TABLE invocation_results
    ADD CONSTRAINT invocation_result_execution_positive
    CHECK (execution_id IS NULL OR execution_id > 0),
    ADD CONSTRAINT invocation_result_execution_tenant_fk
    FOREIGN KEY (execution_id, tenant_id)
    REFERENCES execution_records(id, tenant_id);

CREATE UNIQUE INDEX IF NOT EXISTS uq_invocation_result_execution
    ON invocation_results (execution_id)
    WHERE execution_id IS NOT NULL;
