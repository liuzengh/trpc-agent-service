BEGIN;

-- Durable, tenant-scoped side-effect operation ledger. Raw tool arguments,
-- provider responses, error messages, and lease capabilities are deliberately
-- excluded; callers persist only SHA-256 digests and bounded safe labels.
CREATE TABLE IF NOT EXISTS tool_operations (
    tenant_id text NOT NULL,
    operation_key text NOT NULL,
    payload_hash text NOT NULL CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
    tool_name text NOT NULL,
    operation_class text NOT NULL DEFAULT '',
    target_hash text NOT NULL DEFAULT ''
        CHECK (target_hash = '' OR target_hash ~ '^[0-9a-f]{64}$'),
    state text NOT NULL CHECK (state IN (
        'reserved', 'executing', 'confirmed', 'retryable_not_applied',
        'permanent_rejected', 'unknown'
    )),
    state_version bigint NOT NULL DEFAULT 1 CHECK (state_version > 0),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    current_attempt integer NOT NULL DEFAULT 0 CHECK (current_attempt >= 0),
    lease_owner_hash text NOT NULL DEFAULT ''
        CHECK (lease_owner_hash = '' OR lease_owner_hash ~ '^[0-9a-f]{64}$'),
    lease_expires_at timestamptz,
    next_attempt_at timestamptz,
    outcome_code text NOT NULL DEFAULT '',
    result_hash text NOT NULL DEFAULT ''
        CHECK (result_hash = '' OR result_hash ~ '^[0-9a-f]{64}$'),
    replay_reference text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, operation_key),
    CHECK (octet_length(tenant_id) BETWEEN 1 AND 256),
    CHECK (octet_length(operation_key) BETWEEN 1 AND 256),
    CHECK (octet_length(tool_name) BETWEEN 1 AND 256),
    CHECK (octet_length(operation_class) <= 256),
    CHECK (octet_length(outcome_code) <= 256),
    CHECK (octet_length(replay_reference) <= 256),
    CHECK ((lease_owner_hash = '') = (lease_expires_at IS NULL)),
    CHECK ((state = 'confirmed') = (result_hash <> ''))
);

CREATE INDEX IF NOT EXISTS tool_operations_due_idx
    ON tool_operations (tenant_id, next_attempt_at, created_at, operation_key)
    WHERE state IN ('reserved', 'retryable_not_applied');
CREATE INDEX IF NOT EXISTS tool_operations_expired_lease_idx
    ON tool_operations (lease_expires_at)
    WHERE lease_expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS tool_operations_unknown_idx
    ON tool_operations (tenant_id, updated_at, operation_key)
    WHERE state = 'unknown';

CREATE TABLE IF NOT EXISTS tool_operation_attempts (
    tenant_id text NOT NULL,
    operation_key text NOT NULL,
    attempt_no integer NOT NULL CHECK (attempt_no > 0),
    owner_hash text NOT NULL CHECK (owner_hash ~ '^[0-9a-f]{64}$'),
    phase text NOT NULL CHECK (phase IN ('reserved', 'executing', 'finished')),
    outcome text NOT NULL DEFAULT '' CHECK (outcome IN (
        '', 'confirmed', 'retryable_not_applied', 'permanent_rejected', 'unknown'
    )),
    outcome_code text NOT NULL DEFAULT '',
    result_hash text NOT NULL DEFAULT ''
        CHECK (result_hash = '' OR result_hash ~ '^[0-9a-f]{64}$'),
    replay_reference text NOT NULL DEFAULT '',
    finish_fingerprint text NOT NULL DEFAULT ''
        CHECK (finish_fingerprint = '' OR finish_fingerprint ~ '^[0-9a-f]{64}$'),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    executing_at timestamptz,
    finished_at timestamptz,
    retry_at timestamptz,
    PRIMARY KEY (tenant_id, operation_key, attempt_no),
    FOREIGN KEY (tenant_id, operation_key)
        REFERENCES tool_operations (tenant_id, operation_key) ON DELETE RESTRICT,
    CHECK (octet_length(outcome_code) <= 256),
    CHECK (octet_length(replay_reference) <= 256)
);

CREATE TABLE IF NOT EXISTS tool_operation_resolutions (
    tenant_id text NOT NULL,
    resolution_id text NOT NULL,
    operation_key text NOT NULL,
    from_version bigint NOT NULL CHECK (from_version > 0),
    to_version bigint NOT NULL CHECK (to_version = from_version + 1),
    action text NOT NULL CHECK (action IN ('confirm', 'retry_not_applied', 'reject')),
    actor_hash text NOT NULL CHECK (actor_hash ~ '^[0-9a-f]{64}$'),
    reason_code text NOT NULL,
    result_hash text NOT NULL DEFAULT ''
        CHECK (result_hash = '' OR result_hash ~ '^[0-9a-f]{64}$'),
    replay_reference text NOT NULL DEFAULT '',
    requested_retry_at timestamptz,
    resolved_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, resolution_id),
    FOREIGN KEY (tenant_id, operation_key)
        REFERENCES tool_operations (tenant_id, operation_key) ON DELETE RESTRICT,
    CHECK (octet_length(resolution_id) BETWEEN 1 AND 256),
    CHECK (octet_length(reason_code) BETWEEN 1 AND 256),
    CHECK (octet_length(replay_reference) <= 256)
);

COMMIT;
