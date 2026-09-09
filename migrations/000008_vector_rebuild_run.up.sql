CREATE TABLE vector_rebuild_run (
    tenant_id text NOT NULL,
    run_id text NOT NULL,
    projection_fingerprint text NOT NULL,
    mode text NOT NULL DEFAULT 'rebuild' CHECK (mode IN ('rebuild')),
    phase text NOT NULL DEFAULT 'pending'
        CHECK (phase IN ('pending', 'scanning', 'scanned', 'completed', 'failed', 'cancelled')),
    cursor_memory_id text,
    attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 100),
    scanned integer NOT NULL DEFAULT 0 CHECK (scanned >= 0),
    enqueued integer NOT NULL DEFAULT 0 CHECK (enqueued >= 0),
    tombstoned integer NOT NULL DEFAULT 0 CHECK (tombstoned >= 0),
    lease_owner text,
    lease_epoch bigint,
    lease_fence bigint,
    lease_expires_at timestamptz,
    last_error_category text CHECK (last_error_category IS NULL OR last_error_category IN (
        'cancelled', 'deadline', 'invalid_request', 'stale_task', 'unavailable',
        'retryable', 'permanent', 'unknown', 'lease_lost', 'fence_rejected'
    )),
    deadline_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz,
    PRIMARY KEY (tenant_id, run_id),
    FOREIGN KEY (tenant_id) REFERENCES tenant (tenant_id) ON DELETE CASCADE,
    CHECK (run_id ~ '^vrr-v1-[0-9a-f]{32}$'),
    CHECK (projection_fingerprint ~ '^[0-9a-f]{64}$'),
    CHECK (cursor_memory_id IS NULL OR length(cursor_memory_id) BETWEEN 1 AND 256),
    CHECK (
        phase <> 'scanning'
        OR (lease_owner IS NOT NULL AND length(lease_owner) <= 256
            AND lease_epoch IS NOT NULL AND lease_epoch >= 1
            AND lease_fence IS NOT NULL AND lease_fence >= 1
            AND lease_expires_at IS NOT NULL)
    ),
    CHECK (phase NOT IN ('pending', 'completed', 'failed', 'cancelled') OR lease_owner IS NULL),
    CHECK (phase <> 'completed' OR completed_at IS NOT NULL),
    CHECK (phase NOT IN ('completed', 'failed', 'cancelled') OR completed_at IS NOT NULL)
);

CREATE INDEX ix_vector_rebuild_ready
    ON vector_rebuild_run (updated_at, run_id)
    WHERE phase IN ('pending', 'scanning');

CREATE INDEX ix_vector_rebuild_lease_expiry
    ON vector_rebuild_run (lease_expires_at, run_id)
    WHERE phase = 'scanning';

CREATE INDEX ix_vector_rebuild_tenant
    ON vector_rebuild_run (tenant_id, phase, created_at);
