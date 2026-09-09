CREATE TABLE vector_projection_task (
    tenant_id text NOT NULL,
    task_id text NOT NULL,
    source_type text NOT NULL,
    source_id text NOT NULL,
    projection_scope text NOT NULL,
    document_id text NOT NULL,
    operation text NOT NULL CHECK (operation IN ('upsert', 'delete')),
    source_version bigint NOT NULL CHECK (source_version >= 1),
    source_sequence bigint NOT NULL CHECK (source_sequence >= 0),
    content_hash text NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    model text NOT NULL,
    model_version text NOT NULL,
    dimension integer NOT NULL CHECK (dimension BETWEEN 1 AND 4096),
    schema_version text NOT NULL,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'running', 'retry_wait', 'succeeded', 'dead_letter', 'stale', 'cancelled')),
    attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 100),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error_category text CHECK (last_error_category IS NULL OR last_error_category IN (
        'cancelled', 'deadline', 'invalid_request', 'stale_task',
        'schema_mismatch', 'dimension_mismatch', 'model_mismatch',
        'unavailable', 'retryable', 'permanent', 'unknown', 'lease_lost',
        'closed', 'fence_rejected', 'source_missing'
    )),
    lease_owner text,
    lease_epoch bigint,
    lease_fence bigint,
    lease_expires_at timestamptz,
    claimed_at timestamptz,
    completed_at timestamptz,
    dead_lettered_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, task_id),
    FOREIGN KEY (tenant_id) REFERENCES tenant (tenant_id) ON DELETE CASCADE,
    CHECK (task_id ~ '^vt-v1-[0-9a-f]{32}$'),
    CHECK (document_id ~ '^vd-v1-[0-9a-f]{64}$'),
    CHECK (length(btrim(source_type)) BETWEEN 1 AND 64),
    CHECK (length(source_id) BETWEEN 1 AND 256),
    CHECK (length(btrim(projection_scope)) BETWEEN 1 AND 128),
    CHECK (length(btrim(model)) BETWEEN 1 AND 256),
    CHECK (length(btrim(model_version)) BETWEEN 1 AND 128),
    CHECK (length(btrim(schema_version)) BETWEEN 1 AND 128),
    CHECK (attempt <= max_attempts),
    CHECK (
        status <> 'running'
        OR (lease_owner IS NOT NULL AND length(lease_owner) <= 256
            AND lease_epoch IS NOT NULL AND lease_epoch >= 1
            AND lease_fence IS NOT NULL AND lease_fence >= 1
            AND lease_expires_at IS NOT NULL)
    ),
    CHECK (status NOT IN ('pending', 'retry_wait') OR lease_owner IS NULL),
    CHECK (status <> 'succeeded' OR completed_at IS NOT NULL),
    CHECK (status <> 'dead_letter' OR (completed_at IS NOT NULL AND dead_lettered_at IS NOT NULL))
);

CREATE INDEX ix_vector_task_ready
    ON vector_projection_task (next_attempt_at, created_at, task_id)
    WHERE status IN ('pending', 'retry_wait');

CREATE INDEX ix_vector_task_lease_expiry
    ON vector_projection_task (lease_expires_at, task_id)
    WHERE status = 'running';

CREATE INDEX ix_vector_task_document_head
    ON vector_projection_task (tenant_id, document_id, source_version DESC, source_sequence DESC);

CREATE INDEX ix_vector_task_created
    ON vector_projection_task (created_at, tenant_id);
