CREATE TABLE job_queue (
    tenant_id text NOT NULL,
    job_id text NOT NULL,
    execution_id text NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version >= 1),
    payload jsonb NOT NULL,
    status text NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'in_flight', 'acked', 'discarded')),
    available_at timestamptz NOT NULL DEFAULT now(),
    delivery_id text,
    last_delivery_id text,
    leased_until timestamptz,
    attempt integer NOT NULL DEFAULT 1 CHECK (attempt >= 1),
    delivery_count integer NOT NULL DEFAULT 0 CHECK (delivery_count >= 0),
    acked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, job_id),
    UNIQUE (tenant_id, execution_id),
    UNIQUE (delivery_id),
    FOREIGN KEY (tenant_id) REFERENCES tenant (tenant_id) ON DELETE CASCADE,
    CHECK (
        (status = 'in_flight' AND delivery_id IS NOT NULL AND leased_until IS NOT NULL)
        OR (status <> 'in_flight' AND delivery_id IS NULL AND leased_until IS NULL)
    ),
    CHECK (status = 'acked' OR acked_at IS NULL)
);

CREATE INDEX ix_job_queue_ready
    ON job_queue (available_at, created_at, job_id)
    WHERE status = 'queued';

CREATE INDEX ix_job_queue_expiry
    ON job_queue (leased_until)
    WHERE status = 'in_flight';
