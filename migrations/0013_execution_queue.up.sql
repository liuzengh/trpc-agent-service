-- Issue #76: durable tenant-scoped execution queue.
SET LOCAL search_path = pg_catalog, public, pg_temp;

CREATE TABLE public.runtime_execution_queue (
    tenant_id        TEXT NOT NULL,
    task_id          TEXT NOT NULL CHECK (length(btrim(task_id)) BETWEEN 1 AND 256),
    kind             TEXT NOT NULL CHECK (length(btrim(kind)) BETWEEN 1 AND 128),
    payload          BYTEA NOT NULL CHECK (octet_length(payload) > 0),
    status           TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','leased','retryable','completed','failed')),
    attempts         INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    fencing_token    BIGINT NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    lease_owner      TEXT NOT NULL DEFAULT '',
    lease_expires_at TIMESTAMPTZ,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error_class TEXT NOT NULL DEFAULT '' CHECK (length(last_error_class) <= 128),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, task_id),
    FOREIGN KEY (tenant_id) REFERENCES public.tenant(tenant_id)
);

CREATE INDEX runtime_execution_queue_ready_idx
    ON public.runtime_execution_queue (tenant_id, next_attempt_at, created_at)
    WHERE status IN ('queued','retryable');
CREATE INDEX runtime_execution_queue_lease_idx
    ON public.runtime_execution_queue (tenant_id, lease_expires_at)
    WHERE status = 'leased';

REVOKE ALL ON TABLE public.runtime_execution_queue FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE ON public.runtime_execution_queue TO tenant_app_writer;
GRANT ALL PRIVILEGES ON public.runtime_execution_queue TO migration_owner;
