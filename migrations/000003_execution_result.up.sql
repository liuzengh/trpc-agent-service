CREATE TABLE execution_result (
    tenant_id text NOT NULL,
    execution_id text NOT NULL,
    job_id text NOT NULL,
    session_id text NOT NULL,
    owner_id text NOT NULL,
    epoch bigint NOT NULL CHECK (epoch >= 1),
    fence_token bigint NOT NULL CHECK (fence_token >= 1),
    status text NOT NULL DEFAULT 'succeeded' CHECK (status = 'succeeded'),
    result_version bigint NOT NULL DEFAULT 1 CHECK (result_version >= 1),
    result_json jsonb NOT NULL,
    committed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, execution_id),
    UNIQUE (tenant_id, job_id),
    FOREIGN KEY (tenant_id, session_id)
    REFERENCES session (tenant_id, session_id) ON DELETE CASCADE
);
CREATE INDEX ix_execution_result_session ON execution_result (
    tenant_id, session_id, committed_at
);
