-- Park executions at a durable human-review boundary until an administrator
-- decides the approval. The status is deliberately distinct from PENDING so
-- recovery and dispatchers cannot execute it before the decision is persisted.

ALTER TABLE platform.execution
    DROP CONSTRAINT IF EXISTS execution_status_check;

ALTER TABLE platform.execution
    ADD CONSTRAINT execution_status_check CHECK (
        status IN ('PENDING', 'RUNNING', 'WAITING_APPROVAL', 'SUCCEEDED', 'FAILED', 'UNCERTAIN', 'CANCELED')
    );

DROP INDEX IF EXISTS platform.execution_active_lane_idx;

CREATE INDEX execution_active_lane_idx
    ON platform.execution (tenant_id, app_id, session_principal_id, session_id, turn_seq)
    WHERE status IN ('PENDING', 'RUNNING', 'WAITING_APPROVAL');
