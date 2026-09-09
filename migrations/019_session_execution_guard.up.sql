-- Serialize execution admission for one tenant/app/session independently of
-- request idempotency. Lease expiry never grants a new owner automatically:
-- an uncertain terminal outcome remains BLOCKED until audited reconciliation.
CREATE TABLE IF NOT EXISTS session_execution_guards (
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id),
    agent_app_id VARCHAR(64) NOT NULL,
    session_id VARCHAR(512) NOT NULL,
    generation BIGINT NOT NULL DEFAULT 0 CHECK (generation >= 0),
    status VARCHAR(16) NOT NULL DEFAULT 'READY'
        CHECK (status IN ('READY','RUNNING','BLOCKED')),
    current_execution_id BIGINT,
    blocked_reason VARCHAR(64) NOT NULL DEFAULT '',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, agent_app_id, session_id),
    FOREIGN KEY (agent_app_id, tenant_id) REFERENCES agent_apps(id, tenant_id),
    FOREIGN KEY (current_execution_id, tenant_id)
        REFERENCES execution_records(id, tenant_id),
    CONSTRAINT session_execution_guard_shape CHECK (
        (status='READY' AND current_execution_id IS NULL AND blocked_reason='')
        OR
        (status='RUNNING' AND current_execution_id IS NOT NULL
            AND blocked_reason='' AND generation > 0)
        OR
        (status='BLOCKED' AND current_execution_id IS NOT NULL
            AND blocked_reason<>'' AND generation > 0)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_session_guard_current_execution
    ON session_execution_guards (current_execution_id)
    WHERE current_execution_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_session_guard_status
    ON session_execution_guards (status, updated_at)
    WHERE status <> 'READY';

CREATE TABLE IF NOT EXISTS execution_reconciliations (
    execution_id BIGINT PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL,
    decision VARCHAR(32) NOT NULL CHECK (decision='SAFE_TO_RETRY'),
    actor VARCHAR(256) NOT NULL CHECK (actor<>'' AND octet_length(actor)<=256),
    reason VARCHAR(512) NOT NULL CHECK (reason<>'' AND octet_length(reason)<=512),
    evidence VARCHAR(2048) NOT NULL DEFAULT '' CHECK (octet_length(evidence)<=2048),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (execution_id, tenant_id)
        REFERENCES execution_records(id, tenant_id)
);

CREATE INDEX IF NOT EXISTS idx_execution_reconciliation_tenant_time
    ON execution_reconciliations (tenant_id, created_at DESC);

-- Preserve unresolved pre-migration attempts. Choosing one current row does
-- not discard older uncertainty: reconciliation walks every unresolved row
-- before it can return the guard to READY.
WITH unresolved AS (
    SELECT DISTINCT ON (tenant_id, agent_app_id, session_id)
           tenant_id, agent_app_id, session_id, id, status, attempt_number
    FROM execution_records
    WHERE idempotency_key IS NOT NULL
      AND idempotency_key <> ''
      AND (
          status IN ('RUNNING','ABANDONED')
          OR (status='FAILED' AND retry_safe=FALSE)
      )
    ORDER BY tenant_id, agent_app_id, session_id,
             CASE WHEN status='RUNNING' THEN 0 ELSE 1 END,
             id DESC
)
INSERT INTO session_execution_guards (
    tenant_id, agent_app_id, session_id, generation, status,
    current_execution_id, blocked_reason
)
SELECT tenant_id, agent_app_id, session_id,
       GREATEST(attempt_number::BIGINT, 1),
       CASE WHEN status='RUNNING' THEN 'RUNNING' ELSE 'BLOCKED' END,
       id,
       CASE WHEN status='RUNNING' THEN '' ELSE 'legacy_unresolved_execution' END
FROM unresolved
ON CONFLICT (tenant_id, agent_app_id, session_id) DO NOTHING;

CREATE OR REPLACE FUNCTION trpc_sync_session_execution_guard()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    next_status VARCHAR(16);
    next_reason VARCHAR(64);
BEGIN
    IF OLD.status <> 'RUNNING'
       OR NEW.status = 'RUNNING'
       OR NEW.idempotency_key IS NULL
       OR NEW.idempotency_key = '' THEN
        RETURN NEW;
    END IF;

    IF NEW.status = 'SUCCEEDED'
       OR (NEW.status = 'FAILED' AND NEW.retry_safe = TRUE) THEN
        next_status := 'READY';
        next_reason := '';
    ELSIF NEW.status = 'ABANDONED' THEN
        next_status := 'BLOCKED';
        next_reason := 'expired_execution_lease';
    ELSIF NEW.status = 'FAILED' THEN
        next_status := 'BLOCKED';
        next_reason := 'execution_outcome_uncertain';
    ELSE
        RAISE EXCEPTION 'unsupported guarded execution transition: % -> %',
            OLD.status, NEW.status USING ERRCODE='23514';
    END IF;

    UPDATE session_execution_guards
    SET status=next_status,
        current_execution_id=CASE WHEN next_status='READY' THEN NULL ELSE NEW.id END,
        blocked_reason=next_reason,
        updated_at=now()
    WHERE tenant_id=NEW.tenant_id
      AND agent_app_id=NEW.agent_app_id
      AND session_id=NEW.session_id
      AND status='RUNNING'
      AND current_execution_id=NEW.id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'session execution guard mismatch for execution %', NEW.id
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS execution_record_sync_session_guard ON execution_records;
CREATE TRIGGER execution_record_sync_session_guard
AFTER UPDATE OF status ON execution_records
FOR EACH ROW
EXECUTE FUNCTION trpc_sync_session_execution_guard();
