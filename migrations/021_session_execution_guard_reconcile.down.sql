-- Do not remove the compatibility branch while a legacy non-owner expiry is
-- still present: restoring the strict trigger would make future status
-- maintenance fail and would hide unresolved execution history.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM execution_records e
        JOIN session_execution_guards g
          ON g.tenant_id=e.tenant_id
         AND g.agent_app_id=e.agent_app_id
         AND g.session_id=e.session_id
        WHERE e.status='ABANDONED'
          AND e.error_message='expired_execution_lease'
          AND NOT (
              g.status='BLOCKED'
              AND g.current_execution_id=e.id
          )
    ) THEN
        RAISE EXCEPTION
            'cannot roll back reconciliation trigger while legacy expiry rows are not current guard owners';
    END IF;
END
$$;

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
