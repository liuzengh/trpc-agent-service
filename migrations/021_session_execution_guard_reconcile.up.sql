-- Reconciliation may encounter more than one stale RUNNING attempt for a
-- session left behind before migration 019. The reconciler normally
-- re-points the locked guard before each row, but keep the trigger idempotent
-- for a trusted expiry transition that arrives after the guard is already
-- BLOCKED. Ordinary worker terminal writes still fail closed on a mismatch.
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
        -- Only the expiry reconciler may drain a legacy non-owner row after
        -- another unresolved attempt has already blocked this session. A
        -- worker cannot reach this branch: it has no API that writes
        -- ABANDONED with the reconciler's sentinel error and its token/lease
        -- checks reject terminal writes after expiry.
        IF NEW.status = 'ABANDONED'
           AND NEW.error_message = 'expired_execution_lease'
           AND NEW.completed_at IS NOT NULL
           AND NEW.lease_until <= NEW.completed_at
           AND EXISTS (
               SELECT 1
               FROM session_execution_guards
               WHERE tenant_id=NEW.tenant_id
                 AND agent_app_id=NEW.agent_app_id
                 AND session_id=NEW.session_id
                 AND status='BLOCKED'
           ) THEN
            RETURN NEW;
        END IF;
        RAISE EXCEPTION 'session execution guard mismatch for execution %', NEW.id
            USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END
$$;
