DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM execution_reconciliations) THEN
        RAISE EXCEPTION
            'cannot roll back session guards while reconciliation audit history exists';
    END IF;
    IF EXISTS (
        SELECT 1 FROM session_execution_guards WHERE status <> 'READY'
    ) THEN
        RAISE EXCEPTION
            'cannot roll back session guards while an execution is running or blocked';
    END IF;
END
$$;

-- Perform every safety check before removing the trigger/function. If a
-- rollback is rejected, the live guard remains installed and continues to
-- protect admission and terminal execution transitions.
DROP TRIGGER IF EXISTS execution_record_sync_session_guard ON execution_records;
DROP FUNCTION IF EXISTS trpc_sync_session_execution_guard();

DROP TABLE IF EXISTS execution_reconciliations;
DROP TABLE IF EXISTS session_execution_guards;
