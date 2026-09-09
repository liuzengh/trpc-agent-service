-- Automatic downgrade would merge distinct Session generations and expose old
-- conversation summaries. Require explicit operator handling of derived data.
LOCK TABLE summary_jobs, summary_checkpoints IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM summary_jobs WHERE session_incarnation_id<>'')
        OR EXISTS (SELECT 1 FROM summary_checkpoints WHERE session_incarnation_id<>'') THEN
        RAISE EXCEPTION 'cannot remove Session incarnation while bound summary records exist';
    END IF;
END $$;

ALTER TABLE summary_jobs
    DROP CONSTRAINT summary_jobs_scope_unique,
    DROP COLUMN session_incarnation_id,
    ADD CONSTRAINT summary_jobs_scope_unique
        UNIQUE (tenant_id, agent_app_id, session_owner_id, session_id, filter_key);

ALTER TABLE summary_checkpoints
    DROP CONSTRAINT summary_checkpoints_pkey,
    DROP COLUMN session_incarnation_id,
    ADD CONSTRAINT summary_checkpoints_pkey
        PRIMARY KEY (tenant_id, agent_app_id, session_owner_id, session_id, filter_key);
