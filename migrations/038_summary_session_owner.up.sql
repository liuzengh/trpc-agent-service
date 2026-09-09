-- Summary state is derived from the authoritative Session event stream. V13
-- did not persist the Session user/owner dimension and therefore cannot safely
-- map existing rows to a tRPC Session key. Purge only this rebuildable derived
-- state before re-keying; source events remain untouched.
LOCK TABLE summary_checkpoints, summary_jobs IN ACCESS EXCLUSIVE MODE;
DELETE FROM summary_checkpoints;
DELETE FROM summary_jobs;

ALTER TABLE summary_jobs
    ADD COLUMN session_owner_id VARCHAR(255) NOT NULL;

DO $$
DECLARE constraint_name TEXT;
BEGIN
    SELECT conname INTO constraint_name
    FROM pg_constraint
    WHERE conrelid = 'summary_jobs'::regclass AND contype = 'u'
    LIMIT 1;
    IF constraint_name IS NOT NULL THEN
        EXECUTE format('ALTER TABLE summary_jobs DROP CONSTRAINT %I', constraint_name);
    END IF;
END $$;

ALTER TABLE summary_jobs
    ADD CONSTRAINT summary_jobs_scope_unique
    UNIQUE (tenant_id, agent_app_id, session_owner_id, session_id, filter_key);

ALTER TABLE summary_checkpoints
    ADD COLUMN session_owner_id VARCHAR(255) NOT NULL,
    DROP CONSTRAINT summary_checkpoints_pkey,
    ADD CONSTRAINT summary_checkpoints_pkey
    PRIMARY KEY (tenant_id, agent_app_id, session_owner_id, session_id, filter_key);

COMMENT ON COLUMN summary_jobs.session_owner_id IS
    'Exact tRPC Session user ID; required to distinguish users sharing an IM conversation/session ID.';

