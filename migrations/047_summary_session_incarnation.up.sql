-- Legacy derived records cannot be assigned to a currently live Session after
-- a possible expiry/recreation. Preserve them under the empty legacy identity.
LOCK TABLE summary_jobs, summary_checkpoints IN ACCESS EXCLUSIVE MODE;

ALTER TABLE summary_jobs
    ADD COLUMN session_incarnation_id VARCHAR(36) NOT NULL DEFAULT '',
    DROP CONSTRAINT summary_jobs_scope_unique,
    ADD CONSTRAINT summary_jobs_scope_unique
        UNIQUE (tenant_id, agent_app_id, session_owner_id, session_id, filter_key, session_incarnation_id);

ALTER TABLE summary_checkpoints
    ADD COLUMN session_incarnation_id VARCHAR(36) NOT NULL DEFAULT '',
    DROP CONSTRAINT summary_checkpoints_pkey,
    ADD CONSTRAINT summary_checkpoints_pkey
        PRIMARY KEY (tenant_id, agent_app_id, session_owner_id, session_id, filter_key, session_incarnation_id);

-- Fence old unbound work without deleting its diagnostic record or checkpoint.
UPDATE summary_jobs
SET status='FAILED', attempts=max_attempts, lease_owner=NULL, lease_until=NULL,
    next_attempt_at=NULL, last_error='legacy Session incarnation is unbound', updated_at=now()
WHERE session_incarnation_id='' AND status<>'COMPLETED';

COMMENT ON COLUMN summary_jobs.session_incarnation_id IS
    'Session-owned platform:session_incarnation_id UUID; empty is preserved legacy data that production does not regenerate.';
COMMENT ON COLUMN summary_checkpoints.session_incarnation_id IS
    'Session generation UUID prevents a deleted or expired conversation from hydrating its replacement.';
