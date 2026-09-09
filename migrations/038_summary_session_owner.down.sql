-- Rollback also discards derived summaries because collapsing distinct owners
-- back into the legacy key is not information-preserving.
LOCK TABLE summary_checkpoints, summary_jobs IN ACCESS EXCLUSIVE MODE;
DELETE FROM summary_checkpoints;
DELETE FROM summary_jobs;

ALTER TABLE summary_checkpoints
    DROP CONSTRAINT summary_checkpoints_pkey,
    DROP COLUMN session_owner_id,
    ADD CONSTRAINT summary_checkpoints_pkey
    PRIMARY KEY (tenant_id, agent_app_id, session_id, filter_key);

ALTER TABLE summary_jobs
    DROP CONSTRAINT summary_jobs_scope_unique,
    DROP COLUMN session_owner_id,
    ADD CONSTRAINT summary_jobs_scope_unique
    UNIQUE (tenant_id, agent_app_id, session_id, filter_key);

