-- Summary jobs are derived coordination records. Existing rows predate an
-- immutable Agent version pin and cannot safely choose a model configuration,
-- so discard only those rebuildable requests before tightening the contract.
LOCK TABLE summary_jobs IN ACCESS EXCLUSIVE MODE;
DELETE FROM summary_jobs;

ALTER TABLE summary_jobs
    DROP CONSTRAINT IF EXISTS summary_jobs_target_event_sequence_check,
    ADD COLUMN agent_version_id VARCHAR(64) NOT NULL,
    ADD CONSTRAINT summary_jobs_agent_version_fk
        FOREIGN KEY (agent_version_id, agent_app_id)
        REFERENCES agent_versions(id, agent_app_id),
    ADD CONSTRAINT summary_jobs_target_event_sequence_check
        CHECK (target_event_sequence >= 0);

COMMENT ON COLUMN summary_jobs.agent_version_id IS
    'Immutable Agent version used to build the production summary generator.';
COMMENT ON COLUMN summary_jobs.target_event_sequence IS
    'Exact committed Session event prefix; zero is a leased deferred-resolution marker.';
