-- Jobs using deferred targets or immutable version pins cannot be represented
-- by the legacy schema. They are derived from Session events and are rebuilt.
LOCK TABLE summary_jobs IN ACCESS EXCLUSIVE MODE;
DELETE FROM summary_jobs;

ALTER TABLE summary_jobs
    DROP CONSTRAINT IF EXISTS summary_jobs_agent_version_fk,
    DROP CONSTRAINT IF EXISTS summary_jobs_target_event_sequence_check,
    DROP COLUMN agent_version_id,
    ADD CONSTRAINT summary_jobs_target_event_sequence_check
        CHECK (target_event_sequence > 0);
