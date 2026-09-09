ALTER TABLE summary_jobs
    ADD COLUMN target_resolution_lease_version BIGINT NOT NULL DEFAULT 0
        CHECK (target_resolution_lease_version >= 0);

COMMENT ON COLUMN summary_jobs.target_resolution_lease_version IS
    'Nonzero requests complete-transcript resolution. Requests during a claim target the next lease and coalesce until it is claimed.';
