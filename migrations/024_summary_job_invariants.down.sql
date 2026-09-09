ALTER TABLE summary_jobs
    DROP CONSTRAINT IF EXISTS summary_jobs_completed_sequence_consistency,
    DROP CONSTRAINT IF EXISTS summary_jobs_last_error_length;
