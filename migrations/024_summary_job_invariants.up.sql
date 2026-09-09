-- Tighten invariants added after the initial Summary tables were released.
-- Keep this as a new migration so installations that already applied 023 do
-- not encounter checksum drift.
ALTER TABLE summary_jobs
    ADD CONSTRAINT summary_jobs_last_error_length
        CHECK (octet_length(last_error) <= 4096),
    ADD CONSTRAINT summary_jobs_completed_sequence_consistency
        CHECK ((status = 'COMPLETED' AND completed_event_sequence >= target_event_sequence)
            OR (status <> 'COMPLETED' AND completed_event_sequence <= target_event_sequence));
