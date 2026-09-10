ALTER TABLE platform.data_migration
    ADD COLUMN total_sessions BIGINT NOT NULL DEFAULT 0
        CHECK (total_sessions >= 0),
    ADD COLUMN copy_progress BIGINT NOT NULL DEFAULT 0
        CHECK (copy_progress >= 0 AND copy_progress <= total_sessions),
    ADD COLUMN verify_progress BIGINT NOT NULL DEFAULT 0
        CHECK (verify_progress >= 0 AND verify_progress <= total_sessions),
    ADD COLUMN success_count BIGINT NOT NULL DEFAULT 0
        CHECK (success_count >= 0 AND success_count <= total_sessions),
    ADD COLUMN last_checkpoint_at TIMESTAMPTZ,
    ADD COLUMN last_failure_stage TEXT NOT NULL DEFAULT '';
