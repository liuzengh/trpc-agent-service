-- Additive approval invariants. Keep migration 028 immutable because the
-- migration runner enforces checksums for already-applied versions.
ALTER TABLE tool_approvals
    ADD CONSTRAINT tool_approvals_consumed_requires_grant
    CHECK (consumed_at IS NULL OR granted_at IS NOT NULL);
