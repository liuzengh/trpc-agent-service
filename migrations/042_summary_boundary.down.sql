-- Checkpoints without a structural boundary are unsafe for Runner history
-- trimming. Purge the rebuildable derived rows before restoring the old shape.
LOCK TABLE summary_checkpoints IN ACCESS EXCLUSIVE MODE;
DELETE FROM summary_checkpoints;

ALTER TABLE summary_checkpoints
    DROP CONSTRAINT summary_checkpoint_last_event_id_no_controls,
    DROP CONSTRAINT summary_checkpoint_last_event_id_nonempty,
    DROP COLUMN last_event_id,
    DROP COLUMN cutoff_at;
