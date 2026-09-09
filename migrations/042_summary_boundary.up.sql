-- Summary content is useful to Runner only when it carries the exact event
-- boundary represented by the model input. Existing checkpoints predate that
-- contract and are derived from authoritative Session events, so rebuild them
-- instead of guessing a cutoff from summary generation time.
LOCK TABLE summary_checkpoints IN ACCESS EXCLUSIVE MODE;
DELETE FROM summary_checkpoints;

ALTER TABLE summary_checkpoints
    ADD COLUMN cutoff_at TIMESTAMPTZ NOT NULL,
    ADD COLUMN last_event_id VARCHAR(512) NOT NULL,
    ADD CONSTRAINT summary_checkpoint_last_event_id_nonempty
        CHECK (last_event_id <> ''),
    ADD CONSTRAINT summary_checkpoint_last_event_id_no_controls
        CHECK (last_event_id !~ '[[:cntrl:]]');

COMMENT ON COLUMN summary_checkpoints.cutoff_at IS
    'UTC timestamp of the last event in the exact filtered transcript summarized by the model.';
COMMENT ON COLUMN summary_checkpoints.last_event_id IS
    'Exact last covered event ID; disambiguates events sharing cutoff_at.';
