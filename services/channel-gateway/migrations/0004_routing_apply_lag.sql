-- One continuous backlog episode spans observations, partial progress and
-- process restarts. Only catching the entire known watermark clears it.
ALTER TABLE gateway_route_replay_state
 ADD COLUMN apply_lag_since timestamptz;

-- Old projections have no trustworthy episode start. Upgrade starts the first
-- bounded grace period at the database clock, not at a producer timestamp.
UPDATE gateway_route_replay_state
 SET apply_lag_since = clock_timestamp()
 WHERE highest_sequence > contiguous_sequence;
