DROP TABLE session_lease;
DROP TABLE coordination_epoch;
ALTER TABLE message_dedup DROP COLUMN epoch;
