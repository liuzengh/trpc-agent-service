-- Completed Agent results are durable facts, not Redis cache entries. This
-- receipt covers post-completion audit/accounting/job submission before ACK.
-- Do not backfill old runs: their bookkeeping may have been interrupted.
ALTER TABLE agent_run ADD COLUMN finalized_at TIMESTAMPTZ;
