-- Discover only retained nonterminal requests with admission authorization.
-- Grouping is bounded in the caller; this index avoids scanning legacy/history.
CREATE INDEX execution_authorization_refresh_targets ON execution_runs
 ((request_json->'Authorization'->>'scope_id'),status)
 WHERE request_json->'Authorization'->>'scope_id' IS NOT NULL
 AND status IN ('QUEUED','RUNNING','RETRY_WAIT');
