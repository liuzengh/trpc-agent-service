ALTER TABLE migration_jobs ADD COLUMN IF NOT EXISTS config_version BIGINT;
ALTER TABLE migration_jobs ADD COLUMN IF NOT EXISTS source_route_hash TEXT;
ALTER TABLE migration_jobs ADD COLUMN IF NOT EXISTS source_rows BIGINT NOT NULL DEFAULT 0;
ALTER TABLE migration_jobs ADD COLUMN IF NOT EXISTS copied_rows BIGINT NOT NULL DEFAULT 0;
ALTER TABLE migration_jobs ADD COLUMN IF NOT EXISTS attempts INT NOT NULL DEFAULT 0;
ALTER TABLE migration_jobs ADD COLUMN IF NOT EXISTS claim_token TEXT;
ALTER TABLE migration_jobs ADD COLUMN IF NOT EXISTS retry_at TIMESTAMPTZ;
ALTER TABLE migration_jobs ADD COLUMN IF NOT EXISTS last_error_type TEXT;
ALTER TABLE migration_jobs ADD COLUMN IF NOT EXISTS created_by TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE migration_jobs ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ;
ALTER TABLE migration_jobs ADD COLUMN IF NOT EXISTS completed_at TIMESTAMPTZ;

CREATE UNIQUE INDEX IF NOT EXISTS idx_migration_jobs_config_domain
  ON migration_jobs (tenant_id, app_id, config_version, domain)
  WHERE config_version IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_migration_jobs_claim
  ON migration_jobs (status, retry_at, lease_until, created_at);

-- Destination writes and ledger insertion share one transaction. If updating
-- the control-plane checkpoint fails, replay observes this ledger and does not
-- insert the same source row twice.
CREATE TABLE IF NOT EXISTS storage_migration_items (
  source_route_hash TEXT NOT NULL,
  table_name TEXT NOT NULL,
  source_key TEXT NOT NULL,
  checksum TEXT NOT NULL,
  copied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (source_route_hash, table_name, source_key)
);
