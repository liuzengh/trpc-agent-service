ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS config_version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS policy_version BIGINT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_audit_config_version
  ON audit_logs (tenant_id, config_version, created_at);
