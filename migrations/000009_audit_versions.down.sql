DROP INDEX IF EXISTS idx_audit_config_version;
ALTER TABLE audit_logs DROP COLUMN IF EXISTS policy_version;
ALTER TABLE audit_logs DROP COLUMN IF EXISTS config_version;
