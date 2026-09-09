-- Keep projection completion markers scoped to one migration run. A record
-- projected to target B must not suppress the first projection to target C.
ALTER TABLE data_migration_records
    ADD COLUMN IF NOT EXISTS migration_id TEXT;

UPDATE data_migration_records
SET migration_id = 'legacy'
WHERE migration_id IS NULL;

ALTER TABLE data_migration_records
    ALTER COLUMN migration_id SET DEFAULT 'legacy',
    ALTER COLUMN migration_id SET NOT NULL;

ALTER TABLE data_migration_records
    DROP CONSTRAINT IF EXISTS data_migration_records_pkey;

ALTER TABLE data_migration_records
    ADD CONSTRAINT data_migration_records_pkey
    PRIMARY KEY (tenant_id, domain, record_key, migration_id);

CREATE INDEX IF NOT EXISTS idx_data_migration_records_migration
    ON data_migration_records (tenant_id, domain, migration_id, record_key);
