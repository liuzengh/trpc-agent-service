DROP INDEX IF EXISTS idx_data_migration_records_unprojected;

ALTER TABLE data_migration_records
    DROP COLUMN IF EXISTS projected_at;

