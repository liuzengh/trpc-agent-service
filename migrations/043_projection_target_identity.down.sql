DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM data_migration_records
        GROUP BY tenant_id, domain, record_key
        HAVING COUNT(DISTINCT migration_id) > 1
    ) THEN
        RAISE EXCEPTION 'refusing to remove projection target identity while multiple migration targets exist';
    END IF;
END $$;

DROP INDEX IF EXISTS idx_data_migration_records_migration;
ALTER TABLE data_migration_records
    DROP CONSTRAINT IF EXISTS data_migration_records_pkey;
ALTER TABLE data_migration_records
    DROP COLUMN IF EXISTS migration_id;
ALTER TABLE data_migration_records
    ADD CONSTRAINT data_migration_records_pkey
    PRIMARY KEY (tenant_id, domain, record_key);
