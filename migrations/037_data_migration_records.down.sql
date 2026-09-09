DO $$
BEGIN
    IF to_regclass('data_migration_records') IS NOT NULL
       AND EXISTS (SELECT 1 FROM data_migration_records LIMIT 1) THEN
        RAISE EXCEPTION 'refusing to drop non-empty data_migration_records; archive or drain target data first';
    END IF;
END $$;
DROP TABLE IF EXISTS data_migration_records;
