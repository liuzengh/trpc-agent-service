-- Existing installations may already have applied migration 001 before
-- config_version was introduced. Keep this compatibility migration even
-- though fresh installations receive the column from migration 001.
ALTER TABLE tenants
    ADD COLUMN IF NOT EXISTS config_version BIGINT NOT NULL DEFAULT 1;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'tenants_config_version_positive'
    ) THEN
        ALTER TABLE tenants
            ADD CONSTRAINT tenants_config_version_positive
            CHECK (config_version > 0);
    END IF;
END $$;
