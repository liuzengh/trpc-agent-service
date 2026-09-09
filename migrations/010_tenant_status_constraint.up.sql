DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'tenants_status_valid'
    ) THEN
        ALTER TABLE tenants
            ADD CONSTRAINT tenants_status_valid
            CHECK (status IN ('active','suspended','deleted'));
    END IF;
END $$;
