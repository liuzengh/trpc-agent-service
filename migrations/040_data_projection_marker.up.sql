-- A copied opaque record is not necessarily visible in its provider-native
-- data plane. This marker is committed only after the idempotent external
-- projector succeeds and the migration lease is revalidated.
ALTER TABLE data_migration_records
    ADD COLUMN IF NOT EXISTS projected_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_data_migration_records_unprojected
    ON data_migration_records (tenant_id, domain, version, record_key)
    WHERE projected_at IS NULL;

