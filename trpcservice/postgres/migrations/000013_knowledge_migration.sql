ALTER TABLE platform.data_migration
    ADD COLUMN domain TEXT NOT NULL DEFAULT 'SESSION'
        CHECK (domain IN ('SESSION', 'KNOWLEDGE'));

CREATE INDEX data_migration_domain_status_idx
    ON platform.data_migration (tenant_id, app_id, domain, status);
