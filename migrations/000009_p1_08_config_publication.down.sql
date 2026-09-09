-- Symmetric rollback of the P1-08 configuration publication boundary.
-- The down migration fails closed when rows with extended publication
-- statuses exist; such durable state must not be silently destroyed.

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM tenant_config_version
        WHERE status IN ('validated', 'superseded', 'rejected')
    ) OR EXISTS (SELECT 1 FROM tenant_config_rollout)
      OR EXISTS (SELECT 1 FROM tenant_config_operation) THEN
        RAISE EXCEPTION 'P1-08 down migration refused: durable publication state exists';
    END IF;
END
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_tenant_config_revision_update ON tenant_config_version;
DROP TRIGGER IF EXISTS trg_tenant_config_revision_insert ON tenant_config_version;
DROP FUNCTION IF EXISTS tenant_config_revision_update_guard();
DROP FUNCTION IF EXISTS tenant_config_revision_insert_guard();

ALTER TABLE outbox_message DROP COLUMN IF EXISTS config_version;
ALTER TABLE execution_result DROP COLUMN IF EXISTS config_version;

DROP TABLE IF EXISTS tenant_config_operation;
DROP TABLE IF EXISTS tenant_config_rollout;

DROP INDEX IF EXISTS ix_tenant_config_status;
DROP INDEX IF EXISTS ux_tenant_config_active;

ALTER TABLE tenant_config_version
    DROP CONSTRAINT tenant_config_version_status_check;

ALTER TABLE tenant_config_version
    ADD CONSTRAINT tenant_config_version_status_check
    CHECK (status IN ('draft', 'published', 'recalled'));

ALTER TABLE tenant_config_version
    DROP COLUMN IF EXISTS reason_category,
    DROP COLUMN IF EXISTS recalled_at,
    DROP COLUMN IF EXISTS superseded_at,
    DROP COLUMN IF EXISTS rejected_at,
    DROP COLUMN IF EXISTS validated_at;
