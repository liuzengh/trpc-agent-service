-- P1-08 durable configuration publication boundary.
--
-- Facts:
--   tenant_config_version  gains bounded publication state transitions
--                          (draft -> validated -> published; published ->
--                          superseded|recalled; superseded|recalled ->
--                          published via rollback) while its content columns
--                          become frozen after insert. At most one revision
--                          per tenant may be 'published' (partial unique
--                          index). History is never deleted.
--   tenant_config_rollout  one row per managed tenant: the active revision,
--                          the baseline revision used by tenants outside the
--                          canary bucket, and the canary percentage.
--   tenant_config_operation durable idempotency record for publish, rollout
--                          and rollback operations (same (tenant_id,
--                          operation_id) always converges to the recorded
--                          outcome).
--   execution_result / outbox_message gain an additive nullable
--                          config_version column so completion and outbox
--                          facts retain the job's immutable config version.
--                          Old rows stay NULL and decode unchanged.
--
-- No secret plaintext, raw tenant data or external endpoint is inserted by
-- this migration.

ALTER TABLE tenant_config_version
    DROP CONSTRAINT tenant_config_version_status_check;

ALTER TABLE tenant_config_version
    ADD COLUMN validated_at timestamptz,
    ADD COLUMN rejected_at timestamptz,
    ADD COLUMN superseded_at timestamptz,
    ADD COLUMN recalled_at timestamptz,
    ADD COLUMN reason_category text;

ALTER TABLE tenant_config_version
    ADD CONSTRAINT tenant_config_version_status_check
    CHECK (status IN ('draft', 'validated', 'published', 'superseded', 'recalled', 'rejected'));

-- At most one active (published) revision per tenant.
CREATE UNIQUE INDEX ux_tenant_config_active
    ON tenant_config_version (tenant_id) WHERE status = 'published';

CREATE INDEX ix_tenant_config_status
    ON tenant_config_version (tenant_id, status);

CREATE TABLE tenant_config_rollout (
    tenant_id text PRIMARY KEY,
    active_version bigint NOT NULL,
    baseline_version bigint,
    percentage smallint NOT NULL DEFAULT 100 CHECK (percentage BETWEEN 0 AND 100),
    updated_at timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (tenant_id, active_version)
        REFERENCES tenant_config_version (tenant_id, config_version) ON DELETE RESTRICT,
    FOREIGN KEY (tenant_id, baseline_version)
        REFERENCES tenant_config_version (tenant_id, config_version) ON DELETE RESTRICT
);

CREATE TABLE tenant_config_operation (
    tenant_id text NOT NULL,
    operation_id text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('validate', 'publish', 'rollout', 'rollback')),
    target_version bigint NOT NULL CHECK (target_version >= 1),
    expected_active_version bigint NOT NULL CHECK (expected_active_version >= 0),
    requested_percentage smallint NOT NULL DEFAULT 100 CHECK (requested_percentage BETWEEN 0 AND 100),
    result_active_version bigint,
    outcome text NOT NULL CHECK (outcome IN ('committed', 'rejected', 'stale_version', 'conflict')),
    reason_category text,
    actor_category text NOT NULL DEFAULT 'operator',
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, operation_id),
    FOREIGN KEY (tenant_id) REFERENCES tenant (tenant_id) ON DELETE CASCADE
);

ALTER TABLE execution_result ADD COLUMN config_version bigint;
ALTER TABLE outbox_message ADD COLUMN config_version bigint;

CREATE FUNCTION tenant_config_revision_insert_guard() RETURNS trigger AS $$
BEGIN
    IF NEW.status <> 'draft' THEN
        RAISE EXCEPTION 'tenant_config_version revisions must start as draft';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_tenant_config_revision_insert
    BEFORE INSERT ON tenant_config_version
    FOR EACH ROW EXECUTE FUNCTION tenant_config_revision_insert_guard();

CREATE FUNCTION tenant_config_revision_update_guard() RETURNS trigger AS $$
BEGIN
    IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id
        OR NEW.config_version IS DISTINCT FROM OLD.config_version
        OR NEW.config IS DISTINCT FROM OLD.config
        OR NEW.checksum IS DISTINCT FROM OLD.checksum THEN
        RAISE EXCEPTION 'tenant_config_version revision content is immutable';
    END IF;
    IF NOT (
        (OLD.status = 'draft' AND NEW.status = 'validated')
        OR (OLD.status = 'draft' AND NEW.status = 'rejected')
        OR (OLD.status = 'validated' AND NEW.status = 'published')
        OR (OLD.status = 'published' AND NEW.status = 'superseded')
        OR (OLD.status = 'published' AND NEW.status = 'recalled')
        OR (OLD.status = 'superseded' AND NEW.status = 'published')
        OR (OLD.status = 'recalled' AND NEW.status = 'published')
    ) THEN
        RAISE EXCEPTION 'tenant_config_version invalid status transition % -> %', OLD.status, NEW.status;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_tenant_config_revision_update
    BEFORE UPDATE ON tenant_config_version
    FOR EACH ROW EXECUTE FUNCTION tenant_config_revision_update_guard();
