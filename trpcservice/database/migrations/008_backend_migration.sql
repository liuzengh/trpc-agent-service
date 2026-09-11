DROP INDEX IF EXISTS idx_backend_binding_app;
DROP INDEX IF EXISTS idx_backend_binding_tenant_default;

CREATE UNIQUE INDEX idx_backend_binding_active_app
    ON backend_binding(tenant_id, app_id, resource_type)
    WHERE app_id IS NOT NULL AND migration_state = 'active';

CREATE UNIQUE INDEX idx_backend_binding_active_tenant_default
    ON backend_binding(tenant_id, resource_type)
    WHERE app_id IS NULL AND migration_state = 'active';

CREATE TABLE backend_migration (
    migration_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    app_id VARCHAR(64),
    resource_type VARCHAR(64) NOT NULL,
    source_binding_id VARCHAR(64) NOT NULL REFERENCES backend_binding(binding_id),
    target_binding_id VARCHAR(64) NOT NULL REFERENCES backend_binding(binding_id),
    state VARCHAR(32) NOT NULL DEFAULT 'planned',
    checkpoint JSONB NOT NULL DEFAULT '{}'::jsonb,
    verification JSONB NOT NULL DEFAULT '{}'::jsonb,
    repair_backlog BIGINT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (tenant_id, app_id) REFERENCES agent_app(tenant_id, app_id),
    CHECK (source_binding_id <> target_binding_id)
);

CREATE UNIQUE INDEX idx_backend_migration_active
    ON backend_migration(tenant_id, COALESCE(app_id, ''), resource_type)
    WHERE state NOT IN ('completed', 'rolled_back', 'failed');
