CREATE TABLE IF NOT EXISTS control_plane_audit (
    id BIGSERIAL PRIMARY KEY,
    tenant_id VARCHAR(64) NOT NULL REFERENCES tenants(id),
    actor VARCHAR(256) NOT NULL,
    action VARCHAR(64) NOT NULL,
    resource_type VARCHAR(64) NOT NULL,
    resource_id VARCHAR(128) NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_control_plane_audit_tenant_time
    ON control_plane_audit (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_control_plane_audit_resource
    ON control_plane_audit (tenant_id, resource_type, resource_id, created_at DESC);
