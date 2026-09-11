CREATE TABLE resource_sync (
 tenant_id TEXT NOT NULL, app_id TEXT NOT NULL, resource_type TEXT NOT NULL CHECK(resource_type IN ('session','memory')),
 state JSONB NOT NULL DEFAULT '{}',updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 PRIMARY KEY(tenant_id,app_id,resource_type),
 FOREIGN KEY(tenant_id,app_id) REFERENCES agent_app(tenant_id,app_id)
);
