-- One coordination record per tenant/app. Pending intents survive a vector
-- backend outage; migration proof and cursor are invalidated by every write.
CREATE TABLE knowledge_sync (
 tenant_id TEXT NOT NULL,
 app_id TEXT NOT NULL,
 state JSONB NOT NULL DEFAULT '{"epoch":0,"intents":{},"migrations":{}}',
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 PRIMARY KEY(tenant_id,app_id),
 FOREIGN KEY(tenant_id,app_id) REFERENCES agent_app(tenant_id,app_id)
);
