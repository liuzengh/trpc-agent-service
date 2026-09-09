-- The catalog makes S3 artifacts enumerable without listing or logging object
-- keys. It stores only tenant scope, logical coordinates, version and checksum.
CREATE TABLE IF NOT EXISTS runtime_artifact_catalog (
  tenant_id TEXT NOT NULL,
  app_id TEXT NOT NULL,
  user_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  filename TEXT NOT NULL,
  revision INT NOT NULL,
  checksum TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (tenant_id, app_id, user_id, session_id, filename, revision),
  FOREIGN KEY (tenant_id, app_id) REFERENCES agent_apps(tenant_id, app_id)
);
CREATE INDEX IF NOT EXISTS idx_runtime_artifact_catalog_scope
  ON runtime_artifact_catalog (tenant_id, app_id, user_id, session_id, filename);

ALTER TABLE runtime_knowledge_documents
  ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT '';
