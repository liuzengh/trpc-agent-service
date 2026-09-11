CREATE TABLE runtime_manifests (
 tenant_id text NOT NULL,
 manifest_id text NOT NULL UNIQUE,
 deployment_revision_id text NOT NULL,
 content_digest text NOT NULL,
 envelope_digest text NOT NULL,
 envelope bytea NOT NULL,
 conflicted boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(tenant_id,manifest_id),
 UNIQUE(tenant_id,deployment_revision_id)
);
CREATE TABLE manifest_receipts (
 event_id text PRIMARY KEY,
 event_digest text NOT NULL,
 tenant_id text NOT NULL,
 manifest_id text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 FOREIGN KEY(tenant_id,manifest_id) REFERENCES runtime_manifests(tenant_id,manifest_id)
);
CREATE TABLE manifest_conflicts (
 conflict_id text PRIMARY KEY,
 event_id text NOT NULL,
 digest text NOT NULL,
 reason text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
