-- Identity conflicts remain terminal even if the implicated publication has not
-- arrived yet. Manifest IDs are global; deployment revisions are tenant scoped.
CREATE TABLE manifest_conflict_identities (
 identity_kind text NOT NULL CHECK (identity_kind IN ('MANIFEST', 'REVISION')),
 tenant_id text NOT NULL,
 identity_value text NOT NULL CHECK (identity_value <> ''),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(identity_kind,tenant_id,identity_value),
 CHECK ((identity_kind = 'MANIFEST' AND tenant_id = '') OR
        (identity_kind = 'REVISION' AND tenant_id <> ''))
);

INSERT INTO manifest_conflict_identities(identity_kind,tenant_id,identity_value)
SELECT 'MANIFEST','',manifest_id FROM runtime_manifests WHERE conflicted
UNION ALL
SELECT 'REVISION',tenant_id,deployment_revision_id FROM runtime_manifests WHERE conflicted;
