BEGIN;

CREATE TABLE IF NOT EXISTS data_migrations (
    migration_id text PRIMARY KEY CHECK (migration_id <> ''),
    tenant_id text NOT NULL CHECK (tenant_id <> ''),
    app_name text NOT NULL CHECK (app_name <> ''),
    resource_kind text NOT NULL
        CHECK (resource_kind IN ('session', 'memory', 'artifact', 'knowledge')),
    source_backend text NOT NULL CHECK (source_backend <> ''),
    target_backend text NOT NULL CHECK (target_backend <> ''),
    phase text NOT NULL
        CHECK (phase IN (
            'prepare', 'snapshot', 'catch_up', 'shadow_read', 'canary',
            'cutover', 'drain', 'finalize', 'rollback', 'complete'
        )),
    status text NOT NULL
        CHECK (status IN ('pending', 'running', 'paused', 'failed', 'complete', 'rolled_back')),
    generation bigint NOT NULL DEFAULT 0 CHECK (generation >= 0),
    checkpoint jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(checkpoint) = 'object'),
    copied_count bigint NOT NULL DEFAULT 0 CHECK (copied_count >= 0),
    verified_count bigint NOT NULL DEFAULT 0 CHECK (verified_count >= 0),
    failed_count bigint NOT NULL DEFAULT 0 CHECK (failed_count >= 0),
    mismatch_count bigint NOT NULL DEFAULT 0 CHECK (mismatch_count >= 0),
    last_error_type text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS data_migrations_tenant_status_idx
    ON data_migrations (tenant_id, status, updated_at);

CREATE TABLE IF NOT EXISTS data_migration_objects (
    migration_id text NOT NULL REFERENCES data_migrations (migration_id) ON DELETE CASCADE,
    object_key text NOT NULL,
    source_version text NOT NULL DEFAULT '',
    content_sha256 text NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    target_sha256 text NOT NULL DEFAULT ''
        CHECK (target_sha256 = '' OR target_sha256 ~ '^[0-9a-f]{64}$'),
    status text NOT NULL
        CHECK (status IN ('pending', 'copied', 'verified', 'mismatch', 'tombstoned', 'failed')),
    tombstone boolean NOT NULL DEFAULT false,
    copied_at timestamptz,
    verified_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (migration_id, object_key)
);

CREATE INDEX IF NOT EXISTS data_migration_objects_status_idx
    ON data_migration_objects (migration_id, status, object_key);

COMMIT;
