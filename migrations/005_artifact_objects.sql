BEGIN;

-- Artifact metadata may live in a dedicated PostgreSQL database that does not
-- contain the control-plane 001 schema, so keep its two helper functions local
-- to this append-only migration.
CREATE OR REPLACE FUNCTION platform_touch_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = clock_timestamp();
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION platform_current_tenant_id()
RETURNS text
LANGUAGE sql
STABLE
AS $$
    SELECT NULLIF(current_setting('app.tenant_id', true), '')
$$;

-- Object bytes and relational metadata cannot share a transaction.  This
-- table is therefore an explicit recovery ledger, not just an index over S3.
-- A revision is visible to readers only after it reaches ready.
CREATE TABLE IF NOT EXISTS artifact_blob_versions (
    tenant_id text NOT NULL CHECK (tenant_id <> ''),
    app_name text NOT NULL CHECK (app_name <> ''),
    user_id text NOT NULL CHECK (user_id <> ''),
    session_id text NOT NULL,
    filename text NOT NULL CHECK (filename <> ''),
    revision integer NOT NULL CHECK (revision >= 0),
    object_key text NOT NULL CHECK (object_key <> ''),
    media_type text NOT NULL DEFAULT 'application/octet-stream',
    size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
    content_sha256 text NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    state text NOT NULL CHECK (state IN (
        'upload_pending', 'ready', 'upload_failed', 'delete_pending', 'deleted'
    )),
    last_error_type text NOT NULL DEFAULT '',
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, app_name, user_id, session_id, filename, revision),
    UNIQUE (tenant_id, object_key),
    CHECK (session_id <> '' OR filename LIKE 'user:%')
);

CREATE INDEX IF NOT EXISTS artifact_blob_versions_ready_lookup_idx
    ON artifact_blob_versions
        (tenant_id, app_name, user_id, session_id, filename, revision DESC)
    WHERE state = 'ready';

CREATE INDEX IF NOT EXISTS artifact_blob_versions_recovery_idx
    ON artifact_blob_versions (tenant_id, state, updated_at, object_key)
    WHERE state IN ('upload_pending', 'upload_failed', 'delete_pending');

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'artifact_blob_versions_touch_updated_at'
          AND tgrelid = 'artifact_blob_versions'::regclass
    ) THEN
        CREATE TRIGGER artifact_blob_versions_touch_updated_at
        BEFORE UPDATE ON artifact_blob_versions
        FOR EACH ROW EXECUTE FUNCTION platform_touch_updated_at();
    END IF;
END;
$$;

ALTER TABLE artifact_blob_versions ENABLE ROW LEVEL SECURITY;
ALTER TABLE artifact_blob_versions FORCE ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE schemaname = current_schema()
          AND tablename = 'artifact_blob_versions'
          AND policyname = 'tenant_isolation'
    ) THEN
        CREATE POLICY tenant_isolation ON artifact_blob_versions
            USING (tenant_id = platform_current_tenant_id())
            WITH CHECK (tenant_id = platform_current_tenant_id());
    END IF;
END;
$$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'tenant_agent_runtime') THEN
        EXECUTE 'GRANT SELECT, INSERT, UPDATE, DELETE ON artifact_blob_versions TO tenant_agent_runtime';
    END IF;
END;
$$;

COMMIT;
