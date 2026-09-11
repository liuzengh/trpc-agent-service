BEGIN;

-- Durable, versioned session summaries.  A summary is a claim about an
-- exact event interval; the application validates the claim before writing or
-- serving it and these keys make retries idempotent.
CREATE TABLE IF NOT EXISTS session_turn_summaries (
	tenant_id text NOT NULL DEFAULT '',
    app_name text NOT NULL CHECK (app_name <> ''),
    user_id text NOT NULL CHECK (user_id <> ''),
    session_id text NOT NULL CHECK (session_id <> ''),
    filter_key text NOT NULL DEFAULT '',
    covered_from_sequence bigint NOT NULL CHECK (covered_from_sequence >= 0),
    covered_through_sequence bigint NOT NULL
        CHECK (covered_through_sequence >= covered_from_sequence),
    last_event_id text NOT NULL CHECK (last_event_id <> ''),
    session_version bigint NOT NULL CHECK (session_version >= 0),
    summary_version bigint NOT NULL CHECK (summary_version > 0),
    boundary_version integer NOT NULL CHECK (boundary_version > 0),
    generator_version text NOT NULL CHECK (generator_version <> ''),
    model_version text NOT NULL DEFAULT '',
    prompt_version text NOT NULL DEFAULT '',
    source_sha256 text NOT NULL CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
    summary_sha256 text NOT NULL CHECK (summary_sha256 ~ '^[0-9a-f]{64}$'),
    summary_text text NOT NULL,
    topics jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(topics) = 'array'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (
		tenant_id, app_name, user_id, session_id, filter_key,
        covered_from_sequence, covered_through_sequence, generator_version,
        summary_version
    )
);

CREATE INDEX IF NOT EXISTS session_turn_summaries_latest_idx
    ON session_turn_summaries
       (tenant_id, app_name, user_id, session_id, filter_key,
        covered_through_sequence DESC, session_version DESC, created_at DESC);

CREATE TABLE IF NOT EXISTS session_summary_jobs (
    job_id text PRIMARY KEY CHECK (job_id <> ''),
	tenant_id text NOT NULL DEFAULT '',
    app_name text NOT NULL CHECK (app_name <> ''),
    user_id text NOT NULL CHECK (user_id <> ''),
    session_id text NOT NULL CHECK (session_id <> ''),
    filter_key text NOT NULL DEFAULT '',
    requested_from_sequence bigint NOT NULL CHECK (requested_from_sequence >= 0),
    requested_through_sequence bigint NOT NULL
        CHECK (requested_through_sequence >= requested_from_sequence),
    session_version bigint NOT NULL CHECK (session_version >= 0),
    boundary_version integer NOT NULL CHECK (boundary_version > 0),
    last_event_id text NOT NULL CHECK (last_event_id <> ''),
    source_sha256 text NOT NULL CHECK (source_sha256 ~ '^[0-9a-f]{64}$'),
    generator_version text NOT NULL CHECK (generator_version <> ''),
    prompt_version text NOT NULL DEFAULT '',
    status text NOT NULL CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_owner text NOT NULL DEFAULT '',
    lease_expires_at timestamptz,
    next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_error text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (
		tenant_id, app_name, user_id, session_id, filter_key,
        requested_from_sequence, requested_through_sequence,
        generator_version, source_sha256
    )
);

CREATE INDEX IF NOT EXISTS session_summary_jobs_claim_idx
    ON session_summary_jobs (status, next_attempt_at, lease_expires_at, created_at);

-- A watermark is scoped to a principal rather than a process.  backend_epoch
-- lets readers distinguish a restarted backend from a stalled writer.
CREATE TABLE IF NOT EXISTS memory_visibility_watermarks (
    tenant_id text NOT NULL CHECK (tenant_id <> ''),
    app_name text NOT NULL CHECK (app_name <> ''),
    principal_id text NOT NULL CHECK (principal_id <> ''),
    backend_epoch text NOT NULL CHECK (backend_epoch <> ''),
    watermark bigint NOT NULL CHECK (watermark >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_error text NOT NULL DEFAULT '',
    PRIMARY KEY (tenant_id, app_name, principal_id)
);

-- Audit records are append-only.  The per-tenant head is locked by the SQL
-- sink while it assigns a sequence and extends the hash chain.
CREATE TABLE IF NOT EXISTS audit_tenant_heads (
    tenant_id text PRIMARY KEY CHECK (tenant_id <> ''),
    last_sequence bigint NOT NULL DEFAULT 0 CHECK (last_sequence >= 0),
    last_hash text NOT NULL DEFAULT '',
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS audit_records (
    audit_id text PRIMARY KEY CHECK (audit_id <> ''),
    tenant_id text NOT NULL CHECK (tenant_id <> ''),
    sequence bigint NOT NULL CHECK (sequence > 0),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    content_sha256 text NOT NULL CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    previous_hash text NOT NULL DEFAULT '',
    record_hash text NOT NULL CHECK (record_hash ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (tenant_id, sequence),
    UNIQUE (tenant_id, audit_id)
);

CREATE INDEX IF NOT EXISTS audit_records_tenant_sequence_idx
    ON audit_records (tenant_id, sequence);

CREATE OR REPLACE FUNCTION reject_audit_record_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'audit_records is append-only';
END;
$$;

DROP TRIGGER IF EXISTS audit_records_no_update ON audit_records;
CREATE TRIGGER audit_records_no_update
    BEFORE UPDATE OR DELETE ON audit_records
    FOR EACH ROW EXECUTE FUNCTION reject_audit_record_mutation();

-- Existing migration objects already carry source/target hashes.  These
-- append-only columns capture the session epoch/sequence guards and make
-- rollback conflicts auditable without changing migrations 001-007.
ALTER TABLE data_migration_objects
    ADD COLUMN IF NOT EXISTS source_session_epoch text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS source_through_sequence bigint NOT NULL DEFAULT 0
        CHECK (source_through_sequence >= 0),
    ADD COLUMN IF NOT EXISTS target_version text NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS rollback_state text NOT NULL DEFAULT 'eligible'
        CHECK (rollback_state IN ('eligible', 'deleted', 'conflict', 'retained'));

CREATE TABLE IF NOT EXISTS data_migration_reconciliations (
    migration_id text NOT NULL REFERENCES data_migrations (migration_id) ON DELETE CASCADE,
    object_key text NOT NULL,
    source_hash text NOT NULL DEFAULT ''
        CHECK (source_hash = '' OR source_hash ~ '^[0-9a-f]{64}$'),
    target_hash text NOT NULL DEFAULT ''
        CHECK (target_hash = '' OR target_hash ~ '^[0-9a-f]{64}$'),
    status text NOT NULL CHECK (status IN (
        'matched', 'mismatch', 'stale_source', 'rollback_deleted', 'rollback_conflict'
    )),
    detail text NOT NULL DEFAULT '',
    checked_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (migration_id, object_key)
);

CREATE INDEX IF NOT EXISTS data_migration_reconciliations_status_idx
    ON data_migration_reconciliations (migration_id, status, object_key);

COMMIT;
