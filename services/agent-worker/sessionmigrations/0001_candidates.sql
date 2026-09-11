CREATE TABLE runtime_session.session_candidates (
    candidate_ref text PRIMARY KEY CHECK (candidate_ref ~ '^sc1_[0-9a-f]{64}$'),
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    run_id text NOT NULL,
    attempt_id text NOT NULL,
    parent_ref text NOT NULL,
    parent_digest text NOT NULL,
    content_version text NOT NULL CHECK (content_version = 'worker-session-v1'),
    content_digest text NOT NULL CHECK (content_digest ~ '^sha256:[0-9a-f]{64}$'),
    content bytea NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (tenant_id, session_id, run_id, attempt_id),
    CHECK ((parent_ref = '' AND parent_digest = '') OR
           (parent_ref ~ '^sc1_[0-9a-f]{64}$' AND parent_digest ~ '^sha256:[0-9a-f]{64}$'))
);
REVOKE ALL ON runtime_session.session_candidates FROM PUBLIC, session_runtime;
GRANT SELECT, INSERT ON runtime_session.session_candidates TO session_runtime;
