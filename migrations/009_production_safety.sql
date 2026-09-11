BEGIN;

-- Durable fail-closed content-safety decisions.  A pending row with a live
-- lease is not an allow decision; another node may reclaim it only after the
-- lease expires.  Raw content is intentionally absent from this table.
CREATE TABLE IF NOT EXISTS content_safety_decisions (
    decision_id bigserial PRIMARY KEY,
    tenant_id text NOT NULL CHECK (tenant_id <> ''),
    app_name text NOT NULL CHECK (app_name <> ''),
    session_id text NOT NULL CHECK (session_id <> ''),
    message_key text NOT NULL CHECK (message_key <> ''),
    config_revision text NOT NULL DEFAULT '',
    phase text NOT NULL CHECK (phase IN ('input', 'output')),
    policy_version text NOT NULL CHECK (policy_version <> ''),
    content_hash text NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    status text NOT NULL CHECK (status IN ('pending', 'allowed', 'blocked', 'unknown')),
    decision_hash text NOT NULL DEFAULT '',
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    lease_owner text NOT NULL DEFAULT '',
    lease_expires_at timestamptz,
    evaluated_at timestamptz,
    last_error_code text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (tenant_id, message_key, phase, policy_version)
);

CREATE INDEX IF NOT EXISTS content_safety_decisions_lease_idx
    ON content_safety_decisions (status, lease_expires_at, updated_at);

CREATE INDEX IF NOT EXISTS content_safety_decisions_tenant_idx
    ON content_safety_decisions (tenant_id, created_at DESC);

COMMIT;
