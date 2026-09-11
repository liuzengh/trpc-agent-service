BEGIN;

-- The configuration control plane is deliberately separate from the logical
-- 001 design document.  The executable migration chain currently starts at
-- 002, so this migration owns the durable revision/release/node state used by
-- the running service.

CREATE TABLE IF NOT EXISTS config_tenant_revisions (
    tenant_id text NOT NULL,
    revision text NOT NULL CHECK (revision ~ '^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$'),
    config jsonb NOT NULL CHECK (jsonb_typeof(config) = 'object'),
    config_sha256 text NOT NULL CHECK (config_sha256 ~ '^[0-9a-f]{64}$'),
    created_by text NOT NULL DEFAULT '',
    change_reason text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (tenant_id, revision)
);

CREATE TABLE IF NOT EXISTS config_tenant_state (
    tenant_id text PRIMARY KEY,
    active_revision text NOT NULL,
    canary_revision text,
    rollout_percent smallint NOT NULL DEFAULT 0
        CHECK (rollout_percent BETWEEN 0 AND 100),
    generation bigint NOT NULL DEFAULT 1 CHECK (generation > 0),
    updated_by text NOT NULL DEFAULT '',
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (tenant_id, active_revision)
        REFERENCES config_tenant_revisions (tenant_id, revision),
    FOREIGN KEY (tenant_id, canary_revision)
        REFERENCES config_tenant_revisions (tenant_id, revision)
);

CREATE TABLE IF NOT EXISTS config_releases (
    release_id text PRIMARY KEY,
    tenant_id text NOT NULL,
    kind text NOT NULL CHECK (kind IN ('full', 'canary', 'promote', 'rollback')),
    source_active_revision text NOT NULL,
    source_canary_revision text NOT NULL DEFAULT '',
    target_revision text NOT NULL,
    target_rollout_percent smallint NOT NULL DEFAULT 0
        CHECK (target_rollout_percent BETWEEN 0 AND 100),
    expected_generation bigint NOT NULL CHECK (expected_generation > 0),
    resulting_generation bigint,
    status text NOT NULL DEFAULT 'preparing'
        CHECK (status IN ('preparing', 'active_pending_ack', 'verified', 'failed', 'cancelled')),
    requested_by text NOT NULL DEFAULT '',
    change_reason text NOT NULL DEFAULT '',
    error_message text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    activated_at timestamptz,
    verified_at timestamptz,
    failed_at timestamptz,
    FOREIGN KEY (tenant_id, source_active_revision)
        REFERENCES config_tenant_revisions (tenant_id, revision),
    FOREIGN KEY (tenant_id, target_revision)
        REFERENCES config_tenant_revisions (tenant_id, revision)
);

CREATE INDEX IF NOT EXISTS config_releases_tenant_time_idx
    ON config_releases (tenant_id, created_at DESC);
CREATE INDEX IF NOT EXISTS config_releases_pending_idx
    ON config_releases (tenant_id, status, created_at DESC)
    WHERE status IN ('preparing', 'active_pending_ack');

CREATE TABLE IF NOT EXISTS config_release_nodes (
    release_id text NOT NULL REFERENCES config_releases (release_id) ON DELETE CASCADE,
    node_id text NOT NULL,
    boot_id text NOT NULL,
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'prepared', 'applied', 'failed')),
    loaded_revision text NOT NULL DEFAULT '',
    loaded_generation bigint,
    error_message text NOT NULL DEFAULT '',
    prepared_at timestamptz,
    applied_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (release_id, node_id, boot_id)
);

CREATE INDEX IF NOT EXISTS config_release_nodes_status_idx
    ON config_release_nodes (release_id, status);

CREATE TABLE IF NOT EXISTS config_release_events (
    event_id bigserial PRIMARY KEY,
    release_id text NOT NULL REFERENCES config_releases (release_id) ON DELETE CASCADE,
    tenant_id text NOT NULL,
    event_type text NOT NULL,
    status text NOT NULL,
    actor text NOT NULL DEFAULT '',
    details jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(details) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS config_release_events_release_idx
    ON config_release_events (release_id, event_id);

CREATE TABLE IF NOT EXISTS config_node_heartbeats (
    node_id text NOT NULL,
    boot_id text NOT NULL,
    last_seen_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    ready boolean NOT NULL DEFAULT false,
    loaded_generation bigint NOT NULL DEFAULT 0 CHECK (loaded_generation >= 0),
    active_revision text NOT NULL DEFAULT '',
    canary_revision text NOT NULL DEFAULT '',
    error_message text NOT NULL DEFAULT '',
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (node_id, boot_id)
);

CREATE INDEX IF NOT EXISTS config_node_heartbeats_live_idx
    ON config_node_heartbeats (last_seen_at DESC);

COMMIT;
