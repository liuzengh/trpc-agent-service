BEGIN;

-- Versioned runtime data-plane schema used by the durable Inbox/Outbox queue
-- and the strict PostgreSQL Session turn backend. These tables intentionally
-- remain self-contained: tenant configuration is loaded from a signed YAML
-- snapshot at runtime, so they cannot depend on control-plane tenant rows from
-- 001_schema.sql. Isolation is enforced by the immutable tenant/app identity
-- carried by queue records and the hashed app_name used by Session routing.
--
-- The service executes the body of this file inside its own transaction and
-- records the SHA-256 of this immutable file in schema_migrations. A regular
-- migration executor may execute this complete file and record the same
-- checksum after COMMIT.

CREATE TABLE IF NOT EXISTS schema_migrations (
    version text PRIMARY KEY,
    description text NOT NULL,
    checksum text NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS runtime_inbox (
    inbox_id uuid PRIMARY KEY,
    queue_sequence bigserial NOT NULL,
    tenant_id text NOT NULL,
    channel_type text NOT NULL,
    binding_id text NOT NULL,
    external_message_id text NOT NULL,
    dedup_key text NOT NULL UNIQUE,
    partition_key text NOT NULL DEFAULT '',
    pipeline_schema_version integer NOT NULL DEFAULT 0,
    atomic_commit_mode text NOT NULL DEFAULT '',
    database_identity text NOT NULL DEFAULT '',
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    trace_carrier jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(trace_carrier) = 'object'),
    status text NOT NULL DEFAULT 'received'
        CHECK (status IN ('received', 'processing', 'processed', 'retry', 'dead_letter')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    lease_owner text NOT NULL DEFAULT '',
    lease_expires_at timestamptz,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error_type text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Upgrade queue tables created before session partitioning. An empty key is a
-- deliberate legacy marker: lease queries serialize it at tenant/binding
-- scope until that backlog drains, preserving order without globally blocking
-- unrelated tenants or channels during a rolling upgrade.
ALTER TABLE runtime_inbox
    ADD COLUMN IF NOT EXISTS partition_key text NOT NULL DEFAULT '';
ALTER TABLE runtime_inbox
    ADD COLUMN IF NOT EXISTS queue_sequence bigserial;
ALTER TABLE runtime_inbox
    ADD COLUMN IF NOT EXISTS pipeline_schema_version integer NOT NULL DEFAULT 0;
ALTER TABLE runtime_inbox
    ADD COLUMN IF NOT EXISTS atomic_commit_mode text NOT NULL DEFAULT '';
ALTER TABLE runtime_inbox
    ADD COLUMN IF NOT EXISTS database_identity text NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS runtime_inbox_dispatch_idx
    ON runtime_inbox (status, next_attempt_at)
    WHERE status IN ('received', 'retry');
CREATE INDEX IF NOT EXISTS runtime_inbox_lease_idx
    ON runtime_inbox (lease_expires_at)
    WHERE status = 'processing';
CREATE INDEX IF NOT EXISTS runtime_inbox_partition_order_idx
    ON runtime_inbox (tenant_id, channel_type, binding_id, partition_key, created_at, queue_sequence)
    WHERE status IN ('received', 'retry', 'processing');

CREATE TABLE IF NOT EXISTS runtime_outbox (
    outbox_id uuid PRIMARY KEY,
    queue_sequence bigserial NOT NULL,
    operation_key text NOT NULL DEFAULT '',
    operation_version integer NOT NULL DEFAULT 0 CHECK (operation_version >= 0),
    part_index integer NOT NULL DEFAULT 0 CHECK (part_index >= 0),
    part_count integer NOT NULL DEFAULT 1 CHECK (part_count > 0 AND part_index < part_count),
    payload_hash text NOT NULL DEFAULT '',
    tenant_id text NOT NULL,
    channel_type text NOT NULL,
    binding_id text NOT NULL,
    dedup_key text NOT NULL UNIQUE,
    partition_key text NOT NULL DEFAULT '',
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    payload_bytes bytea,
    trace_id text NOT NULL DEFAULT '',
    trace_carrier jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(trace_carrier) = 'object'),
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'sending', 'sent', 'retry', 'dead_letter')),
    delivery_state text NOT NULL DEFAULT 'pending'
        CHECK (delivery_state IN (
            'pending', 'in_flight', 'confirmed', 'retryable_not_sent',
            'permanent_rejected', 'unknown', 'retry_exhausted', 'canceled'
        )),
    state_version bigint NOT NULL DEFAULT 0 CHECK (state_version >= 0),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    lease_owner text NOT NULL DEFAULT '',
    lease_expires_at timestamptz,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    last_error_type text NOT NULL DEFAULT '',
    provider_code text NOT NULL DEFAULT '',
    provider_message_id text NOT NULL DEFAULT '',
    provider_request_id text NOT NULL DEFAULT '',
    response_hash text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

-- Upgrade runtime tables produced by all prior service releases. The
-- additions are deliberately online-safe and preserve conservative ordering
-- for rows that predate exact partition keys and immutable delivery bytes.
ALTER TABLE runtime_outbox
    ADD COLUMN IF NOT EXISTS trace_carrier jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE runtime_outbox
    ADD COLUMN IF NOT EXISTS partition_key text NOT NULL DEFAULT '';
ALTER TABLE runtime_outbox
    ADD COLUMN IF NOT EXISTS queue_sequence bigserial;
ALTER TABLE runtime_outbox
    ADD COLUMN IF NOT EXISTS operation_key text NOT NULL DEFAULT '';
ALTER TABLE runtime_outbox
    ADD COLUMN IF NOT EXISTS operation_version integer NOT NULL DEFAULT 0;
ALTER TABLE runtime_outbox
    ADD COLUMN IF NOT EXISTS part_index integer NOT NULL DEFAULT 0;
ALTER TABLE runtime_outbox
    ADD COLUMN IF NOT EXISTS part_count integer NOT NULL DEFAULT 1;
ALTER TABLE runtime_outbox
    ADD COLUMN IF NOT EXISTS payload_hash text NOT NULL DEFAULT '';
ALTER TABLE runtime_outbox
    ADD COLUMN IF NOT EXISTS payload_bytes bytea;
ALTER TABLE runtime_outbox
    ADD COLUMN IF NOT EXISTS delivery_state text NOT NULL DEFAULT 'pending';
ALTER TABLE runtime_outbox
    ADD COLUMN IF NOT EXISTS state_version bigint NOT NULL DEFAULT 0;
ALTER TABLE runtime_outbox
    ADD COLUMN IF NOT EXISTS provider_code text NOT NULL DEFAULT '';
ALTER TABLE runtime_outbox
    ADD COLUMN IF NOT EXISTS provider_message_id text NOT NULL DEFAULT '';
ALTER TABLE runtime_outbox
    ADD COLUMN IF NOT EXISTS provider_request_id text NOT NULL DEFAULT '';
ALTER TABLE runtime_outbox
    ADD COLUMN IF NOT EXISTS response_hash text NOT NULL DEFAULT '';

-- Legacy jsonb rows have already lost insignificant whitespace; canonical
-- jsonb text is the strongest immutable byte representation available during
-- this one-time backfill.
UPDATE runtime_outbox
SET payload_bytes = convert_to(payload::text, 'UTF8')
WHERE payload_bytes IS NULL;
ALTER TABLE runtime_outbox
    ALTER COLUMN payload_bytes SET NOT NULL;

-- Existing operations did not carry a provider idempotency key. Their row ID
-- is stable and storage-safe. The trigger keeps rolling old writers compatible
-- until their backlog has drained.
UPDATE runtime_outbox
SET operation_key = 'legacy:' || outbox_id::text
WHERE operation_key = '';
UPDATE runtime_outbox
SET payload_hash = 'legacy:' || md5(payload_bytes)
WHERE payload_hash = '';
UPDATE runtime_outbox
SET delivery_state = CASE status
    WHEN 'sent' THEN 'confirmed'
    WHEN 'retry' THEN 'retryable_not_sent'
    WHEN 'dead_letter' THEN 'permanent_rejected'
    WHEN 'sending' THEN 'in_flight'
    ELSE 'pending'
END
WHERE operation_version = 0 AND delivery_state = 'pending';

CREATE OR REPLACE FUNCTION runtime_outbox_legacy_defaults()
RETURNS trigger LANGUAGE plpgsql AS $body$
BEGIN
    IF NEW.operation_key = '' THEN
        NEW.operation_key := 'legacy:' || NEW.outbox_id::text;
    END IF;
    IF NEW.payload_bytes IS NULL THEN
        NEW.payload_bytes := convert_to(NEW.payload::text, 'UTF8');
    END IF;
    IF NEW.payload_hash = '' THEN
        NEW.payload_hash := 'legacy:' || md5(NEW.payload_bytes);
    END IF;
    RETURN NEW;
END
$body$;
DROP TRIGGER IF EXISTS runtime_outbox_legacy_defaults ON runtime_outbox;
CREATE TRIGGER runtime_outbox_legacy_defaults
BEFORE INSERT ON runtime_outbox
FOR EACH ROW EXECUTE FUNCTION runtime_outbox_legacy_defaults();

CREATE UNIQUE INDEX IF NOT EXISTS runtime_outbox_operation_key_uidx
    ON runtime_outbox (operation_key);

CREATE TABLE IF NOT EXISTS runtime_outbox_attempts (
    outbox_id uuid NOT NULL REFERENCES runtime_outbox(outbox_id) ON DELETE CASCADE,
    operation_key text NOT NULL,
    attempt_no integer NOT NULL CHECK (attempt_no > 0),
    lease_owner text NOT NULL,
    phase text NOT NULL CHECK (phase IN ('leased', 'dispatched', 'finished')),
    outcome text NOT NULL DEFAULT ''
        CHECK (outcome IN ('', 'confirmed', 'retryable_not_sent', 'permanent_rejected', 'unknown')),
    error_type text NOT NULL DEFAULT '',
    provider_code text NOT NULL DEFAULT '',
    http_status integer NOT NULL DEFAULT 0,
    provider_message_id text NOT NULL DEFAULT '',
    provider_request_id text NOT NULL DEFAULT '',
    response_hash text NOT NULL DEFAULT '',
    started_at timestamptz NOT NULL DEFAULT now(),
    dispatched_at timestamptz,
    finished_at timestamptz,
    PRIMARY KEY (outbox_id, attempt_no)
);

CREATE TABLE IF NOT EXISTS runtime_outbox_resolutions (
    resolution_id text PRIMARY KEY,
    outbox_id uuid NOT NULL REFERENCES runtime_outbox(outbox_id) ON DELETE CASCADE,
    expected_version bigint NOT NULL,
    expected_attempt integer NOT NULL,
    action text NOT NULL CHECK (action IN ('assume_delivered', 'retry', 'cancel')),
    actor text NOT NULL,
    reason text NOT NULL,
    resulting_status text NOT NULL,
    resulting_delivery_state text NOT NULL,
    resulting_version bigint NOT NULL,
    resolved_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS runtime_outbox_dispatch_idx
    ON runtime_outbox (status, next_attempt_at)
    WHERE status IN ('pending', 'retry');
CREATE INDEX IF NOT EXISTS runtime_outbox_lease_idx
    ON runtime_outbox (lease_expires_at)
    WHERE status = 'sending' AND lease_expires_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS runtime_outbox_partition_order_idx
    ON runtime_outbox (tenant_id, channel_type, binding_id, partition_key, created_at, queue_sequence)
    WHERE status IN ('pending', 'retry', 'sending');
CREATE INDEX IF NOT EXISTS runtime_outbox_unknown_idx
    ON runtime_outbox (updated_at, queue_sequence)
    WHERE status = 'sending' AND delivery_state = 'unknown';
CREATE INDEX IF NOT EXISTS runtime_outbox_attempt_phase_idx
    ON runtime_outbox_attempts (phase, started_at)
    WHERE phase <> 'finished';
CREATE INDEX IF NOT EXISTS runtime_outbox_resolution_outbox_idx
    ON runtime_outbox_resolutions (outbox_id, resolved_at DESC);

CREATE TABLE IF NOT EXISTS session_turn_sessions (
    app_name text NOT NULL,
    user_id text NOT NULL,
    session_id text NOT NULL,
    state jsonb NOT NULL DEFAULT '{}'::jsonb,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    fencing_token bigint NOT NULL DEFAULT 0 CHECK (fencing_token >= 0),
    last_event_sequence bigint NOT NULL DEFAULT 0 CHECK (last_event_sequence >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (app_name, user_id, session_id)
);

CREATE TABLE IF NOT EXISTS session_turns (
    app_name text NOT NULL,
    user_id text NOT NULL,
    session_id text NOT NULL,
    turn_id text NOT NULL,
    expected_version bigint NOT NULL CHECK (expected_version >= 0),
    fencing_token bigint NOT NULL CHECK (fencing_token > 0),
    status text NOT NULL CHECK (status IN ('active', 'committed', 'aborted')),
    replay bytea NOT NULL DEFAULT '\x'::bytea,
    committed_version bigint CHECK (committed_version > 0),
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    completed_at timestamptz,
    PRIMARY KEY (app_name, user_id, session_id, turn_id),
    UNIQUE (app_name, user_id, session_id, fencing_token),
    CHECK (
        (status = 'active' AND committed_version IS NULL AND completed_at IS NULL)
        OR (status = 'aborted' AND committed_version IS NULL AND completed_at IS NOT NULL)
        OR (status = 'committed' AND committed_version IS NOT NULL AND completed_at IS NOT NULL)
    ),
    FOREIGN KEY (app_name, user_id, session_id)
        REFERENCES session_turn_sessions (app_name, user_id, session_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS session_turn_events (
    app_name text NOT NULL,
    user_id text NOT NULL,
    session_id text NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    turn_id text NOT NULL,
    event_id text NOT NULL,
    event_data jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (app_name, user_id, session_id, sequence),
    FOREIGN KEY (app_name, user_id, session_id, turn_id)
        REFERENCES session_turns (app_name, user_id, session_id, turn_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS session_turn_events_turn_idx
    ON session_turn_events (app_name, user_id, session_id, turn_id, sequence);

CREATE TABLE IF NOT EXISTS session_turn_app_states (
    app_name text NOT NULL,
    key text NOT NULL,
    value bytea,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (app_name, key)
);

CREATE TABLE IF NOT EXISTS session_turn_user_states (
    app_name text NOT NULL,
    user_id text NOT NULL,
    key text NOT NULL,
    value bytea,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (app_name, user_id, key)
);

COMMIT;
