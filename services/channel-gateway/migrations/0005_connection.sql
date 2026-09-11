-- Connection owns non-secret desired account state and durable owner fencing.
-- Rows are permanent identities: neither Release nor replacement deletes them.
CREATE TABLE gateway_connection_accounts (
 account_id text PRIMARY KEY,
 bot_id text NOT NULL UNIQUE,
 credential_ref text NOT NULL,
 revision bigint NOT NULL CHECK (revision > 0),
 enabled boolean NOT NULL,
 instance_id text NOT NULL DEFAULT '',
 epoch bigint NOT NULL DEFAULT 0 CHECK (epoch >= 0),
 lease_until timestamptz,
 blocked_revision bigint NOT NULL DEFAULT 0 CHECK (blocked_revision >= 0 AND blocked_revision <= revision),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
