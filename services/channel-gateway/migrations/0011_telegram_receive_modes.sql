-- One platform receiver control row per physical Telegram Bot. Scope/account
-- remain immutable ownership references; disabled accounts retain their cursor.
CREATE TABLE gateway_telegram_receivers (
 bot_id text PRIMARY KEY CHECK(bot_id ~ '^[1-9][0-9]*$'),
 scope_id text NOT NULL,
 account_id text NOT NULL,
 source_epoch text NOT NULL,
 connection_revision bigint NOT NULL CHECK(connection_revision BETWEEN 1 AND 9007199254740991),
 owner_epoch bigint NOT NULL DEFAULT 0 CHECK(owner_epoch BETWEEN 0 AND 9007199254740991),
 instance_id text NOT NULL DEFAULT '',
 instance_epoch text NOT NULL DEFAULT '',
 lease_until timestamptz NOT NULL DEFAULT 'epoch',
 call_id text NOT NULL DEFAULT '',
 call_until timestamptz NOT NULL DEFAULT 'epoch',
 next_offset bigint NOT NULL DEFAULT 0 CHECK(next_offset BETWEEN 0 AND 9007199254740991),
 last_update_at timestamptz NOT NULL DEFAULT 'epoch',
 last_poll_at timestamptz NOT NULL DEFAULT 'epoch',
 managed_url text NOT NULL DEFAULT '',
 pending_url text NOT NULL DEFAULT '',
 managed_revision bigint NOT NULL DEFAULT 0,
 next_due timestamptz NOT NULL DEFAULT 'epoch',
 last_reason text NOT NULL DEFAULT 'NONE',
 UNIQUE(scope_id,account_id),
 FOREIGN KEY(scope_id,account_id) REFERENCES gateway_account_directory(scope_id,account_id)
);
