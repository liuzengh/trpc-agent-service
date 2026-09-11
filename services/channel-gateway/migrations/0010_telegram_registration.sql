-- Separate from the WeCom connection lease and Delivery claims.
CREATE TABLE gateway_telegram_registrations (
 scope_id text NOT NULL, account_id text NOT NULL, connection_revision bigint NOT NULL,
 epoch bigint NOT NULL DEFAULT 0, instance_id text NOT NULL DEFAULT '', instance_epoch text NOT NULL DEFAULT '',
 lease_until timestamptz NOT NULL DEFAULT '-infinity', next_due timestamptz NOT NULL DEFAULT '-infinity',
 operation_id text NOT NULL DEFAULT '', state text NOT NULL DEFAULT 'PENDING',
 PRIMARY KEY(scope_id,account_id)
);
CREATE TABLE gateway_telegram_registration_attempts (
 operation_id text PRIMARY KEY, scope_id text NOT NULL, account_id text NOT NULL,
 connection_revision bigint NOT NULL, epoch bigint NOT NULL, instance_id text NOT NULL, instance_epoch text NOT NULL,
 state text NOT NULL DEFAULT 'PREPARING', result text, created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 calling_at timestamptz, finished_at timestamptz
);
CREATE INDEX gateway_telegram_registration_attempt_account ON gateway_telegram_registration_attempts(scope_id,account_id,created_at);
