-- Non-secret Control replica. Connection leases and Delivery ledgers retain
-- their existing owners and tables. No credential value is stored here.
CREATE TABLE gateway_account_catalogs (
 scope_id text PRIMARY KEY,
 source_epoch text NOT NULL,
 revision bigint NOT NULL DEFAULT 0 CHECK(revision BETWEEN 0 AND 9007199254740991),
 digest text NOT NULL DEFAULT '',
 snapshot_json jsonb,
 blocked boolean NOT NULL DEFAULT false,
 UNIQUE(scope_id,source_epoch)
);
CREATE TABLE gateway_account_directory (
 scope_id text NOT NULL,
 source_epoch text NOT NULL,
 account_id text NOT NULL,
 tenant_id text NOT NULL,
 provider text NOT NULL CHECK(provider IN ('telegram','wecom')),
 provider_account_id text NOT NULL,
 connection_revision bigint NOT NULL CHECK(connection_revision BETWEEN 1 AND 9007199254740991),
 min_route_generation bigint NOT NULL CHECK(min_route_generation BETWEEN 0 AND 9007199254740991),
 enabled boolean NOT NULL,
 present boolean NOT NULL,
 account_json jsonb NOT NULL,
 PRIMARY KEY(scope_id,account_id),
 FOREIGN KEY(scope_id,source_epoch) REFERENCES gateway_account_catalogs(scope_id,source_epoch),
 UNIQUE(scope_id,provider,provider_account_id)
);
CREATE TABLE gateway_account_qualifications (
 scope_id text NOT NULL,
 source_epoch text NOT NULL,
 instance_id text NOT NULL,
 instance_epoch text NOT NULL,
 generation bigint NOT NULL DEFAULT 1 CHECK(generation>0),
 source_revision bigint NOT NULL DEFAULT 0,
 valid_until timestamptz NOT NULL,
 enabled boolean NOT NULL DEFAULT false,
 PRIMARY KEY(scope_id,instance_id,instance_epoch),
 FOREIGN KEY(scope_id,source_epoch) REFERENCES gateway_account_catalogs(scope_id,source_epoch)
);
CREATE TABLE gateway_account_snapshot_receipts (
 scope_id text NOT NULL REFERENCES gateway_account_catalogs(scope_id),
 source_epoch text NOT NULL,
 revision bigint NOT NULL,
 digest text NOT NULL,
 received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(scope_id,source_epoch,revision)
);
CREATE INDEX gateway_account_snapshot_receipts_age ON gateway_account_snapshot_receipts(received_at);
