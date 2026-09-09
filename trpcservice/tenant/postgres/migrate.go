package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// migrationLockKey is an arbitrary but fixed advisory-lock key, spelling
// "trpc" and a schema generation in hex. Its only job is to be the same number
// in every process that runs this migration and a number no unrelated code is
// likely to pick.
//
// The lock is per database, not per schema, so two pools migrating two
// different schemas of one database serialise against each other. That costs a
// little startup time and buys one rule with no exceptions.
const migrationLockKey int64 = 0x7472_7063_7401_0001

const acquireMigrationLockSQL = `SELECT pg_advisory_xact_lock($1)`

// migrations brings an empty schema up to the current control-plane shape.
//
// The whole list is a single transaction, and DDL in PostgreSQL is
// transactional, so a failure half way leaves nothing behind.
//
// Every statement is IF NOT EXISTS and the transaction takes an advisory lock
// first. Both are needed: IF NOT EXISTS alone still races, because two
// sessions creating the same table concurrently both pass the existence check
// and the loser fails on pg_type's unique index rather than being skipped.
// With the lock, the second worker finds the tables already there and does
// nothing. This is deliberately not a migration framework — there is one
// version and no rollback. A second version needs a real tool, not another
// entry here.
//
// Design notes that are not obvious from the DDL:
//
//   - Every domain id is text, not uuid. The domain validates ids against its
//     own pattern and callers choose them ("assistant", "demo"), so a uuid
//     column would reject valid ids.
//   - Tenant-owned tables are keyed on (tenant_id, ...), so the tenant is part
//     of the primary key and no lookup can reach another tenant's row by
//     guessing a resource id.
//   - No foreign key cascades. Deleting a tenant that still owns apps is a
//     multi-step lifecycle (stop the ingress, expire the sessions, then
//     collect), and a cascade would silently do the destructive part of it on
//     a single mistaken DELETE.
//   - The CHECK constraints restate invariants the Go layer already enforces.
//     They are backstops: a violation means a bug or an out-of-band write, so
//     it is reported as an infrastructure failure, not as invalid input.
var migrations = []string{
	`CREATE TABLE IF NOT EXISTS tenants (
		id           text        NOT NULL,
		slug         text        NOT NULL,
		name         text        NOT NULL,
		status       text        NOT NULL,
		quota        jsonb       NOT NULL,
		audit_policy jsonb       NOT NULL,
		created_at   timestamptz NOT NULL,
		updated_at   timestamptz NOT NULL,
		CONSTRAINT tenants_pkey PRIMARY KEY (id),
		CONSTRAINT tenants_slug_key UNIQUE (slug)
	)`,

	`CREATE TABLE IF NOT EXISTS agent_apps (
		tenant_id       text        NOT NULL,
		id              text        NOT NULL,
		name            text        NOT NULL,
		status          text        NOT NULL,
		routing_version bigint      NOT NULL,
		routing_policy  jsonb       NOT NULL,
		created_at      timestamptz NOT NULL,
		updated_at      timestamptz NOT NULL,
		CONSTRAINT agent_apps_pkey PRIMARY KEY (tenant_id, id),
		CONSTRAINT agent_apps_tenant_fkey FOREIGN KEY (tenant_id)
			REFERENCES tenants (id),
		CONSTRAINT agent_apps_routing_version_check CHECK (routing_version >= 0)
	)`,

	`CREATE TABLE IF NOT EXISTS agent_revisions (
		tenant_id     text        NOT NULL,
		agent_app_id  text        NOT NULL,
		id            text        NOT NULL,
		revision_no   bigint      NOT NULL,
		status        text        NOT NULL,
		created_by    text        NOT NULL,
		config        jsonb       NOT NULL,
		config_digest text        NOT NULL,
		created_at    timestamptz NOT NULL,
		published_at  timestamptz,
		CONSTRAINT agent_revisions_pkey PRIMARY KEY (tenant_id, agent_app_id, id),
		CONSTRAINT agent_revisions_revision_no_key
			UNIQUE (tenant_id, agent_app_id, revision_no),
		CONSTRAINT agent_revisions_agent_app_fkey FOREIGN KEY (tenant_id, agent_app_id)
			REFERENCES agent_apps (tenant_id, id),
		CONSTRAINT agent_revisions_revision_no_check CHECK (revision_no > 0)
	)`,

	// Backend profiles are immutable: there is no UPDATE and no DELETE against
	// this table anywhere in this package, and the id is the version. The
	// columns reflect that — spec and fingerprint are written once, together,
	// and read back as a pair so a row that was edited out of band is caught
	// before a Runtime is built from it.
	//
	// The fingerprint is stored rather than only derived. It is not an
	// authenticator, since anything able to edit the spec can edit the column
	// beside it; it catches the accident — a partial restore, a manual UPDATE,
	// a half-applied migration — which is the failure that actually happens.
	`CREATE TABLE IF NOT EXISTS backend_profiles (
		tenant_id   text        NOT NULL,
		id          text        NOT NULL,
		spec        jsonb       NOT NULL,
		fingerprint text        NOT NULL,
		created_by  text        NOT NULL,
		created_at  timestamptz NOT NULL,
		CONSTRAINT backend_profiles_pkey PRIMARY KEY (tenant_id, id),
		CONSTRAINT backend_profiles_tenant_fkey FOREIGN KEY (tenant_id)
			REFERENCES tenants (id)
	)`,
}

// Migrate creates the control-plane tables if they are not already there.
//
// It is safe to call concurrently from several processes and safe to call
// again on an already-migrated database; both are the normal case when every
// worker migrates on startup.
//
// It is not called by New. A caller decides when schema changes happen.
//
// Migrate borrows pool and does not close it. It acts on the first schema of
// the pool's search_path, so a pool pointed at one schema migrates that schema
// only.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if pool == nil {
		return errInvalidPool()
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return storageError(ctx, "begin migration", err, nil)
	}
	// A no-op once the transaction has committed.
	defer func() { _ = tx.Rollback(ctx) }()

	// Held until this transaction ends, so it covers every statement below
	// without a separate release path on the error return.
	if _, err := tx.Exec(ctx, acquireMigrationLockSQL, migrationLockKey); err != nil {
		return storageError(ctx, "acquire migration lock", err, nil)
	}
	for i, statement := range migrations {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return storageError(ctx, fmt.Sprintf("apply migration statement %d", i+1), err, nil)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return storageError(ctx, "commit migration", err, nil)
	}
	return nil
}
