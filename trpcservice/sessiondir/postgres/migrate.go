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
// It is deliberately not the control plane's key. The two migrations touch
// different tables and neither waits on the other, so sharing a key would only
// serialise unrelated startups.
//
// The lock is per database, not per schema, so two pools migrating two
// different schemas of one database serialise against each other. That costs a
// little startup time and buys one rule with no exceptions.
const migrationLockKey int64 = 0x7472_7063_7402_0001

const acquireMigrationLockSQL = `SELECT pg_advisory_xact_lock($1)`

// migrations brings an empty schema up to the current session-directory shape.
//
// The whole list is a single transaction, and DDL in PostgreSQL is
// transactional, so a failure half way leaves nothing behind.
//
// Every statement is IF NOT EXISTS and the transaction takes an advisory lock
// first. Both are needed: IF NOT EXISTS alone still races, because two
// sessions creating the same table concurrently both pass the existence check
// and the loser fails on pg_type's unique index rather than being skipped.
// With the lock, the second worker finds the table already there and does
// nothing. This is deliberately not a migration framework — there is one
// version and no rollback. A second version needs a real tool, not another
// entry here.
//
// Design notes that are not obvious from the DDL:
//
//   - The primary key is all five fields of sessiondir.Key. A conversation is
//     addressed by the whole tuple, so a key missing any field would merge two
//     different conversations onto one pin; putting tenant_id first also means
//     no lookup can reach another tenant's row by guessing a session id.
//   - epoch is bigint, not integer. sessiondir.Key.Epoch is a Go uint32, whose
//     upper half does not fit in PostgreSQL's signed 4-byte integer. The CHECK
//     restates the full unsigned range rather than only rejecting negatives.
//   - There is no foreign key to agent_revisions, and that is a decision, not
//     an omission. A Directory stores whatever candidate its caller resolved;
//     the caller has already resolved the revision against the control plane
//     before it gets here, and MemoryDirectory accepts any well-formed id. A
//     foreign key would make this implementation reject rows the reference
//     implementation accepts, which is the contract drifting rather than a
//     safety net. It would also tie the pin to a table that may live in another
//     schema or another database entirely.
//   - Only the fields the Directory interface owns are stored. Anything else a
//     session needs — its events, its state — belongs to the session store
//     upstream, not here.
var migrations = []string{
	`CREATE TABLE IF NOT EXISTS sessions (
		tenant_id          text        NOT NULL,
		agent_app_id       text        NOT NULL,
		principal_id       text        NOT NULL,
		session_id         text        NOT NULL,
		epoch              bigint      NOT NULL,
		pinned_revision_id text        NOT NULL,
		created_at         timestamptz NOT NULL,
		CONSTRAINT sessions_pkey PRIMARY KEY
			(tenant_id, agent_app_id, principal_id, session_id, epoch),
		CONSTRAINT sessions_epoch_check CHECK (epoch >= 0 AND epoch <= 4294967295)
	)`,
}

// Migrate creates the session-directory table if it is not already there.
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
		return storageError(ctx, "begin migration", err)
	}
	// A no-op once the transaction has committed.
	defer func() { _ = tx.Rollback(ctx) }()

	// Held until this transaction ends, so it covers every statement below
	// without a separate release path on the error return.
	if _, err := tx.Exec(ctx, acquireMigrationLockSQL, migrationLockKey); err != nil {
		return storageError(ctx, "acquire migration lock", err)
	}
	for i, statement := range migrations {
		if _, err := tx.Exec(ctx, statement); err != nil {
			return storageError(ctx, fmt.Sprintf("apply migration statement %d", i+1), err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return storageError(ctx, "commit migration", err)
	}
	return nil
}
