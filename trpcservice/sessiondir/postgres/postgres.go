// Package postgres stores the session revision pin — which agent revision each
// conversation adopted on its first run — in PostgreSQL.
//
// It is a second implementation of sessiondir.Directory, held to the same
// contract as the in-memory reference by the shared suite in
// sessiondir/sessiondirtest. A behaviour difference between the two is a bug in
// one of them, not a property of the backend. What this implementation adds is
// that the pin outlives the process: a worker that restarts, or a second worker
// that never saw the first run, resolves the same revision.
//
// # Ownership
//
// A Directory borrows the *pgxpool.Pool it is handed. It never closes the
// pool, holds nothing else that needs closing, and is safe for concurrent use,
// so one pool can back several directories and the caller closes it once,
// after the last user has stopped. Migrate borrows a pool the same way.
//
// # Migrations are explicit
//
// New performs no DDL and does not reach the database at all. A process
// starting against an unmigrated database has to call Migrate itself. Keeping
// the two apart is what lets a deployment migrate from one place — a job, a
// leader, an operator — while every other process only reads and writes.
//
// # Schema placement
//
// Every statement names its table unqualified, so Migrate and Directory both
// act on the first schema of the connection's search_path. Point a pool at a
// schema once on the pool config rather than per query:
//
//	config, err := pgxpool.ParseConfig(dsn)
//	config.ConnConfig.RuntimeParams["search_path"] = "control_plane"
//
// The table is named sessions. Upstream tRPC-Agent-Go owns session_states,
// session_events and session_track_events in whatever schema it is pointed at,
// so this name does not collide with the session store even when both share a
// schema. The two are separate concerns: upstream stores conversation content,
// this stores which revision may produce it.
//
// # Errors
//
// There are two kinds, and which one a failure belongs to decides the status
// the chat API answers with.
//
// A caller's mistake — a malformed key or candidate revision id — comes back as
// tenant.ErrInvalidArgument, exactly as MemoryDirectory reports it, carrying a
// message built only from values that caller supplied.
//
// Everything else — a dropped connection, a missing table, a constraint this
// package does not recognise — comes back under ErrStorage, wrapping the driver
// error, and is answered with a generic 500. Nothing the database can do maps
// to a 4xx: an unreachable store is not a bad request, and a session whose pin
// cannot be read must not be allowed to run against an arbitrary revision.
//
// Unlike the control-plane repository, no constraint here means a domain
// condition. A duplicate key is the contended first run, which EnsurePin
// resolves rather than reports, and the epoch CHECK is a backstop against an
// out-of-band write. So a constraint violation that reaches the caller is
// always an infrastructure fault, never something the caller could fix.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// ErrInvalidConfig reports a Directory or Migrate that was wired wrong. It is
// separate from ErrStorage because it can never be caused by the database.
var ErrInvalidConfig = errors.New("sessiondir/postgres: invalid configuration")

// ErrStorage is the sentinel behind every failure that is the database's or
// this package's fault rather than the caller's. It deliberately matches none
// of the tenant sentinels, so a caller mapping errors to HTTP statuses cannot
// mistake an unreachable database for a 400 or a 404 and let a session past the
// pin it could not read.
var ErrStorage = errors.New("sessiondir/postgres: session directory storage failure")

// Directory is the PostgreSQL implementation of sessiondir.Directory.
type Directory struct {
	pool *pgxpool.Pool
}

var _ sessiondir.Directory = (*Directory)(nil)

// New wraps an existing pool. It does not connect, migrate, or take ownership
// of the pool; see the package documentation.
func New(pool *pgxpool.Pool) (*Directory, error) {
	if pool == nil {
		return nil, errInvalidPool()
	}
	return &Directory{pool: pool}, nil
}

func errInvalidPool() error {
	return fmt.Errorf("%w: a pgxpool.Pool is required", ErrInvalidConfig)
}

// validateCall mirrors the check every MemoryDirectory entry point makes: the
// receiver first, so a value that never went through New refuses instead of
// dereferencing nil, then the context, so a caller that has already given up
// never costs a round trip.
func (d *Directory) validateCall(ctx context.Context) error {
	if d == nil || d.pool == nil {
		return fmt.Errorf("%w: session directory is not initialised", ErrInvalidConfig)
	}
	return contextError(ctx)
}

// contextError refuses a context that cannot be used, before anything else.
func contextError(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("%w: context is required", tenant.ErrInvalidArgument)
	}
	return ctx.Err()
}

// storageError is the tail of every database call.
//
// A cancelled context takes priority over whatever the driver reported: once
// the caller has given up, pgx reports the cancellation as a connection
// failure, and reporting that as an infrastructure fault would blame the
// database for the caller's own deadline.
//
// ctx is never nil: every entry point runs validateCall first.
func storageError(ctx context.Context, operation string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return storageFailure(operation, err)
}

// storageFailure wraps a non-domain failure. The driver error is kept in the
// chain on purpose: this error never reaches a client body, and without it an
// operator has nothing to debug from.
func storageFailure(operation string, err error) error {
	return fmt.Errorf("%w: %s: %w", ErrStorage, operation, err)
}

// checkStoredPin rejects a pinned_revision_id the database handed back that
// this package would never have written. EnsurePin validates every candidate
// before it inserts, so a stored value failing that same check did not come
// from here: it is an operator's UPDATE, a migration that backfilled badly, or
// corruption. The column is only text NOT NULL, so nothing in the schema
// prevents one.
//
// Returning it would fail open at the storage boundary, in two different ways.
// A malformed id reaches tenant.ValidateResourceID again in the revision
// resolver, where it becomes tenant.ErrInvalidArgument and the chat API answers
// 400 — a database fault reported as the caller's mistake. An empty one is
// worse and silent: the resolver reads "" as "no pin was given" and falls
// through to the app's current default revision, so a corrupt row quietly moves
// a pinned conversation onto whatever is published now. That is the exact
// outcome this package exists to prevent, and it would arrive as a 200.
//
// The validation error is deliberately not wrapped. It carries
// tenant.ErrInvalidArgument, and putting that in this chain would recreate the
// 400 this check exists to stop; the caller must see nothing but ErrStorage.
func checkStoredPin(operation string, pinned string) error {
	if tenant.ValidateResourceID("revision id", pinned) == nil {
		return nil
	}
	return storageFailure(operation, fmt.Errorf(
		"stored pinned_revision_id %s is not a well-formed revision id",
		redactStoredValue(pinned)))
}

// storedValueLogLimit bounds how much of a rejected value is repeated back.
const storedValueLogLimit = 64

// redactStoredValue renders a value that failed validation for an operator's
// log. This error never reaches a response body, but it does reach a log, and
// the value is by definition one that nothing in this package wrote: %q escapes
// newlines and control characters so a crafted value cannot forge a log line,
// and the bound keeps a large one from filling the file.
func redactStoredValue(value string) string {
	if len(value) <= storedValueLogLimit {
		return fmt.Sprintf("%q", value)
	}
	return fmt.Sprintf("%q (truncated from %d bytes)", value[:storedValueLogLimit], len(value))
}

// storedNow returns the timestamp a pin records.
//
// It is UTC and truncated to microseconds because that is exactly what
// timestamptz keeps, so the value a row is written with is the value it comes
// back with. Nothing in this package reads created_at, but a column that only
// approximately holds what was written is a trap for whatever does later.
func storedNow() time.Time {
	return time.Now().UTC().Truncate(time.Microsecond)
}

// queryRower is the part of *pgxpool.Pool and pgx.Tx this package reads
// through, so one scan helper serves both the pooled read and the read inside
// the EnsurePin transaction.
type queryRower interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// keyArguments lays a Key out in the order every statement binds it.
//
// Epoch is a uint32 widened to int64. The column is a bigint, so the whole
// unsigned range fits with room to spare and no value can wrap to a negative
// number; a CHECK in the migration keeps anything written out of band inside
// that range.
func keyArguments(key sessiondir.Key) []any {
	return []any{
		key.TenantID,
		key.AppID,
		key.PrincipalID,
		key.SessionID,
		int64(key.Epoch),
	}
}
