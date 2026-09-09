// Package postgres stores the channel pipeline — accepted external events, the
// Runs that answer them, and the answers on their way back out — in PostgreSQL.
//
// This implementation is checked against the shared channels.Store contract
// in channels/channelstest. Any alternative backend must obey the same
// state-transition and tenant-isolation rules. The suite checks the storage
// operations; it does not establish an end-to-end IM execution guarantee.
//
// Committed Inbox, Run and Outbox records survive application restarts while
// the database remains durable. A caller can inspect unfinished Runs and
// persisted replies, but recovery requires an execution layer to interpret
// Session and Tool evidence before deciding whether work can be retried.
// This package does not start Workers, dispatch replies, publish wakeups or
// scan for recovery. Database failover durability depends on deployment.
//
// # Ownership
//
// A Store borrows the *pgxpool.Pool it is handed. It never closes the pool,
// holds nothing else that needs closing, and is safe for concurrent use, so one
// pool can back several stores and the caller closes it once, after the last
// user has stopped. Migrate borrows a pool the same way.
//
// # Migrations are explicit
//
// New performs no DDL and does not reach the database at all. A process
// starting against an unmigrated database has to call Migrate itself.
//
// # Schema placement
//
// Every statement names its table unqualified, so Migrate and Store both act on
// the first schema of the connection's search_path. Nothing here assumes the
// control-plane tables or the upstream session tables are in that schema — the
// channel tables are self-contained and carry no foreign key out of the set.
//
// # Errors, and what may not be in them
//
// A caller's mistake comes back as tenant.ErrInvalidArgument or
// tenant.ErrTenantScope; a missing row as tenant.ErrNotFound; a lost CAS as
// channels.ErrStaleClaim. Everything else comes back under ErrStorage.
//
// The one rule this package has that the others do not: a database error is
// never passed through as the driver produced it. A PostgreSQL unique violation
// carries a Detail field containing the whole conflicting key — for
// channel_inbox_messages that is the third-party external event id, and for the
// outbox it is a channel-scoped idempotency key. Those are exactly the values
// §2 of the pipeline contract keeps out of logs. So every driver error is
// reduced to its operation, its SQLSTATE and its constraint name before it
// enters an error chain, and the original is dropped rather than wrapped: a
// wrapped one is still reachable with errors.As, and a %+v in some logger three
// layers up would print it.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// ErrInvalidConfig reports a Store or Migrate that was wired wrong. It is
// separate from ErrStorage because it can never be caused by the database.
var ErrInvalidConfig = errors.New("channels/postgres: invalid configuration")

// ErrStorage is the sentinel behind every failure that is the database's or
// this package's fault rather than the caller's. It deliberately matches none
// of the tenant sentinels and not channels.ErrStaleClaim, so a caller mapping
// errors to HTTP statuses cannot mistake an unreachable database for a 400, and
// a Worker cannot mistake one for "somebody else owns this Run" and give up a
// claim it still holds.
var ErrStorage = errors.New("channels/postgres: channel store storage failure")

// SQLSTATE classes this package recognises.
const (
	uniqueViolation = "23505"
	checkViolation  = "23514"
)

// Store is the PostgreSQL implementation of channels.Store.
type Store struct {
	pool *pgxpool.Pool
}

var _ channels.Store = (*Store)(nil)

// New wraps an existing pool. It does not connect, migrate, or take ownership
// of the pool; see the package documentation.
func New(pool *pgxpool.Pool) (*Store, error) {
	if pool == nil {
		return nil, errInvalidPool()
	}
	return &Store{pool: pool}, nil
}

func errInvalidPool() error {
	return fmt.Errorf("%w: a pgxpool.Pool is required", ErrInvalidConfig)
}

// validateCall mirrors the check every in-memory entry point makes: the
// receiver first, so a value that never went through New refuses instead of
// dereferencing nil, then the context, so a caller that has already given up
// never costs a round trip.
func (s *Store) validateCall(ctx context.Context) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("%w: channel store is not initialised", ErrInvalidConfig)
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
func storageError(ctx context.Context, operation string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return storageFailure(operation, err)
}

// storageFailure wraps a non-domain failure, after redacting it.
func storageFailure(operation string, err error) error {
	return fmt.Errorf("%w: %s: %w", ErrStorage, operation, redactDriverError(err))
}

// constraintError maps a named constraint violation to a domain error and
// falls back to a storage failure for anything else.
func constraintError(
	ctx context.Context,
	operation string,
	err error,
	byConstraint map[string]error,
) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if name, ok := violatedConstraint(err); ok {
		if mapped, ok := byConstraint[name]; ok {
			return mapped
		}
	}
	return storageFailure(operation, err)
}

// violatedConstraint reports which constraint a driver error violated.
//
// Only the name is taken. It is a schema identifier chosen in migrate.go, so it
// is safe to compare, safe to log, and says nothing about the row.
func violatedConstraint(err error) (string, bool) {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return "", false
	}
	switch pgErr.Code {
	case uniqueViolation, checkViolation:
		return pgErr.ConstraintName, true
	default:
		return "", false
	}
}

// redactDriverError reduces a database error to what may be kept.
//
// A *pgconn.PgError is replaced outright rather than wrapped. Its Detail field
// spells out the conflicting key of a unique violation — the external event id,
// or an outbox idempotency key — and its Message can name a value for some
// classes. Wrapping would leave both reachable through errors.As, so the
// original is dropped and only the SQLSTATE and the constraint name survive.
// Between them those are enough for an operator to know exactly which rule was
// broken, and they cannot say by which row.
//
// Anything that is not a PgError — a dead connection, a pool timeout, a scan
// mismatch — is passed through: those are produced from the driver's own state
// and carry no row content.
func redactDriverError(err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}
	if pgErr.ConstraintName != "" {
		return fmt.Errorf(
			"postgres rejected the statement (SQLSTATE %s, constraint %s)",
			pgErr.Code, pgErr.ConstraintName)
	}
	return fmt.Errorf("postgres rejected the statement (SQLSTATE %s)", pgErr.Code)
}

// notFound builds the missing-row error, naming only the id the caller passed.
func notFound(kind, id string) error {
	return fmt.Errorf("%w: %s %q", tenant.ErrNotFound, kind, id)
}

// alreadyExists builds the duplicate-id error. It never names the value,
// because the values that can collide here include the external event id.
func alreadyExists(what string) error {
	return fmt.Errorf("%w: %s is already used", tenant.ErrAlreadyExists, what)
}

// requiredTimeError rejects a zero clock reading, which every comparison in
// this package would read as the beginning of time.
func requiredTimeError(field string) error {
	return fmt.Errorf("%w: %s is required", tenant.ErrInvalidArgument, field)
}

// tooManyRefsError rejects a batch larger than any scan could have produced.
func tooManyRefsError(kind string) error {
	return fmt.Errorf(
		"%w: cannot mark more than %d %s at once",
		tenant.ErrInvalidArgument, channels.MaxListLimit, kind)
}

// tenantMismatchError reports a call whose payload names a tenant other than
// the authenticated one.
func tenantMismatchError(what, got, want string) error {
	return fmt.Errorf("%w: %s tenant %q does not match %q", tenant.ErrTenantScope, what, got, want)
}

// integrityError reports a stored row this package would never have written.
//
// It is deliberately not built from tenant.ErrInvalidArgument: a corrupt row is
// not the caller's mistake, and reporting it as one would answer a 400 for a
// database fault. It also names no value — the whole point is that the value is
// untrusted.
func integrityError(operation, column string) error {
	return storageFailure(operation, fmt.Errorf("stored %s is not well formed", column))
}

// sessionOrderingLockClass namespaces the per-Session ordering lock.
//
// PostgreSQL keeps the one-argument and two-argument advisory lock forms in
// separate key spaces, so this cannot collide with migrationLockKey however the
// hashes fall. The class is a fixed constant and the object is a hash of the
// Session key, which is what makes two accepts for one conversation serialise
// and two accepts for different conversations not.
const sessionOrderingLockClass int32 = 0x7470_6301

const acquireSessionLockSQL = `SELECT pg_advisory_xact_lock($1, $2)`

// sessionLockObject hashes a Session key into the lock's object id.
//
// Collisions are possible — 32 bits, and two unrelated conversations can land
// on the same number. That is harmless and it is why this is acceptable: a
// collision costs two accepts a little serialisation, it never lets two accepts
// for the *same* Session run concurrently, which is the property the lock
// exists for. The alternative, a lock table keyed by the real tuple, would be a
// row to insert and clean up on a path that has to be fast.
//
// The hash covers all four fields with a separator that cannot appear in a
// resource id, so no two different keys concatenate to the same string.
func sessionLockObject(key sessiondir.Key) int32 {
	digest := fnv.New32a()
	for _, part := range []string{key.TenantID, key.AppID, key.PrincipalID, key.SessionID} {
		_, _ = digest.Write([]byte(part))
		_, _ = digest.Write([]byte{0})
	}
	return int32(digest.Sum32())
}

// withTx runs fn inside a transaction, rolling back unless fn commits by
// returning nil.
//
// Every multi-statement operation in this package goes through here, so there
// is one rollback path rather than one per method. The deferred Rollback is a
// no-op after a successful Commit.
func (s *Store) withTx(ctx context.Context, operation string, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return storageError(ctx, "begin "+operation, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return storageError(ctx, "commit "+operation, err)
	}
	return nil
}

// encodeJSON renders a value for a text column.
func encodeJSON(operation string, value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, storageFailure(operation, err)
	}
	return encoded, nil
}

// decodeJSON parses a stored text column back into a value.
func decodeJSON(operation, column string, raw []byte, target any) error {
	if err := json.Unmarshal(raw, target); err != nil {
		// The parse error would quote the malformed document, which for these
		// columns is a message body. Only the column name survives.
		return integrityError(operation, column)
	}
	return nil
}

// millisFromDuration renders a duration for a bigint column.
func millisFromDuration(value time.Duration) int64 {
	return int64(value / time.Millisecond)
}

// durationFromMillis reads a bigint column back as a duration.
func durationFromMillis(value int64) time.Duration {
	return time.Duration(value) * time.Millisecond
}

// nullableTime normalises an optional timestamp read from the database. pgx
// hands back a location that depends on the session, and every comparison in
// the channels package assumes UTC.
func nullableTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	normalized := value.UTC()
	return &normalized
}

// int32FromColumn narrows a stored integer that the Go type models as an int32.
func int32FromColumn(operation, column string, value int64) (int32, error) {
	if value < -(1<<31) || value > (1<<31)-1 {
		return 0, integrityError(operation, column)
	}
	return int32(value), nil
}

// uint16FromColumn narrows a stored delivery-target version. The column is an
// integer with a CHECK, so this only fires on an out-of-band write.
func uint16FromColumn(operation, column string, value int32) (uint16, error) {
	if value < 0 || value > 0xFFFF {
		return 0, integrityError(operation, column)
	}
	return uint16(value), nil
}
