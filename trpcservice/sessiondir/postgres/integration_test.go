package postgres_test

import (
	"context"
	"errors"
	"math"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir/sessiondirtest"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
)

// These are the only tests in this package that touch a real server, and they
// stay off unless the operator asks for them. `go test ./...` on a machine
// with no postgres and no network must stay green, so the gate is checked
// before any connection is built.
//
// They reuse the session spike's compose file and environment variables rather
// than adding a third set:
//
//	docker compose -f deploy/docker-compose.session.yml up -d --wait
//	TRPC_SERVICE_SESSION_INTEGRATION=1 \
//	TRPC_SERVICE_POSTGRES_DSN='postgres://trpc:trpc-local-dev@127.0.0.1:55432/trpc_session?sslmode=disable' \
//	go test -race -timeout 300s ./trpcservice/sessiondir/...
//
// Those credentials are the compose file's development defaults; see
// docs/session-backend.md.
const (
	// envIntegration must be "1" for anything in this file to run.
	envIntegration = "TRPC_SERVICE_SESSION_INTEGRATION"
	// envPostgresDSN carries the connection string.
	envPostgresDSN = "TRPC_SERVICE_POSTGRES_DSN"

	// integrationTimeout bounds every individual test and every setup or
	// teardown statement. A reachable server answers in milliseconds; this
	// only stops an unreachable one from hanging until the package timeout.
	integrationTimeout = 60 * time.Second

	// schemaPrefix namespaces the throwaway schemas these tests create. Every
	// test gets a schema of its own and drops it, so nothing accumulates in the
	// database between runs and two runs never see each other's rows — which
	// also makes `-count=3` meaningful rather than a second run finding the
	// first run's pins.
	schemaPrefix = "session_dir_"
)

func requireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv(envIntegration) != "1" {
		t.Skipf("set %s=1 to run session directory integration tests", envIntegration)
	}
}

func requireDSN(t *testing.T) string {
	t.Helper()
	requireIntegration(t)
	dsn := os.Getenv(envPostgresDSN)
	if dsn == "" {
		t.Skipf("set %s to run this test", envPostgresDSN)
	}
	return dsn
}

// integrationContext bounds a test body.
func integrationContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	t.Cleanup(cancel)
	return ctx
}

// setupContext returns a context for work that must not inherit a test body's
// deadline: schema creation, migration and teardown. A cleanup in particular
// runs after the body's context is already cancelled, so a teardown that
// inherited it would leave its schema behind on every run.
func setupContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), integrationTimeout)
}

func uniqueSchemaName() string {
	return schemaPrefix + uuid.New().String()[:8]
}

// quoteIdentifier quotes a schema name for DDL. The names here are generated
// from a fixed prefix and hex, so this is belt and braces, but a schema name
// is the one thing in this file that cannot be a bind parameter.
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// openPool opens an independent pool. When schema is not empty every
// connection the pool opens is pinned to it.
func openPool(t *testing.T, dsn string, schema string) *pgxpool.Pool {
	t.Helper()
	return openPoolWithParams(t, dsn, schema, nil)
}

// openPoolWithParams is openPool with extra connection runtime parameters, for
// the one test that has to run against a server configured differently from
// this one. They are set the same way search_path is, in the startup packet, so
// they apply to every connection rather than to whichever one a SET happened to
// be checked out on.
func openPoolWithParams(
	t *testing.T,
	dsn string,
	schema string,
	params map[string]string,
) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	if schema != "" {
		// Set on the pool config rather than with a SET after checkout: this
		// way every connection the pool opens, including one it opens later to
		// replace a dropped one, carries the same search_path.
		config.ConnConfig.RuntimeParams["search_path"] = schema
	}
	for name, value := range params {
		config.ConnConfig.RuntimeParams[name] = value
	}

	ctx, cancel := setupContext()
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	// NewWithConfig does not connect, so without this an unreachable server
	// would first be reported from somewhere in the middle of a test body.
	require.NoError(t, pool.Ping(ctx))
	// Close is guarded by a sync.Once, so a test that closes a pool early to
	// simulate a worker going away can still leave this cleanup registered.
	t.Cleanup(pool.Close)
	return pool
}

// createSchema makes an empty schema and schedules its removal. It does not
// migrate: the concurrent-migration and missing-table tests both need a schema
// that is still empty.
//
// Cleanup order matters and is LIFO. The admin pool is registered first so it
// closes last, the drop is registered second so it runs after every pool a
// test opens later has closed, and DROP SCHEMA therefore never waits on a live
// connection.
func createSchema(t *testing.T, dsn string) string {
	t.Helper()
	schema := uniqueSchemaName()

	// The schema has to exist before a pool can point search_path at it:
	// unqualified DDL against a search_path naming only a missing schema fails
	// with "no schema has been selected to create in".
	admin := openPool(t, dsn, "")

	ctx, cancel := setupContext()
	defer cancel()
	_, err := admin.Exec(ctx, `CREATE SCHEMA `+quoteIdentifier(schema))
	require.NoError(t, err)

	t.Cleanup(func() {
		dropCtx, cancelDrop := setupContext()
		defer cancelDrop()
		// Reported with Errorf rather than require: a failure here still has to
		// fall through to closing the admin pool, and require would abort the
		// remaining cleanups.
		if _, err := admin.Exec(dropCtx, `DROP SCHEMA `+quoteIdentifier(schema)+` CASCADE`); err != nil {
			t.Errorf("drop schema %s: %v", schema, err)
		}
	})
	return schema
}

// newSchema makes an empty schema and migrates it.
func newSchema(t *testing.T, dsn string) string {
	t.Helper()
	schema := createSchema(t, dsn)

	ctx, cancel := setupContext()
	defer cancel()
	require.NoError(t, postgres.Migrate(ctx, openPool(t, dsn, schema)))
	return schema
}

func newDirectory(t *testing.T, pool *pgxpool.Pool) *postgres.Directory {
	t.Helper()
	directory, err := postgres.New(pool)
	require.NoError(t, err)
	return directory
}

// newIsolatedDirectory is the one-pool, one-schema case most tests want.
func newIsolatedDirectory(t *testing.T, dsn string) *postgres.Directory {
	t.Helper()
	return newDirectory(t, openPool(t, dsn, newSchema(t, dsn)))
}

// keyArguments binds a key in the order every statement in this file uses. It
// is spelled out here rather than imported so the tests reach the table the
// way an operator would, not the way the implementation does.
func keyArguments(key sessiondir.Key) []any {
	return []any{key.TenantID, key.AppID, key.PrincipalID, key.SessionID, int64(key.Epoch)}
}

const sessionKeyWhere = `WHERE tenant_id = $1 AND agent_app_id = $2 AND principal_id = $3
	AND session_id = $4 AND epoch = $5`

// countPins reports how many rows the whole table holds. Several assertions
// turn on "and only one row exists", which no amount of reading through the
// Directory can show.
func countPins(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int {
	t.Helper()
	var count int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM sessions`).Scan(&count))
	return count
}

type storedPin struct {
	RevisionID string
	Epoch      int64
	CreatedAt  time.Time
}

// loadPin reads a row directly, so a test can assert on what is stored rather
// than on what the implementation chose to return.
func loadPin(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	key sessiondir.Key,
) (storedPin, bool) {
	t.Helper()
	var pin storedPin
	err := pool.QueryRow(ctx,
		`SELECT pinned_revision_id, epoch, created_at FROM sessions `+sessionKeyWhere,
		keyArguments(key)...,
	).Scan(&pin.RevisionID, &pin.Epoch, &pin.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return storedPin{}, false
	}
	require.NoError(t, err)
	return pin, true
}

// requireStorageFailure is the classification assertion. Reporting a database
// fault as any tenant sentinel would put it on a 4xx path, and the caller would
// act on it: ErrNotFound in particular reads as "this session has no pin yet".
func requireStorageFailure(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.ErrorIs(t, err, postgres.ErrStorage)
	for _, sentinel := range []error{
		tenant.ErrInvalidArgument,
		tenant.ErrNotFound,
		tenant.ErrAlreadyExists,
		tenant.ErrTenantScope,
		tenant.ErrConfigIntegrity,
	} {
		require.NotErrorIs(t, err, sentinel)
	}
}

// TestIntegrationDirectoryConformance runs the same contract the in-memory
// reference runs, against PostgreSQL. Each subtest gets a schema of its own,
// because the suite reuses fixed ids such as "tenant-a" across subtests.
//
// This is where cancelled and nil contexts, invalid keys and candidates, the
// full uint32 epoch range and the basic pin-once behaviour are covered for this
// implementation; the tests below are the ones that only a real server can
// answer.
func TestIntegrationDirectoryConformance(t *testing.T) {
	dsn := requireDSN(t)
	sessiondirtest.RunDirectorySuite(t, func(t *testing.T) sessiondir.Directory {
		return newIsolatedDirectory(t, dsn)
	})
}

// TestIntegrationEnsurePinAgreesAcrossPoolsAndInstances is the acceptance case
// this implementation exists for.
//
// Thirty-two concurrent first runs of one session, each with a different
// candidate, released together, alternating between two independent pools and
// two Directory values. Nothing in this arrangement shares a process-level
// lock, so the single winner can only come from the primary-key index.
//
// The assertions are of two kinds: every caller was told the same revision,
// and the table holds exactly one row. Either alone is too weak — a directory
// that returned each caller its own candidate would still leave one row, and
// one that agreed in Go could still have written thirty-two.
func TestIntegrationEnsurePinAgreesAcrossPoolsAndInstances(t *testing.T) {
	dsn := requireDSN(t)
	schema := newSchema(t, dsn)
	firstPool := openPool(t, dsn, schema)
	secondPool := openPool(t, dsn, schema)
	first := newDirectory(t, firstPool)
	second := newDirectory(t, secondPool)

	ctx := integrationContext(t)
	key := sessiondirtest.Key()
	calls := sessiondirtest.ContendOneKey(key, first, second)
	require.Len(t, calls, sessiondirtest.Callers)

	results := sessiondirtest.EnsurePinConcurrently(t, ctx, calls)
	winner := sessiondirtest.RequireOneWinner(t, ctx, first, key, results)

	require.Equal(t, 1, countPins(t, ctx, firstPool),
		"thirty-two contended first runs must leave exactly one pin")
	stored, found := loadPin(t, ctx, secondPool, key)
	require.True(t, found)
	require.Equal(t, winner, stored.RevisionID,
		"the stored row must be the revision every caller was told")

	// The instance that did not perform the read above agrees.
	pinned, found, err := second.GetPin(ctx, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, winner, pinned)

	// A candidate arriving after the contention is over is discarded, and the
	// winning row is not rewritten: created_at moving would mean the idempotent
	// path is a write, however unchanged the value looks.
	late, err := second.EnsurePin(ctx, key, "revision-late")
	require.NoError(t, err)
	require.Equal(t, winner, late)

	after, found := loadPin(t, ctx, firstPool, key)
	require.True(t, found)
	require.Equal(t, winner, after.RevisionID)
	require.True(t, stored.CreatedAt.Equal(after.CreatedAt),
		"an existing pin must never be rewritten: created_at moved from %s to %s",
		stored.CreatedAt, after.CreatedAt)
	require.Equal(t, 1, countPins(t, ctx, firstPool))
}

// TestIntegrationEnsurePinAgreesUnderRepeatableReadDefault runs the contended
// case against a server default the implementation does not want.
//
// EnsurePin asks for READ COMMITTED explicitly, and every other test here runs
// on pools that were already going to get it, so deleting that request would
// leave them all green. It is not cosmetic. Under REPEATABLE READ the losing
// insert cannot skip a conflicting row it cannot see, so it waits for the
// winner and then aborts with "could not serialize access due to concurrent
// update" (SQLSTATE 40001). EnsurePin returns on any error, so the retry loop
// never sees it, and thirty-one of thirty-two first runs would be answered with
// ErrStorage instead of the winner.
//
// This is the test that fails when pgx.TxOptions{IsoLevel: pgx.ReadCommitted}
// is dropped from ensurePinOnce.
func TestIntegrationEnsurePinAgreesUnderRepeatableReadDefault(t *testing.T) {
	dsn := requireDSN(t)
	schema := newSchema(t, dsn)
	hostile := map[string]string{"default_transaction_isolation": "repeatable read"}
	firstPool := openPoolWithParams(t, dsn, schema, hostile)
	secondPool := openPoolWithParams(t, dsn, schema, hostile)

	ctx := integrationContext(t)
	// Without this the test quietly degrades into a second copy of the
	// read-committed case, and would keep passing with the isolation level
	// removed — which is the one thing it is here to notice.
	for name, pool := range map[string]*pgxpool.Pool{"first": firstPool, "second": secondPool} {
		var level string
		require.NoError(t, pool.QueryRow(ctx, `SHOW default_transaction_isolation`).Scan(&level))
		require.Equal(t, "repeatable read", level,
			"the %s pool did not take the hostile server default, so this test proves nothing", name)
	}

	first := newDirectory(t, firstPool)
	second := newDirectory(t, secondPool)
	key := sessiondirtest.Key()
	results := sessiondirtest.EnsurePinConcurrently(t, ctx, sessiondirtest.ContendOneKey(key, first, second))
	winner := sessiondirtest.RequireOneWinner(t, ctx, first, key, results)

	require.Equal(t, 1, countPins(t, ctx, firstPool),
		"the isolation level must not change how many rows a contended first run leaves")
	stored, found := loadPin(t, ctx, secondPool, key)
	require.True(t, found)
	require.Equal(t, winner, stored.RevisionID)
}

// The naive algorithm this package deliberately does not ship, in the two
// shapes it is usually written in. Both are used only by the test below.
const (
	naiveSelectSQL = `SELECT pinned_revision_id FROM sessions ` + sessionKeyWhere

	naiveInsertSQL = `INSERT INTO sessions (
			tenant_id, agent_app_id, principal_id, session_id, epoch,
			pinned_revision_id, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)`

	naiveInsertIgnoringConflictSQL = naiveInsertSQL + ` ON CONFLICT DO NOTHING`
)

// nonAtomicDirectory implements the contract the wrong way on purpose: it asks
// whether a pin exists and then writes one, with a window in between.
//
// afterRead widens that window to certainty. Without it the failure is merely
// very likely, and a test that is merely very likely to fail is not evidence
// that the shipped algorithm is what makes the real test pass.
type nonAtomicDirectory struct {
	pool      *pgxpool.Pool
	insertSQL string
	afterRead func()
}

var _ sessiondir.Directory = (*nonAtomicDirectory)(nil)

func (d *nonAtomicDirectory) GetPin(
	ctx context.Context,
	key sessiondir.Key,
) (string, bool, error) {
	var pinned string
	err := d.pool.QueryRow(ctx, naiveSelectSQL, keyArguments(key)...).Scan(&pinned)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return pinned, true, nil
}

func (d *nonAtomicDirectory) EnsurePin(
	ctx context.Context,
	key sessiondir.Key,
	candidateRevisionID string,
) (string, error) {
	pinned, found, err := d.GetPin(ctx, key)
	if err != nil {
		return "", err
	}
	if found {
		return pinned, nil
	}
	// Every caller has now decided, on its own stale read, that it is first.
	d.afterRead()

	arguments := append(keyArguments(key), candidateRevisionID, time.Now().UTC())
	if _, err := d.pool.Exec(ctx, d.insertSQL, arguments...); err != nil {
		return "", err
	}
	return candidateRevisionID, nil
}

// rendezvous returns a function that blocks until all parties have called it.
func rendezvous(parties int) func() {
	var waitGroup sync.WaitGroup
	waitGroup.Add(parties)
	return func() {
		waitGroup.Done()
		waitGroup.Wait()
	}
}

// TestIntegrationNonAtomicEnsurePinIsRejected is the reverse of the test above.
// It runs the same agreement check against directories that lack the atomic
// first write, and requires the check to complain — a mutation test kept as
// code rather than performed by hand, so a future edit that quietly drops the
// arbitration is caught by the suite instead of by review.
//
// The two mutants fail differently, and the second is the reason the shipped
// implementation re-reads:
//
//   - "read then insert" is loud. Thirty-one callers get a duplicate-key error
//     and no revision at all, so a first run fails for no reason the user did.
//   - "read then insert, conflict ignored" is silent. Every caller succeeds and
//     is told its own candidate, while one row is stored. Thirty-one
//     conversations then run against a revision the directory did not pin,
//     which is exactly the drift this whole package exists to prevent.
func TestIntegrationNonAtomicEnsurePinIsRejected(t *testing.T) {
	dsn := requireDSN(t)
	for name, insertSQL := range map[string]string{
		"read then insert":                   naiveInsertSQL,
		"read then insert, conflict ignored": naiveInsertIgnoringConflictSQL,
	} {
		t.Run(name, func(t *testing.T) {
			pool := openPool(t, dsn, newSchema(t, dsn))
			directory := &nonAtomicDirectory{
				pool:      pool,
				insertSQL: insertSQL,
				afterRead: rendezvous(sessiondirtest.Callers),
			}

			ctx := integrationContext(t)
			key := sessiondirtest.Key()
			results := sessiondirtest.EnsurePinConcurrently(
				t, ctx, sessiondirtest.ContendOneKey(key, directory),
			)

			_, err := sessiondirtest.AgreementError(ctx, directory, key, results)
			require.Error(t, err,
				"a directory without an atomic first write must not satisfy the contract")
			t.Logf("rejected as expected: %v", err)

			// The store itself is still consistent: the primary key holds even
			// when the algorithm above it does not. The damage is entirely in
			// what the callers were told, which is why counting rows alone
			// would not have caught this.
			require.Equal(t, 1, countPins(t, ctx, pool))
		})
	}
}

// TestIntegrationKeyFieldsAreSeparateRows proves the separation at the storage
// layer. The shared suite already proves it through the interface; what only a
// real table can show is that the composite primary key is what provides it,
// rather than five conversations sharing one row and the last write winning.
func TestIntegrationKeyFieldsAreSeparateRows(t *testing.T) {
	dsn := requireDSN(t)
	pool := openPool(t, dsn, newSchema(t, dsn))
	directory := newDirectory(t, pool)

	ctx := integrationContext(t)
	base := sessiondirtest.Key()
	winner, err := directory.EnsurePin(ctx, base, "revision-base")
	require.NoError(t, err)
	require.Equal(t, "revision-base", winner)

	mutations := sessiondirtest.MutateKey()
	for name, mutate := range mutations {
		neighbour := base
		mutate(&neighbour)
		expected := "revision-" + name

		pinned, found, getErr := directory.GetPin(ctx, neighbour)
		require.NoError(t, getErr)
		require.Falsef(t, found, "a different %s must not inherit the pin", name)
		require.Empty(t, pinned)

		winner, err := directory.EnsurePin(ctx, neighbour, expected)
		require.NoError(t, err)
		require.Equal(t, expected, winner)
	}

	require.Equal(t, 1+len(mutations), countPins(t, ctx, pool),
		"every field of the key must address a row of its own")

	// The tenant is the one that must not be reachable across, so it is
	// asserted on its own rather than only as one entry in the loop. Same app,
	// same principal, same session id, different tenant: different pin, and the
	// row really does carry the other tenant's id.
	otherTenant := base
	otherTenant.TenantID = "tenant-b"
	require.Equal(t, base.SessionID, otherTenant.SessionID)

	pinned, found, err := directory.GetPin(ctx, otherTenant)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "revision-tenant", pinned)

	stored, found := loadPin(t, ctx, pool, base)
	require.True(t, found)
	require.Equal(t, "revision-base", stored.RevisionID,
		"the original tenant's pin must be untouched by its neighbour")
}

// TestIntegrationStoresMaxEpochExactly pins the upper end of the epoch column
// against a real database.
//
// The shared suite proves both implementations accept the whole uint32 range.
// What only a real server can show is that the boundary value survives the
// uint32 to bigint conversion and the column itself without wrapping: an
// integer column would reject it outright, and a signed reading of it would
// come back negative.
func TestIntegrationStoresMaxEpochExactly(t *testing.T) {
	dsn := requireDSN(t)
	pool := openPool(t, dsn, newSchema(t, dsn))
	directory := newDirectory(t, pool)

	ctx := integrationContext(t)
	key := sessiondirtest.Key()
	key.Epoch = math.MaxUint32

	winner, err := directory.EnsurePin(ctx, key, "revision-1")
	require.NoError(t, err)
	require.Equal(t, "revision-1", winner)

	stored, found := loadPin(t, ctx, pool, key)
	require.True(t, found)
	require.Equal(t, int64(math.MaxUint32), stored.Epoch)

	pinned, found, err := directory.GetPin(ctx, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "revision-1", pinned)
}

// insertPinDirectly writes a row the way an operator or another program would,
// bypassing the Directory entirely. That is the only way to reach the two
// states this package cannot itself produce: a value outside what Go's types
// can express, and one that would never pass validation on the way in.
func insertPinDirectly(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	key sessiondir.Key,
	epoch int64,
	revisionID string,
) error {
	t.Helper()
	_, err := pool.Exec(ctx, `INSERT INTO sessions (
			tenant_id, agent_app_id, principal_id, session_id, epoch,
			pinned_revision_id, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, now())`,
		key.TenantID, key.AppID, key.PrincipalID, key.SessionID, epoch, revisionID)
	return err
}

// TestIntegrationEpochCheckRejectsOutOfRange is the negative half of the epoch
// evidence. TestIntegrationStoresMaxEpochExactly proves the range is wide
// enough; on its own it would still pass if the CHECK were dropped or weakened
// on one side, because a bigint holds both of these values happily.
//
// Neither value is expressible as a Go uint32, so they can only arrive out of
// band — which is exactly the traffic the CHECK is a backstop against. The
// constraint is asserted by name so that replacing it with a differently-scoped
// one is a failure rather than a silent substitution.
func TestIntegrationEpochCheckRejectsOutOfRange(t *testing.T) {
	dsn := requireDSN(t)
	pool := openPool(t, dsn, newSchema(t, dsn))
	ctx := integrationContext(t)

	for name, epoch := range map[string]int64{
		"below zero":           -1,
		"above the uint32 max": math.MaxUint32 + 1,
	} {
		t.Run(name, func(t *testing.T) {
			key := sessiondirtest.Key()
			key.SessionID = "conversation-out-of-range"
			err := insertPinDirectly(t, ctx, pool, key, epoch, "revision-1")
			require.Error(t, err, "epoch %d must not be storable", epoch)

			var pgErr *pgconn.PgError
			require.ErrorAs(t, err, &pgErr)
			// 23514 is check_violation.
			require.Equal(t, "23514", pgErr.Code)
			require.Equal(t, "sessions_epoch_check", pgErr.ConstraintName)
		})
	}

	require.Equal(t, 0, countPins(t, ctx, pool),
		"a rejected epoch must leave nothing behind")
}

// TestIntegrationPinSurvivesPoolReplacement is the worker-restart case, in the
// only form that proves anything: the pool that wrote the pin is closed before
// the pin is read back, and the reader is a new Directory on a new pool. Two
// objects sharing one live pool would prove only that the pool has a cache.
func TestIntegrationPinSurvivesPoolReplacement(t *testing.T) {
	dsn := requireDSN(t)
	schema := newSchema(t, dsn)

	ctx := integrationContext(t)
	key := sessiondirtest.Key()

	firstPool := openPool(t, dsn, schema)
	first := newDirectory(t, firstPool)
	winner, err := first.EnsurePin(ctx, key, "revision-1")
	require.NoError(t, err)
	require.Equal(t, "revision-1", winner)

	// The worker that pinned the session goes away, with every connection it
	// held. Closing is idempotent, so the cleanup openPool registered still
	// runs harmlessly at the end of the test.
	firstPool.Close()
	_, _, err = first.GetPin(ctx, key)
	requireStorageFailure(t, err)

	// A different worker: new pool, new connections, new Directory. Nothing is
	// shared with the one that wrote the pin except the database.
	secondPool := openPool(t, dsn, schema)
	second := newDirectory(t, secondPool)

	pinned, found, err := second.GetPin(ctx, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "revision-1", pinned,
		"a pin must outlive the pool and the process that wrote it")

	// The first run after the restart arrives with the revision that is current
	// now, and is still told the one the conversation started on.
	adopted, err := second.EnsurePin(ctx, key, "revision-2")
	require.NoError(t, err)
	require.Equal(t, "revision-1", adopted)
	require.Equal(t, 1, countPins(t, ctx, secondPool))
}

// TestIntegrationMigrateIsIdempotent covers the normal case of every process
// migrating on startup.
func TestIntegrationMigrateIsIdempotent(t *testing.T) {
	dsn := requireDSN(t)
	pool := openPool(t, dsn, createSchema(t, dsn))

	ctx := integrationContext(t)
	require.NoError(t, postgres.Migrate(ctx, pool))
	require.NoError(t, postgres.Migrate(ctx, pool))

	// The schema is usable, not merely present.
	directory := newDirectory(t, pool)
	winner, err := directory.EnsurePin(ctx, sessiondirtest.Key(), "revision-1")
	require.NoError(t, err)
	require.Equal(t, "revision-1", winner)
}

// TestIntegrationConcurrentMigrateIsSafe is the test the advisory lock exists
// for. CREATE TABLE IF NOT EXISTS alone does not make concurrent migration
// safe: two sessions both pass the existence check and the loser fails on
// pg_type's unique index. Without pg_advisory_xact_lock this fails.
func TestIntegrationConcurrentMigrateIsSafe(t *testing.T) {
	dsn := requireDSN(t)
	schema := createSchema(t, dsn)

	const workers = 6
	// Independent pools, because two workers migrating on startup are two
	// processes, not two goroutines sharing a connection pool.
	pools := make([]*pgxpool.Pool, workers)
	for i := range pools {
		pools[i] = openPool(t, dsn, schema)
	}

	ctx := integrationContext(t)
	errs := make([]error, workers)
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(workers)
	done.Add(workers)
	for i, pool := range pools {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			errs[i] = postgres.Migrate(ctx, pool)
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()

	for i, err := range errs {
		require.NoErrorf(t, err, "worker %d failed to migrate", i)
	}

	directory := newDirectory(t, pools[0])
	winner, err := directory.EnsurePin(ctx, sessiondirtest.Key(), "revision-1")
	require.NoError(t, err)
	require.Equal(t, "revision-1", winner)
}

// TestIntegrationDirectoriesShareOnePool pins the ownership contract: a
// Directory borrows its pool and never closes it, so several directories can
// share one and each sees the others' writes.
func TestIntegrationDirectoriesShareOnePool(t *testing.T) {
	dsn := requireDSN(t)
	pool := openPool(t, dsn, newSchema(t, dsn))
	writer := newDirectory(t, pool)
	reader := newDirectory(t, pool)

	ctx := integrationContext(t)
	key := sessiondirtest.Key()
	_, err := writer.EnsurePin(ctx, key, "revision-1")
	require.NoError(t, err)

	pinned, found, err := reader.GetPin(ctx, key)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "revision-1", pinned)

	// Neither directory closed the pool it borrowed.
	require.NoError(t, pool.Ping(ctx))
}

// TestIntegrationStorageFailuresAreClassified covers the failure paths a
// caller must never mistake for a domain answer.
func TestIntegrationStorageFailuresAreClassified(t *testing.T) {
	dsn := requireDSN(t)
	key := sessiondirtest.Key()

	t.Run("missing table", func(t *testing.T) {
		// A schema that exists but was never migrated: the process started
		// against a database nobody had brought up to date.
		directory := newDirectory(t, openPool(t, dsn, createSchema(t, dsn)))
		ctx := integrationContext(t)

		_, _, err := directory.GetPin(ctx, key)
		requireStorageFailure(t, err)
		_, err = directory.EnsurePin(ctx, key, "revision-1")
		requireStorageFailure(t, err)
	})

	t.Run("unavailable store", func(t *testing.T) {
		pool := openPool(t, dsn, newSchema(t, dsn))
		directory := newDirectory(t, pool)
		ctx := integrationContext(t)
		pool.Close()

		_, _, err := directory.GetPin(ctx, key)
		requireStorageFailure(t, err)
		_, err = directory.EnsurePin(ctx, key, "revision-1")
		requireStorageFailure(t, err)
	})

	t.Run("canceled context outranks an unavailable store", func(t *testing.T) {
		// Both faults are present at once. The context has to win, which also
		// shows it is checked before the pool is reached: an entry point that
		// went to the store first would report the closed pool instead.
		pool := openPool(t, dsn, newSchema(t, dsn))
		directory := newDirectory(t, pool)
		pool.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, _, err := directory.GetPin(ctx, key)
		require.ErrorIs(t, err, context.Canceled)
		require.NotErrorIs(t, err, postgres.ErrStorage)
		_, err = directory.EnsurePin(ctx, key, "revision-1")
		require.ErrorIs(t, err, context.Canceled)
		require.NotErrorIs(t, err, postgres.ErrStorage)
	})
}

// TestIntegrationCorruptStoredPinIsAStorageFault covers the row this package
// could not have written: a pinned_revision_id that is not a well-formed
// revision id. The column is only text NOT NULL, so an operator's UPDATE, a
// migration that backfilled badly, or corruption can put one there.
//
// Handing it back would fail open at the storage boundary in two ways, and the
// quieter one is the worse one. A malformed id is validated again by the
// revision resolver, becomes tenant.ErrInvalidArgument, and the chat API
// answers 400 — a database fault charged to the caller. An empty one is not
// rejected anywhere downstream: the resolver reads "" as "no pin was given" and
// falls through to the app's current default revision, so a corrupt row moves a
// pinned conversation onto whatever is published now and answers 200.
//
// Both must come back as ErrStorage, and neither may match a tenant sentinel.
// A CHECK constraint would not make this test redundant: rows that predate it,
// or arrive while it is dropped, still have to be read safely.
func TestIntegrationCorruptStoredPinIsAStorageFault(t *testing.T) {
	dsn := requireDSN(t)

	for name, corrupt := range map[string]string{
		"empty":             "",
		"embedded space":    "revision invalid",
		"path separator":    "revision/1",
		"leading dash":      "-revision-1",
		"control character": "revision-1\nrevision-2",
	} {
		t.Run(name, func(t *testing.T) {
			pool := openPool(t, dsn, newSchema(t, dsn))
			directory := newDirectory(t, pool)
			ctx := integrationContext(t)
			key := sessiondirtest.Key()
			require.NoError(t, insertPinDirectly(t, ctx, pool, key, int64(key.Epoch), corrupt))

			// A read must not pass it on, and must not report it as "no pin
			// yet" either: that answer would let the session adopt a fresh
			// revision, which is the same fail-open by a different route.
			pinned, found, err := directory.GetPin(ctx, key)
			requireStorageFailure(t, err)
			require.Empty(t, pinned)
			require.False(t, found)

			// EnsurePin reaches the same value down its conflict-then-read
			// path, where returning it would present corruption as a winner.
			winner, err := directory.EnsurePin(ctx, key, "revision-1")
			requireStorageFailure(t, err)
			require.Empty(t, winner)

			// The refusal is a read-side judgement. Nothing may be repaired,
			// overwritten or deleted underneath the operator who has to
			// diagnose it.
			stored, found := loadPin(t, ctx, pool, key)
			require.True(t, found)
			require.Equal(t, corrupt, stored.RevisionID)
			require.Equal(t, 1, countPins(t, ctx, pool))
		})
	}
}
