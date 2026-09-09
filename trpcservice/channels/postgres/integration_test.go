package postgres_test

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/channelstest"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const (
	envIntegration     = "TRPC_SERVICE_SESSION_INTEGRATION"
	envPostgresDSN     = "TRPC_SERVICE_POSTGRES_DSN"
	integrationTimeout = 60 * time.Second
	schemaPrefix       = "channel_store_"
)

func requireDSN(t *testing.T) string {
	t.Helper()
	if os.Getenv(envIntegration) != "1" {
		t.Skipf("set %s=1 to run channel Store integration tests", envIntegration)
	}
	dsn := os.Getenv(envPostgresDSN)
	if dsn == "" {
		t.Skipf("set %s to run channel Store integration tests", envPostgresDSN)
	}
	return dsn
}

func setupContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), integrationTimeout)
}

func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func openPool(t *testing.T, dsn, schema string) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	if schema != "" {
		config.ConnConfig.RuntimeParams["search_path"] = schema
	}
	ctx, cancel := setupContext()
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx))
	t.Cleanup(pool.Close)
	return pool
}

func createSchema(t *testing.T, dsn string) string {
	t.Helper()
	schema := schemaPrefix + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
	admin := openPool(t, dsn, "")
	ctx, cancel := setupContext()
	defer cancel()
	_, err := admin.Exec(ctx, `CREATE SCHEMA `+quoteIdentifier(schema))
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := setupContext()
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx, `DROP SCHEMA `+quoteIdentifier(schema)+` CASCADE`); err != nil {
			t.Errorf("drop schema %s: %v", schema, err)
		}
	})
	return schema
}

func newIsolatedStore(t *testing.T, dsn string) channels.Store {
	t.Helper()
	schema := createSchema(t, dsn)
	pool := openPool(t, dsn, schema)
	ctx, cancel := setupContext()
	defer cancel()
	require.NoError(t, postgres.Migrate(ctx, pool))
	store, err := postgres.New(pool)
	require.NoError(t, err)
	return store
}

func TestIntegrationStoreConformance(t *testing.T) {
	dsn := requireDSN(t)
	channelstest.RunStoreSuite(t, func(t *testing.T) channels.Store {
		return newIsolatedStore(t, dsn)
	})
}

func TestIntegrationMigrateIsIdempotent(t *testing.T) {
	dsn := requireDSN(t)
	schema := createSchema(t, dsn)
	pool := openPool(t, dsn, schema)
	ctx, cancel := setupContext()
	defer cancel()
	require.NoError(t, postgres.Migrate(ctx, pool))
	require.NoError(t, postgres.Migrate(ctx, pool))
	store, err := postgres.New(pool)
	require.NoError(t, err)
	_, err = store.Accept(ctx, tenant.TenantContext{TenantID: "tenant-a"}, channelstest.Request(
		"tenant-a", "migration", "session-migration", "event-migration"))
	require.NoError(t, err)
}

// seedRun drives one event into a state an older database could be holding it
// in. started says whether the attempt reached the Runner, finished whether it
// then terminated.
func seedRun(
	t *testing.T,
	ctx context.Context,
	store channels.Store,
	suffix string,
	started, finished bool,
) (channels.RunToken, time.Time) {
	t.Helper()
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	request := channelstest.Request("tenant-a", suffix, "session-"+suffix, "event-"+suffix)
	_, err := store.Accept(ctx, scope, request)
	require.NoError(t, err)
	claim, ok, err := store.ClaimNextRun(ctx, scope, request.Envelope.SessionKey(),
		channels.ClaimRunRequest{
			ClaimToken: "claim-" + suffix,
			ClaimedBy:  "worker-main",
			Now:        request.Now,
		})
	require.NoError(t, err)
	require.True(t, ok)
	token := channels.RunToken{RunID: claim.Run.RunID, ClaimToken: claim.Run.ClaimToken}
	startedAt := channels.NormalizeTime(request.Now.Add(time.Second))
	if started {
		require.NoError(t, store.MarkRunStarted(ctx, scope, token, startedAt))
	}
	if finished {
		_, err = store.FinishRun(ctx, scope, channels.FinishRunRequest{
			Token:      token,
			Status:     channels.RunSucceeded,
			RevisionID: "revision-main",
			Stats:      channels.RunStats{EventCount: 1, ExecutionMillis: 5},
			Now:        startedAt.Add(time.Second),
		})
		require.NoError(t, err)
	}
	return token, startedAt
}

// TestIntegrationMigrateUpgradesAnOlderRunsTable is the case the fresh CREATE
// cannot cover: a database that was migrated before the permanent execution
// marker existed and already holds rows. The column has to appear, the rows
// that can be backfilled have to be, and the ones that cannot have to be left
// honestly empty rather than guessed at.
func TestIntegrationMigrateUpgradesAnOlderRunsTable(t *testing.T) {
	dsn := requireDSN(t)
	schema := createSchema(t, dsn)
	pool := openPool(t, dsn, schema)
	ctx, cancel := setupContext()
	defer cancel()
	require.NoError(t, postgres.Migrate(ctx, pool))
	store, err := postgres.New(pool)
	require.NoError(t, err)
	scope := tenant.TenantContext{TenantID: "tenant-a"}

	// The three shapes an older table could hold: an attempt that reached the
	// Runner, one that was claimed and never started, and one that finished.
	startedToken, startedAt := seedRun(t, ctx, store, "upgrade-started", true, false)
	idleToken, _ := seedRun(t, ctx, store, "upgrade-idle", false, false)
	finishedToken, _ := seedRun(t, ctx, store, "upgrade-finished", true, true)
	mixedToken, _ := seedRun(t, ctx, store, "upgrade-mixed", false, false)

	// Take the table back to the shape the older build left behind.
	_, err = pool.Exec(ctx, `ALTER TABLE channel_agent_runs DROP COLUMN first_execution_started_at`)
	require.NoError(t, err)

	// A fresh pool, because the connections above hold plans for a table this
	// test has just changed underneath them.
	upgradedPool := openPool(t, dsn, schema)
	require.NoError(t, postgres.Migrate(ctx, upgradedPool))
	upgraded, err := postgres.New(upgradedPool)
	require.NoError(t, err)

	run, err := upgraded.GetRun(ctx, scope, startedToken.RunID)
	require.NoError(t, err)
	require.Equal(t, startedAt, *run.ExecutionStartedAt)
	require.Equal(t, startedAt, *run.FirstExecutionStartedAt,
		"an attempt still carrying its start time is exactly backfillable")

	run, err = upgraded.GetRun(ctx, scope, idleToken.RunID)
	require.NoError(t, err)
	require.Nil(t, run.FirstExecutionStartedAt, "this attempt never reached the Runner")

	run, err = upgraded.GetRun(ctx, scope, finishedToken.RunID)
	require.NoError(t, err)
	require.Nil(t, run.ExecutionStartedAt)
	require.Nil(t, run.FirstExecutionStartedAt,
		"a row whose transient marker was already cleared cannot be recovered, "+
			"and the attempt counter cannot stand in for it")

	// Migrating again changes nothing, backfill included.
	require.NoError(t, postgres.Migrate(ctx, upgradedPool))
	run, err = upgraded.GetRun(ctx, scope, startedToken.RunID)
	require.NoError(t, err)
	require.Equal(t, startedAt, *run.FirstExecutionStartedAt)
	run, err = upgraded.GetRun(ctx, scope, finishedToken.RunID)
	require.NoError(t, err)
	require.Nil(t, run.FirstExecutionStartedAt)

	// And the upgraded table is fully writable: the attempt that never started
	// records both markers when it does.
	restartedAt := channels.NormalizeTime(startedAt.Add(time.Minute))
	require.NoError(t, upgraded.MarkRunStarted(ctx, scope, idleToken, restartedAt))
	run, err = upgraded.GetRun(ctx, scope, idleToken.RunID)
	require.NoError(t, err)
	require.Equal(t, restartedAt, *run.ExecutionStartedAt)
	require.Equal(t, restartedAt, *run.FirstExecutionStartedAt)

	// The window the backfill cannot close on its own: during a rolling deploy an
	// older build is still writing the transient marker and knows nothing about
	// the permanent one, so a row can go back to that shape after the migration
	// ran. The next write from a new build has to adopt the start that is already
	// on the row instead of stamping the moment it noticed.
	oldBuildStartedAt := channels.NormalizeTime(startedAt.Add(2 * time.Minute))
	_, err = upgradedPool.Exec(ctx, `UPDATE channel_agent_runs
		SET execution_started_at = $2, first_execution_started_at = NULL
		WHERE tenant_id = 'tenant-a' AND run_id = $1`, mixedToken.RunID, oldBuildStartedAt)
	require.NoError(t, err)
	noticedAt := channels.NormalizeTime(oldBuildStartedAt.Add(time.Hour))
	require.NoError(t, upgraded.MarkRunStarted(ctx, scope, mixedToken, noticedAt))
	run, err = upgraded.GetRun(ctx, scope, mixedToken.RunID)
	require.NoError(t, err)
	require.Equal(t, oldBuildStartedAt, *run.ExecutionStartedAt,
		"a repeated mark never moves the transient marker either")
	require.Equal(t, oldBuildStartedAt, *run.FirstExecutionStartedAt,
		"the permanent marker takes the start the row already carries, not the hour it was noticed")
}

// TestIntegrationDispatchGenerationSurvivesAConcurrentRequeue runs the
// announce/record interleaving across two connections, so the CAS is judged by
// PostgreSQL under real transactions rather than by one process's lock.
func TestIntegrationDispatchGenerationSurvivesAConcurrentRequeue(t *testing.T) {
	dsn := requireDSN(t)
	schema := createSchema(t, dsn)
	scannerPool := openPool(t, dsn, schema)
	workerPool := openPool(t, dsn, schema)
	ctx, cancel := setupContext()
	defer cancel()
	require.NoError(t, postgres.Migrate(ctx, scannerPool))
	scanner, err := postgres.New(scannerPool)
	require.NoError(t, err)
	worker, err := postgres.New(workerPool)
	require.NoError(t, err)

	scope := tenant.TenantContext{TenantID: "tenant-a"}
	request := channelstest.Request("tenant-a", "race", "session-race", "event-race")
	accepted, err := scanner.Accept(ctx, scope, request)
	require.NoError(t, err)
	ref := channels.RunRef{TenantID: "tenant-a", RunID: accepted.RunID}

	candidates, err := scanner.ListDispatchableRuns(ctx, channels.DispatchScanRequest{
		Now: request.Now, StaleAfter: time.Minute, Limit: 10,
	})
	require.NoError(t, err)
	require.Equal(t, []channels.RunDispatch{{RunRef: ref}}, candidates)

	// On the other connection a Worker takes the row and hands it back, all
	// before the scan gets to its note.
	claim, ok, err := worker.ClaimNextRun(ctx, scope, request.Envelope.SessionKey(),
		channels.ClaimRunRequest{
			ClaimToken: "claim-race",
			ClaimedBy:  "worker-main",
			Now:        request.Now,
		})
	require.NoError(t, err)
	require.True(t, ok)
	yieldAt := request.Now.Add(time.Second)
	outcome, err := worker.YieldRun(ctx, scope, channels.RunToken{
		RunID:      claim.Run.RunID,
		ClaimToken: claim.Run.ClaimToken,
	}, channels.YieldRunRequest{ErrorType: channels.ErrorRunCancelled, Now: yieldAt})
	require.NoError(t, err)
	require.Equal(t, channels.RequeueRetried, outcome)

	// The note names the generation that is over. It is refused even though the
	// row is due again by the time it lands.
	dueAt := channels.NormalizeTime(yieldAt.Add(channels.Backoff(1, accepted.RunID)))
	marked, err := scanner.MarkRunsDispatched(ctx, candidates, dueAt)
	require.NoError(t, err)
	require.Zero(t, marked)
	run, err := scanner.GetRun(ctx, scope, accepted.RunID)
	require.NoError(t, err)
	require.Nil(t, run.LastDispatchedAt)

	// So the next scan still finds it, and its note is taken.
	candidates, err = scanner.ListDispatchableRuns(ctx, channels.DispatchScanRequest{
		Now: dueAt, StaleAfter: time.Minute, Limit: 10,
	})
	require.NoError(t, err)
	require.Equal(t, []channels.RunDispatch{{RunRef: ref, Attempt: 1}}, candidates)
	marked, err = scanner.MarkRunsDispatched(ctx, candidates, dueAt)
	require.NoError(t, err)
	require.Equal(t, 1, marked)
	run, err = scanner.GetRun(ctx, scope, accepted.RunID)
	require.NoError(t, err)
	require.Equal(t, dueAt, *run.LastDispatchedAt)

	// Two scanners overlapping: the older reading still matches the row, and
	// still must not pull it back into the next scan.
	later := channels.NormalizeTime(dueAt.Add(time.Minute))
	marked, err = worker.MarkRunsDispatched(ctx, candidates, later)
	require.NoError(t, err)
	require.Equal(t, 1, marked)
	marked, err = scanner.MarkRunsDispatched(ctx, candidates, dueAt)
	require.NoError(t, err)
	require.Equal(t, 1, marked, "the row still matched the CAS the older cycle published for")
	run, err = scanner.GetRun(ctx, scope, accepted.RunID)
	require.NoError(t, err)
	require.Equal(t, later, *run.LastDispatchedAt)
}

func TestIntegrationConcurrentMigrateIsSafe(t *testing.T) {
	dsn := requireDSN(t)
	schema := createSchema(t, dsn)
	const workers = 6
	pools := make([]*pgxpool.Pool, workers)
	for i := range pools {
		pools[i] = openPool(t, dsn, schema)
	}
	ctx, cancel := setupContext()
	defer cancel()
	errs := make([]error, workers)
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(workers)
	done.Add(workers)
	for i, pool := range pools {
		i, pool := i, pool
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
		require.NoErrorf(t, err, "migration worker %d", i)
	}
}
