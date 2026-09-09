package postgres_test

import (
	"context"
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant/tenanttest"
	"github.com/stretchr/testify/require"
)

// These are the only tests in this package that touch a real server, and they
// stay off unless the operator asks for them. `go test ./...` on a machine
// with no postgres and no network must stay green, so the gate is checked
// before any connection is built.
//
// They reuse the session spike's compose file and environment variables rather
// than adding a second set:
//
//	docker compose -f deploy/docker-compose.session.yml up -d --wait
//	TRPC_SERVICE_SESSION_INTEGRATION=1 \
//	TRPC_SERVICE_POSTGRES_DSN='postgres://trpc:trpc-local-dev@127.0.0.1:55432/trpc_session?sslmode=disable' \
//	go test -race -timeout 300s ./trpcservice/tenant/...
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

	// schemaPrefix namespaces the throwaway schemas these tests create. Unlike
	// the session spike, which shares one set of tables, every test here gets
	// a schema of its own and drops it, so nothing accumulates in the database
	// between runs and two runs never see each other's rows.
	schemaPrefix = "tenant_ctl_"
)

func requireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv(envIntegration) != "1" {
		t.Skipf("set %s=1 to run tenant control-plane integration tests", envIntegration)
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
	config, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	if schema != "" {
		// Set on the pool config rather than with a SET after checkout: this
		// way every connection the pool opens, including one it opens later to
		// replace a dropped one, carries the same search_path.
		config.ConnConfig.RuntimeParams["search_path"] = schema
	}

	ctx, cancel := setupContext()
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	// NewWithConfig does not connect, so without this an unreachable server
	// would first be reported from somewhere in the middle of a test body.
	require.NoError(t, pool.Ping(ctx))
	t.Cleanup(pool.Close)
	return pool
}

// createSchema makes an empty schema and schedules its removal. It does not
// migrate: the concurrent-migration test needs a schema that is still empty.
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

func newRepository(t *testing.T, pool *pgxpool.Pool) *postgres.Repository {
	t.Helper()
	repository, err := postgres.New(pool)
	require.NoError(t, err)
	return repository
}

// newIsolatedRepository is the one-pool, one-schema case most tests want.
func newIsolatedRepository(t *testing.T, dsn string) *postgres.Repository {
	t.Helper()
	return newRepository(t, openPool(t, dsn, newSchema(t, dsn)))
}

// TestIntegrationRepositoryConformance runs the same contract the in-memory
// reference runs, against PostgreSQL. Each subtest gets a schema of its own,
// because the suite reuses fixed ids such as "tenant-a" across subtests.
func TestIntegrationRepositoryConformance(t *testing.T) {
	dsn := requireDSN(t)
	tenanttest.RunRepositorySuite(t, func(t *testing.T) tenant.Repository {
		return newIsolatedRepository(t, dsn)
	})
}

// TestIntegrationMigrateIsIdempotent covers the normal case of every process
// migrating on startup.
func TestIntegrationMigrateIsIdempotent(t *testing.T) {
	dsn := requireDSN(t)
	schema := createSchema(t, dsn)
	pool := openPool(t, dsn, schema)

	ctx := integrationContext(t)
	require.NoError(t, postgres.Migrate(ctx, pool))
	require.NoError(t, postgres.Migrate(ctx, pool))

	// The schema is usable, not merely present.
	repository := newRepository(t, pool)
	tenanttest.SeedTenant(t, ctx, repository, "tenant-a", tenant.StatusActive)
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

	repository := newRepository(t, pools[0])
	tenanttest.SeedTenant(t, ctx, repository, "tenant-a", tenant.StatusActive)
}

type publishCall struct {
	repository tenant.Repository
	appID      string
	revisionID string
}

type publishResult struct {
	app      tenant.AgentApp
	revision tenant.AgentRevision
	err      error
}

// publishConcurrently releases every call at the same moment and waits for all
// of them. The barrier matters: without it the goroutines start far enough
// apart that the second one usually finds the first already committed, which
// is the easy case rather than the contended one.
func publishConcurrently(
	t *testing.T,
	ctx context.Context,
	scope tenant.TenantContext,
	calls []publishCall,
) []publishResult {
	t.Helper()
	results := make([]publishResult, len(calls))
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(len(calls))
	done.Add(len(calls))
	for i, call := range calls {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			app, revision, err := call.repository.PublishRevision(
				ctx, scope, call.appID, call.revisionID,
			)
			results[i] = publishResult{app: app, revision: revision, err: err}
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	return results
}

// TestIntegrationConcurrentPublishOfOneDraftAdvancesRoutingOnce is case (a):
// the same draft published twice at once is still one publish.
//
// The two callers use two pools and two Repository values, so nothing about
// the result can come from a lock inside one repository.
func TestIntegrationConcurrentPublishOfOneDraftAdvancesRoutingOnce(t *testing.T) {
	dsn := requireDSN(t)
	schema := newSchema(t, dsn)
	first := newRepository(t, openPool(t, dsn, schema))
	second := newRepository(t, openPool(t, dsn, schema))

	ctx := integrationContext(t)
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	tenanttest.SeedTenantAndApp(t, ctx, first, scope, "assistant")
	tenanttest.SeedDraftRevision(t, ctx, first, scope, "assistant", "revision-1", 1)

	results := publishConcurrently(t, ctx, scope, []publishCall{
		{repository: first, appID: "assistant", revisionID: "revision-1"},
		{repository: second, appID: "assistant", revisionID: "revision-1"},
	})
	for i, result := range results {
		require.NoErrorf(t, result.err, "publisher %d failed", i)
		require.Equal(t, tenant.RevisionStatusPublished, result.revision.Status)
	}

	app, err := first.GetAgentApp(ctx, scope, "assistant")
	require.NoError(t, err)
	require.Equal(t, uint64(1), app.RoutingVersion,
		"publishing one draft twice at once must advance routing exactly once")
	require.Equal(t, "revision-1", app.RoutingPolicy.DefaultRevisionID)

	// Both callers observed the same published revision, and it resolves.
	resolved, err := second.ResolveRevision(ctx, scope, "assistant", "")
	require.NoError(t, err)
	require.Equal(t, "revision-1", resolved.ID)

	// One publish wrote published_at; the other took the idempotent path. Both
	// must report the same instant, because only the first one wrote.
	require.NotNil(t, results[0].revision.PublishedAt)
	require.NotNil(t, results[1].revision.PublishedAt)
	require.True(t, results[0].revision.PublishedAt.Equal(*results[1].revision.PublishedAt),
		"the idempotent publish must not rewrite published_at")
}

// TestIntegrationConcurrentPublishOfTwoDraftsAdvancesRoutingTwice is case (b).
func TestIntegrationConcurrentPublishOfTwoDraftsAdvancesRoutingTwice(t *testing.T) {
	dsn := requireDSN(t)
	schema := newSchema(t, dsn)
	first := newRepository(t, openPool(t, dsn, schema))
	second := newRepository(t, openPool(t, dsn, schema))

	ctx := integrationContext(t)
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	tenanttest.SeedTenantAndApp(t, ctx, first, scope, "assistant")
	tenanttest.SeedDraftRevision(t, ctx, first, scope, "assistant", "revision-1", 1)
	tenanttest.SeedDraftRevision(t, ctx, first, scope, "assistant", "revision-2", 2)

	results := publishConcurrently(t, ctx, scope, []publishCall{
		{repository: first, appID: "assistant", revisionID: "revision-1"},
		{repository: second, appID: "assistant", revisionID: "revision-2"},
	})
	for i, result := range results {
		require.NoErrorf(t, result.err, "publisher %d failed", i)
	}

	app, err := first.GetAgentApp(ctx, scope, "assistant")
	require.NoError(t, err)
	require.Equal(t, uint64(2), app.RoutingVersion,
		"two distinct drafts must advance routing twice, with no lost update")
	require.Contains(t, []string{"revision-1", "revision-2"}, app.RoutingPolicy.DefaultRevisionID)

	// Whichever won, both revisions are published and the winner resolves.
	for _, revisionID := range []string{"revision-1", "revision-2"} {
		revision, err := first.GetRevision(ctx, scope, "assistant", revisionID)
		require.NoError(t, err)
		require.Equal(t, tenant.RevisionStatusPublished, revision.Status)
	}
	resolved, err := first.ResolveRevision(ctx, scope, "assistant", "")
	require.NoError(t, err)
	require.Equal(t, app.RoutingPolicy.DefaultRevisionID, resolved.ID)
}

// TestIntegrationRepublishingCurrentDefaultChangesNothing is case (c). The
// assertion is not "the result looks the same" but "no column moved": a
// publish that rewrote the same values would still advance routing_version and
// updated_at, and every cache keyed on routing_version would treat that as a
// real change.
func TestIntegrationRepublishingCurrentDefaultChangesNothing(t *testing.T) {
	dsn := requireDSN(t)
	repository := newIsolatedRepository(t, dsn)

	ctx := integrationContext(t)
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	tenanttest.SeedTenantAndApp(t, ctx, repository, scope, "assistant")
	published := tenanttest.SeedPublishedRevision(
		t, ctx, repository, scope, "assistant", "revision-1", 1,
	)
	require.NotNil(t, published.PublishedAt)

	before, err := repository.GetAgentApp(ctx, scope, "assistant")
	require.NoError(t, err)
	require.Equal(t, uint64(1), before.RoutingVersion)

	for attempt := 0; attempt < 3; attempt++ {
		app, revision, err := repository.PublishRevision(ctx, scope, "assistant", "revision-1")
		require.NoError(t, err)
		require.Equal(t, before.RoutingVersion, app.RoutingVersion)
		require.Truef(t, before.UpdatedAt.Equal(app.UpdatedAt),
			"attempt %d moved app.updated_at from %s to %s",
			attempt, before.UpdatedAt, app.UpdatedAt)
		require.NotNil(t, revision.PublishedAt)
		require.Truef(t, published.PublishedAt.Equal(*revision.PublishedAt),
			"attempt %d moved published_at from %s to %s",
			attempt, published.PublishedAt, revision.PublishedAt)
	}

	// The returned values above are computed in process. These come back from
	// the database, which is what actually has to be unchanged.
	after, err := repository.GetAgentApp(ctx, scope, "assistant")
	require.NoError(t, err)
	require.Equal(t, before.RoutingVersion, after.RoutingVersion)
	require.True(t, before.UpdatedAt.Equal(after.UpdatedAt))
	require.Equal(t, before.RoutingPolicy, after.RoutingPolicy)

	storedRevision, err := repository.GetRevision(ctx, scope, "assistant", "revision-1")
	require.NoError(t, err)
	require.NotNil(t, storedRevision.PublishedAt)
	require.True(t, published.PublishedAt.Equal(*storedRevision.PublishedAt))
}

// TestIntegrationJSONBConfigRoundTrip is case (d): a config survives the
// write, the read and a publish unchanged, and the digest computed from the
// value that comes back equals the digest stored at create time.
//
// This is the assertion that jsonb's normalisation cannot break the digest.
// jsonb reorders keys and drops insignificant whitespace, so a digest taken
// over the stored column text would not survive a round trip; the digest is
// taken over the marshalled Go value instead.
func TestIntegrationJSONBConfigRoundTrip(t *testing.T) {
	dsn := requireDSN(t)
	repository := newIsolatedRepository(t, dsn)

	ctx := integrationContext(t)
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	tenanttest.SeedTenantAndApp(t, ctx, repository, scope, "assistant")

	temperature := 0.7
	config := tenant.RevisionConfig{
		AgentName:   "round-trip-agent",
		Description: `quotes " backslash \ unicode 中文 emoji 🙂`,
		Instruction: "Line one.\nLine two.\tTabbed.",
		Model: tenant.ModelConfig{
			Provider:    "deterministic",
			Name:        "echo-v1",
			SecretRef:   "secret://tenant-a/model-key",
			Temperature: &temperature,
			MaxTokens:   4096,
		},
		ToolRefs:         []string{"orders.read", "orders.write"},
		SkillRefs:        []string{"skill.summarise"},
		KnowledgeRefs:    []string{"kb.faq", "kb.policies"},
		PolicyRefs:       []string{"policy.pii"},
		BackendProfileID: "profile-primary",
	}
	expectedDigest, err := config.Digest()
	require.NoError(t, err)

	input := tenanttest.DraftRevisionInput(scope, "assistant", "revision-1", 1)
	input.Config = config
	created, err := repository.CreateRevision(ctx, scope, input)
	require.NoError(t, err)
	require.Equal(t, expectedDigest, created.ConfigDigest)

	loaded, err := repository.GetRevision(ctx, scope, "assistant", "revision-1")
	require.NoError(t, err)
	require.Equal(t, config, loaded.Config, "the config must survive the jsonb round trip")
	require.Equal(t, expectedDigest, loaded.ConfigDigest)

	loadedDigest, err := loaded.Config.Digest()
	require.NoError(t, err)
	require.Equal(t, expectedDigest, loadedDigest,
		"a digest recomputed from the stored config must equal the stored digest")

	_, publishedRevision, err := repository.PublishRevision(ctx, scope, "assistant", "revision-1")
	require.NoError(t, err)
	require.Equal(t, config, publishedRevision.Config)
	require.Equal(t, expectedDigest, publishedRevision.ConfigDigest)

	resolved, err := repository.ResolveRevision(ctx, scope, "assistant", "")
	require.NoError(t, err)
	require.Equal(t, config, resolved.Config)
	require.Equal(t, expectedDigest, resolved.ConfigDigest)
}

// TestIntegrationPublishRejectsTamperedConfig proves the digest is re-derived
// from the locked row rather than trusted. A config edited outside this
// package — a manual UPDATE, a bad migration, a compromised admin path — no
// longer matches the digest recorded when it was created, and must not be
// publishable.
//
// The classification is part of the assertion. The publish request here is
// well formed: valid tenant, valid app id, valid revision id, and a revision
// that really is a publishable draft. Nothing about it is the caller's fault
// and no edit to it would help, so this must not come back as
// ErrInvalidArgument, which the admin API answers with 400 and the caller's
// own error text.
func TestIntegrationPublishRejectsTamperedConfig(t *testing.T) {
	dsn := requireDSN(t)
	schema := newSchema(t, dsn)
	pool := openPool(t, dsn, schema)
	repository := newRepository(t, pool)

	ctx := integrationContext(t)
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	tenanttest.SeedTenantAndApp(t, ctx, repository, scope, "assistant")
	tenanttest.SeedDraftRevision(t, ctx, repository, scope, "assistant", "revision-1", 1)

	// Behind the repository's back, as an operator with a psql prompt would.
	_, err := pool.Exec(ctx,
		`UPDATE agent_revisions
		 SET config = jsonb_set(config, '{instruction}', '"do something else"')
		 WHERE tenant_id = $1 AND agent_app_id = $2 AND id = $3`,
		scope.TenantID, "assistant", "revision-1",
	)
	require.NoError(t, err)

	_, _, err = repository.PublishRevision(ctx, scope, "assistant", "revision-1")
	require.ErrorIs(t, err, tenant.ErrConfigIntegrity)
	require.NotErrorIs(t, err, tenant.ErrInvalidArgument,
		"a corrupt stored row is a server fault, not a bad request")

	// The whole transaction rolled back: the revision is still a draft and the
	// app never moved.
	revision, err := repository.GetRevision(ctx, scope, "assistant", "revision-1")
	require.NoError(t, err)
	require.Equal(t, tenant.RevisionStatusDraft, revision.Status)
	require.Nil(t, revision.PublishedAt)

	app, err := repository.GetAgentApp(ctx, scope, "assistant")
	require.NoError(t, err)
	require.Zero(t, app.RoutingVersion)
	require.Empty(t, app.RoutingPolicy.DefaultRevisionID)
}

// TestIntegrationPublishSerializesAcrossIndependentPools is case (e), in its
// strongest form: eight distinct drafts published at once, alternating between
// two independent pools and two Repository values.
//
// Routing must end at exactly eight. Anything less is a lost update, which is
// what a read-modify-write of routing_version in this process would produce
// and what "routing_version = routing_version + 1" in SQL prevents. Nothing in
// this arrangement shares a process-level lock, so the ordering can only come
// from the row lock the publish transaction takes.
func TestIntegrationPublishSerializesAcrossIndependentPools(t *testing.T) {
	dsn := requireDSN(t)
	schema := newSchema(t, dsn)
	first := newRepository(t, openPool(t, dsn, schema))
	second := newRepository(t, openPool(t, dsn, schema))

	ctx := integrationContext(t)
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	tenanttest.SeedTenantAndApp(t, ctx, first, scope, "assistant")

	const drafts = 8
	calls := make([]publishCall, 0, drafts)
	revisionIDs := make([]string, 0, drafts)
	for i := 1; i <= drafts; i++ {
		revisionID := fmt.Sprintf("revision-%d", i)
		revisionIDs = append(revisionIDs, revisionID)
		tenanttest.SeedDraftRevision(t, ctx, first, scope, "assistant", revisionID, uint64(i))

		repository := tenant.Repository(first)
		if i%2 == 0 {
			repository = second
		}
		calls = append(calls, publishCall{
			repository: repository, appID: "assistant", revisionID: revisionID,
		})
	}

	for i, result := range publishConcurrently(t, ctx, scope, calls) {
		require.NoErrorf(t, result.err, "publisher %d failed", i)
	}

	app, err := second.GetAgentApp(ctx, scope, "assistant")
	require.NoError(t, err)
	require.Equal(t, uint64(drafts), app.RoutingVersion,
		"every publish must advance routing exactly once, across pools")
	require.Contains(t, revisionIDs, app.RoutingPolicy.DefaultRevisionID)
	require.Len(t, app.RoutingPolicy.Routes, 1)
	require.Equal(t, app.RoutingPolicy.DefaultRevisionID, app.RoutingPolicy.Routes[0].RevisionID)

	for _, revisionID := range revisionIDs {
		revision, err := second.GetRevision(ctx, scope, "assistant", revisionID)
		require.NoError(t, err)
		require.Equalf(t, tenant.RevisionStatusPublished, revision.Status,
			"%s was left unpublished", revisionID)
	}
}

// TestIntegrationRepositoriesShareOnePool pins the ownership contract: a
// Repository borrows its pool and never closes it, so several repositories can
// share one and each sees the others' writes.
func TestIntegrationRepositoriesShareOnePool(t *testing.T) {
	dsn := requireDSN(t)
	pool := openPool(t, dsn, newSchema(t, dsn))
	writer := newRepository(t, pool)
	reader := newRepository(t, pool)

	ctx := integrationContext(t)
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	tenanttest.SeedTenantAndApp(t, ctx, writer, scope, "assistant")
	tenanttest.SeedPublishedRevision(t, ctx, writer, scope, "assistant", "revision-1", 1)

	resolved, err := reader.ResolveRevision(ctx, scope, "assistant", "")
	require.NoError(t, err)
	require.Equal(t, "revision-1", resolved.ID)

	// Neither repository closed the pool it borrowed.
	require.NoError(t, pool.Ping(ctx))
}

// TestIntegrationStoresLargestRevisionNumberExactly pins the upper end of the
// revision_no column against a real database.
//
// Rejecting anything larger is a domain rule and the shared suite proves both
// implementations enforce it. What only a real server can show is that the
// boundary value itself is not the problem: MaxInt64 goes through the uint64
// to bigint conversion, the column, and the bigint to uint64 conversion back
// without wrapping to a negative number or losing its low bits.
func TestIntegrationStoresLargestRevisionNumberExactly(t *testing.T) {
	dsn := requireDSN(t)
	repository := newIsolatedRepository(t, dsn)

	ctx := integrationContext(t)
	scope := tenant.TenantContext{TenantID: "tenant-a"}
	tenanttest.SeedTenantAndApp(t, ctx, repository, scope, "assistant")

	input := tenanttest.DraftRevisionInput(scope, "assistant", "revision-max", math.MaxInt64)
	created, err := repository.CreateRevision(ctx, scope, input)
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxInt64), created.RevisionNo)

	loaded, err := repository.GetRevision(ctx, scope, "assistant", "revision-max")
	require.NoError(t, err)
	require.Equal(t, uint64(math.MaxInt64), loaded.RevisionNo)
}

// TestIntegrationTimestampsRoundTripExactly pins the truncation. PostgreSQL
// keeps microseconds, so a value written with nanosecond precision would come
// back different from the value the caller was just handed, and every
// "nothing changed" assertion would turn into a near-miss.
func TestIntegrationTimestampsRoundTripExactly(t *testing.T) {
	dsn := requireDSN(t)
	repository := newIsolatedRepository(t, dsn)

	ctx := integrationContext(t)
	created := tenanttest.SeedTenant(t, ctx, repository, "tenant-a", tenant.StatusActive)
	loaded, err := repository.GetTenant(ctx, "tenant-a")
	require.NoError(t, err)

	require.True(t, created.CreatedAt.Equal(loaded.CreatedAt),
		"created_at came back as %s, not %s", loaded.CreatedAt, created.CreatedAt)
	require.True(t, created.UpdatedAt.Equal(loaded.UpdatedAt),
		"updated_at came back as %s, not %s", loaded.UpdatedAt, created.UpdatedAt)
	// pgx decodes timestamptz into time.Local, so a read that skipped the
	// normalisation would still compare equal above but print a local time.
	require.Equal(t, time.UTC, loaded.CreatedAt.Location())
	require.Zero(t, created.CreatedAt.Nanosecond()%1000,
		"a stored timestamp must already be truncated to the microsecond timestamptz keeps")
}
