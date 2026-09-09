package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	platformconfig "github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// These are the only tests in this package that touch a real server, and they
// stay off unless the operator asks for them. `go test ./...` on a machine with
// no postgres and no network must stay green, so the gate is checked before any
// configuration is built.
//
// They reuse the session spike's compose file and environment variables rather
// than adding a third set:
//
//	docker compose -f deploy/docker-compose.session.yml up -d --wait
//	TRPC_SERVICE_SESSION_INTEGRATION=1 \
//	TRPC_SERVICE_POSTGRES_DSN='postgres://trpc:trpc-local-dev@127.0.0.1:55432/trpc_session?sslmode=disable' \
//	go test -race -count=1 -timeout 300s ./cmd/trpc-service/...
//
// Those credentials are the compose file's development defaults; see
// docs/session-backend.md.
//
// What is being tested here is the bootstrap itself, not the packages under it.
// Each of those has its own integration suite; this one asserts the thing only
// the entrypoint can be wrong about — that one profile puts the control plane,
// the pin and the conversation in one place, and that a restart finds all three
// where it left them.
const (
	// envIntegration must be "1" for anything in this file to run.
	envIntegration = "TRPC_SERVICE_SESSION_INTEGRATION"

	// integrationTimeout bounds every test body and every setup or teardown
	// statement. A reachable server answers in milliseconds; this only stops an
	// unreachable one from hanging until the package timeout.
	integrationTimeout = 60 * time.Second

	// schemaPrefix namespaces the throwaway schemas these tests create. Every
	// test gets a schema of its own and drops it, so nothing accumulates
	// between runs and two runs never see each other's rows. It matters more
	// here than elsewhere: upstream creates its six session tables but never
	// drops them.
	schemaPrefix = "trpc_service_boot_"

	// profileSchemaPrefix is the same thing, shorter, for the tests that put a
	// tenant's own tables in the schema beside the platform's.
	//
	// The length is the reason it exists. Upstream builds its index names as
	// "idx_<schema>_<prefix>_<table>_<suffix>" and PostgreSQL truncates an
	// identifier at 63 bytes, so a schema and a table prefix have 26 characters
	// between them — see sessionbackend.validateGeneratedIndexNames. A schema
	// named with schemaPrefix is 26 on its own and leaves a table prefix
	// nothing, which is a property of the fixture rather than of the code under
	// test.
	profileSchemaPrefix = "trpc_bp_"
)

func requireIntegration(t *testing.T) string {
	t.Helper()
	if os.Getenv(envIntegration) != "1" {
		t.Skipf("set %s=1 to run bootstrap integration tests", envIntegration)
	}
	dsn := os.Getenv(postgresDSNEnvVar)
	if dsn == "" {
		t.Skipf("set %s to run this test", postgresDSNEnvVar)
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
// deadline: schema creation and teardown. A cleanup runs after the body's
// context is already cancelled, so a teardown that inherited it would leave its
// schema behind on every run.
func setupContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), integrationTimeout)
}

// quoteIdentifier quotes a schema name for DDL. The names here are generated
// from a fixed prefix and hex, so this is belt and braces, but a schema name is
// the one thing in this file that cannot be a bind parameter.
func quoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// createSchema makes an empty schema and schedules its removal.
func createSchema(t *testing.T, dsn string) string {
	t.Helper()
	return createSchemaNamed(t, dsn, schemaPrefix)
}

// createSchemaNamed is createSchema with the namespace chosen by the caller,
// for tests whose schema name has to leave room for something else.
//
// Cleanup order matters and is LIFO. The admin pool is registered first so it
// closes last, and the drop is registered second so it runs after every stack
// the test opened has been closed — DROP SCHEMA never waits on a live
// connection. CASCADE is what removes the upstream session tables, which
// nothing else drops.
func createSchemaNamed(t *testing.T, dsn, prefix string) string {
	t.Helper()
	schema := prefix + uuid.New().String()[:8]

	config, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	ctx, cancel := setupContext()
	defer cancel()
	admin, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(admin.Close)
	require.NoError(t, admin.Ping(ctx))

	// The schema has to exist before a pool can point search_path at it, and
	// nothing in the bootstrap creates one: unqualified DDL against a
	// search_path naming only a missing schema fails with "no schema has been
	// selected to create in".
	_, err = admin.Exec(ctx, `CREATE SCHEMA `+quoteIdentifier(schema))
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

// bootstrap opens a stack the way the process does — through openStorage with
// the real steps — and closes it when the test ends. Going through the profile
// builder rather than assembling the components by hand is the point: this test
// is here to catch a bootstrap that wires or orders something wrongly, which a
// hand-assembled stack would hide.
func bootstrap(t *testing.T, dsn, schema string) *storageStack {
	t.Helper()
	cfg := bootstrapConfig(dsn, schema)
	require.NoError(t, cfg.validate())

	ctx, cancel := setupContext()
	defer cancel()
	stack, err := openStorage(ctx, cfg, defaultStorageDeps())
	require.NoError(t, err)
	require.NotNil(t, stack)
	t.Cleanup(func() { require.NoError(t, stack.close()) })
	return stack
}

// bootstrapConfig is the configuration every stack in these gated tests boots
// from. It is a function rather than a literal inside bootstrap because a
// runtime stack opened over one of these stacks has to derive its process
// constraints from the same value: a second literal would let the two drift,
// and the drift would silently change which backends a tenant profile may name.
//
// In-memory coordination: these tests restart one process against one schema,
// so there is no second Worker to coordinate with, and this file has to keep
// running with nothing but PostgreSQL available. The redis backend has its own
// gated suite.
func bootstrapConfig(dsn, schema string) storageConfig {
	return storageConfig{
		profile:      profilePostgres,
		dsn:          dsn,
		schema:       schema,
		coordination: coordinationInMemory,
	}
}

// TestIntegrationBootstrapCreatesEverySchemaItNeeds proves the profile is
// complete: one DSN and one schema, and all three storage families land in it.
// A missing table here is a process that starts and then fails on the first
// request.
func TestIntegrationBootstrapCreatesEverySchemaItNeeds(t *testing.T) {
	dsn := requireIntegration(t)
	schema := createSchema(t, dsn)
	ctx := integrationContext(t)

	stack := bootstrap(t, dsn, schema)
	// The session tables are created by the upstream constructor, which is
	// inside openStorage; seeding is what the process does next.
	require.NoError(t, platformconfig.SeedDemo(ctx, stack.repository))

	inspector := openInspector(t, dsn)
	for _, table := range []string{
		// tenant/postgres.
		"tenants",
		"agent_apps",
		"agent_revisions",
		// sessiondir/postgres.
		"sessions",
		// Upstream tRPC-Agent-Go, created during construction of its service.
		"session_states",
		"session_events",
		"session_track_events",
		"session_summaries",
		"app_states",
		"user_states",
	} {
		require.Truef(t, tableExists(t, ctx, inspector, schema, table),
			"table %q is missing from schema %q", table, schema)
	}
}

// TestIntegrationStateSurvivesARestart is the reason this profile exists.
//
// It writes what a served conversation writes — a tenant and a published
// revision, a pin, and a real turn of history — then closes the whole stack and
// opens a new one on the same schema, exactly as a restarted process would. All
// three have to still be there, and they have to agree with each other: a pin
// naming a revision that did not survive is no better than no pin at all.
func TestIntegrationStateSurvivesARestart(t *testing.T) {
	dsn := requireIntegration(t)
	schema := createSchema(t, dsn)
	ctx := integrationContext(t)
	scope := tenant.TenantContext{TenantID: platformconfig.DemoTenantID}

	key := sessiondir.Key{
		TenantID:    platformconfig.DemoTenantID,
		AppID:       platformconfig.DemoAgentAppID,
		PrincipalID: platformconfig.DemoPrincipalID,
		SessionID:   "restart-" + uuid.New().String()[:8],
		Epoch:       1,
	}
	sessionKey := session.Key{
		AppName:   platformconfig.DemoAgentAppID,
		UserID:    platformconfig.DemoPrincipalID,
		SessionID: key.SessionID,
	}

	// First boot: seed, pin the session to the published revision, and record
	// one complete turn.
	before := bootstrap(t, dsn, schema)
	require.NoError(t, platformconfig.SeedDemo(ctx, before.repository))

	published, err := before.repository.ResolveRevision(ctx, scope, platformconfig.DemoAgentAppID, "")
	require.NoError(t, err)
	require.Equal(t, platformconfig.DemoRevisionID, published.ID)

	pinned, err := before.directory.EnsurePin(ctx, key, published.ID)
	require.NoError(t, err)
	require.Equal(t, published.ID, pinned)

	created, err := before.sessions.CreateSession(ctx, sessionKey, session.StateMap{})
	require.NoError(t, err)
	require.NotNil(t, created)
	// Both events of a turn: upstream drops an assistant message with no user
	// message before it, so a one-event session reads back empty.
	prompt, reply := newTurn("inv-"+key.SessionID, "hello before restart", "reply before restart")
	require.NoError(t, before.sessions.AppendEvent(ctx, created, prompt))
	require.NoError(t, before.sessions.AppendEvent(ctx, created, reply))

	// The restart. Closing releases the pools; nothing of this stack is used
	// again.
	require.NoError(t, before.close())

	after := bootstrap(t, dsn, schema)
	// A restarted process seeds again, which must not disturb what is there.
	require.NoError(t, platformconfig.SeedDemo(ctx, after.repository))

	t.Run("the control plane survived", func(t *testing.T) {
		resolved, err := after.repository.ResolveRevision(ctx, scope, platformconfig.DemoAgentAppID, "")
		require.NoError(t, err)
		require.Equal(t, published.ID, resolved.ID)
		require.Equal(t, tenant.RevisionStatusPublished, resolved.Status)

		revision, err := after.repository.GetRevision(ctx, scope, platformconfig.DemoAgentAppID, published.ID)
		require.NoError(t, err)
		require.Equal(t, published.Config.Model.Provider, revision.Config.Model.Provider)
	})

	t.Run("the pin survived", func(t *testing.T) {
		found, ok, err := after.directory.GetPin(ctx, key)
		require.NoError(t, err)
		require.True(t, ok, "a pin written before the restart must still be there")
		require.Equal(t, published.ID, found)

		// And it still names a revision the control plane can resolve: a pin
		// pointing at nothing would send the conversation to whatever is
		// published now, which is what the pin exists to prevent.
		resolved, err := after.repository.ResolveRevision(
			ctx, scope, platformconfig.DemoAgentAppID, found)
		require.NoError(t, err)
		require.Equal(t, found, resolved.ID)
	})

	t.Run("the conversation survived", func(t *testing.T) {
		loaded, err := after.sessions.GetSession(ctx, sessionKey)
		require.NoError(t, err)
		require.NotNil(t, loaded, "the session must outlive the process that created it")
		require.Len(t, loaded.Events, 2)
		require.NotNil(t, loaded.Events[0].Response)
		require.Len(t, loaded.Events[0].Response.Choices, 1)
		require.Equal(t, "hello before restart", loaded.Events[0].Response.Choices[0].Message.Content)
		require.Equal(t, "reply before restart", loaded.Events[1].Response.Choices[0].Message.Content)
	})

	t.Cleanup(func() {
		cleanupCtx, cancel := setupContext()
		defer cancel()
		// Best effort. The schema is dropped anyway; this keeps the upstream
		// soft-deleted rows from being the only trace if the drop ever fails.
		_ = after.sessions.DeleteSession(cleanupCtx, sessionKey)
	})
}

// TestIntegrationRestartDoesNotClobberAPublishedRevision is the persistent
// counterpart of the config unit test, and the failure it guards against only
// exists once the control plane survives a restart: seeding on boot used to
// publish echo-v1 unconditionally, which against this profile would move a
// deployed default back on nothing but a process restart.
func TestIntegrationRestartDoesNotClobberAPublishedRevision(t *testing.T) {
	dsn := requireIntegration(t)
	schema := createSchema(t, dsn)
	ctx := integrationContext(t)
	scope := tenant.TenantContext{TenantID: platformconfig.DemoTenantID}

	before := bootstrap(t, dsn, schema)
	require.NoError(t, platformconfig.SeedDemo(ctx, before.repository))

	// An operator deploys a second revision.
	const secondRevisionID = "echo-v2"
	_, err := before.repository.CreateRevision(ctx, scope, tenant.AgentRevision{
		ID:         secondRevisionID,
		TenantID:   platformconfig.DemoTenantID,
		AgentAppID: platformconfig.DemoAgentAppID,
		RevisionNo: 2,
		CreatedBy:  "integration-operator",
		Config: tenant.RevisionConfig{
			AgentName:   "echo-assistant",
			Description: "Operator revision",
			Instruction: "Return the model response.",
			Model: tenant.ModelConfig{
				Provider: "deterministic",
				Name:     "deterministic-echo",
			},
		},
	})
	require.NoError(t, err)
	_, _, err = before.repository.PublishRevision(
		ctx, scope, platformconfig.DemoAgentAppID, secondRevisionID)
	require.NoError(t, err)
	require.NoError(t, before.close())

	// The restart, seeding against a store that already holds the deployment.
	after := bootstrap(t, dsn, schema)
	require.NoError(t, platformconfig.SeedDemo(ctx, after.repository))

	resolved, err := after.repository.ResolveRevision(ctx, scope, platformconfig.DemoAgentAppID, "")
	require.NoError(t, err)
	require.Equal(t, secondRevisionID, resolved.ID, "the restart moved the default back")

	app, err := after.repository.GetAgentApp(ctx, scope, platformconfig.DemoAgentAppID)
	require.NoError(t, err)
	require.Equal(t, secondRevisionID, app.RoutingPolicy.DefaultRevisionID)
}

// openInspector opens a pool for reading the catalog. It is deliberately not
// the bootstrap's pool: asserting on the schema through the same search_path
// the code under test uses would pass even if that search_path pointed
// somewhere else entirely.
func openInspector(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	config, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)

	ctx, cancel := setupContext()
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, pool.Ping(ctx))
	return pool
}

func tableExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, schema, table string) bool {
	t.Helper()
	var exists bool
	err := pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_schema = $1 AND table_name = $2
		)`, schema, table).Scan(&exists)
	require.NoError(t, err)
	return exists
}

// newTurn builds the two events of one complete turn. Upstream filters an
// assistant message that no user message precedes, so a session written with
// only one of these reads back empty and would make the restart assertions
// meaningless.
func newTurn(invocationID, prompt, reply string) (*event.Event, *event.Event) {
	return newIntegrationEvent(invocationID+"-user", model.NewUserMessage(prompt)),
		newIntegrationEvent(invocationID+"-assistant", model.NewAssistantMessage(reply))
}

func newIntegrationEvent(invocationID string, message model.Message) *event.Event {
	return event.NewResponseEvent(invocationID, "trpc-service-bootstrap-test", &model.Response{
		Done:    true,
		Choices: []model.Choice{{Index: 0, Message: message}},
	})
}
