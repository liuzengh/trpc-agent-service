package sessionbackend

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// These tests are the only ones in this package that touch a real server, and
// they stay off unless the operator asks for them. `go test ./...` on a
// machine with no postgres, no redis and no network must stay green, so the
// gate is checked before any config is built.
//
// Bring the servers up with deploy/docker-compose.session.yml, then:
//
//	docker compose -f deploy/docker-compose.session.yml up -d --wait
//	TRPC_SERVICE_SESSION_INTEGRATION=1 \
//	TRPC_SERVICE_POSTGRES_DSN='postgres://trpc:trpc-local-dev@127.0.0.1:55432/trpc_session?sslmode=disable' \
//	TRPC_SERVICE_REDIS_URL='redis://:trpc-local-dev@127.0.0.1:56379/0' \
//	go test -race -timeout 120s ./trpcservice/sessionbackend/...
//
// Those credentials are the compose file's development defaults; see
// docs/session-backend.md.
const (
	// envIntegration must be "1" for anything in this file to run.
	envIntegration = "TRPC_SERVICE_SESSION_INTEGRATION"
	// envPostgresDSN and envRedisURL each gate only their own backend, so a
	// machine running just one of the two still exercises that one.
	envPostgresDSN = "TRPC_SERVICE_POSTGRES_DSN"
	envRedisURL    = "TRPC_SERVICE_REDIS_URL"

	// integrationTimeout bounds every individual integration test. A backend
	// that is reachable answers in milliseconds; this only stops an
	// unreachable one from hanging until the package timeout.
	integrationTimeout = 30 * time.Second

	// postgresTablePrefix is deliberately stable across runs. A per-run prefix
	// would leave a fresh set of six tables behind on every execution, and
	// upstream creates tables but never drops them. Row-level isolation comes
	// from the unique app name instead.
	postgresTablePrefix = "spike"
)

// requireIntegration skips unless the operator opted in.
func requireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv(envIntegration) != "1" {
		t.Skipf("set %s=1 to run session backend integration tests", envIntegration)
	}
}

// requireEnv skips when the connection string for this backend is absent, so
// running with only postgres configured does not fail the redis test.
func requireEnv(t *testing.T, name string) string {
	t.Helper()
	requireIntegration(t)
	value := os.Getenv(name)
	if value == "" {
		t.Skipf("set %s to run this test", name)
	}
	return value
}

// runID namespaces every key this test writes, so concurrent runs and repeated
// runs against the same server never collide.
func runID(t *testing.T) string {
	t.Helper()
	return uuid.New().String()[:8]
}

func integrationContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), integrationTimeout)
	t.Cleanup(cancel)
	return ctx
}

// cleanupContext returns the context a t.Cleanup uses to undo a write. It is
// deliberately not integrationContext: the test body's context is normally
// already cancelled by the time cleanups run, and a cleanup that inherits a
// cancelled context leaves the row behind on every failing run.
func cleanupContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), integrationTimeout)
}

// newIntegrationService builds a service and schedules its Close.
//
// Close is registered here, before the caller registers anything, so it runs
// last: t.Cleanup is LIFO, and a cleanup that deletes a session needs the
// service to still be open. Registering it also means a require failure
// mid-test closes the pool instead of leaking it for the rest of the package.
func newIntegrationService(t *testing.T, cfg Config) session.Service {
	t.Helper()
	service, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, service)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	return service
}

// postgresConfig builds a config isolated by table prefix, as required for a
// shared database.
func postgresConfig(t *testing.T) Config {
	t.Helper()
	return Config{
		Backend: BackendPostgres,
		Postgres: PostgresConfig{
			DSN:         requireEnv(t, envPostgresDSN),
			TablePrefix: postgresTablePrefix,
		},
	}
}

// redisConfig builds a config isolated by a per-run key prefix. Unlike
// postgres tables, redis keys are cheap to namespace per run and the test
// deletes what it wrote.
func redisConfig(t *testing.T, id string) Config {
	t.Helper()
	return Config{
		Backend: BackendRedis,
		Redis: RedisConfig{
			URL:       requireEnv(t, envRedisURL),
			KeyPrefix: "spike:" + id,
		},
	}
}

func TestIntegrationPostgresRoundTrip(t *testing.T) {
	cfg := postgresConfig(t)
	assertRoundTrip(t, newIntegrationService(t, cfg), runID(t))
}

func TestIntegrationRedisRoundTrip(t *testing.T) {
	id := runID(t)
	assertRoundTrip(t, newIntegrationService(t, redisConfig(t, id)), id)
}

// assertRoundTrip exercises the behaviour the platform actually depends on:
// a session is created, an event survives a write and a read, a missing
// session is reported as a miss rather than an error, and a delete removes the
// session from the read path.
func assertRoundTrip(t *testing.T, service session.Service, id string) {
	t.Helper()
	ctx := integrationContext(t)
	key := session.Key{
		AppName:   "spike-app-" + id,
		UserID:    "spike-user-" + id,
		SessionID: "spike-session-" + id,
	}

	// A session that was never created is a miss, not an error. Both backends
	// return (nil, nil) here, so a caller checking only err would dereference
	// nil.
	missing, err := service.GetSession(ctx, key)
	require.NoError(t, err)
	require.Nil(t, missing)

	created, err := service.CreateSession(ctx, key, session.StateMap{})
	require.NoError(t, err)
	require.NotNil(t, created)
	require.Equal(t, key.SessionID, created.ID)
	t.Cleanup(func() {
		// Best effort: the body deletes this session itself, so this only has
		// work to do when an assertion above it failed.
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_ = service.DeleteSession(cleanupCtx, key)
	})

	prompt, reply := newTurn("inv-"+id, "hello from "+id, "reply to "+id)
	require.NoError(t, service.AppendEvent(ctx, created, prompt))
	require.NoError(t, service.AppendEvent(ctx, created, reply))

	loaded, err := service.GetSession(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	require.Equal(t, key.AppName, loaded.AppName)
	require.Equal(t, key.UserID, loaded.UserID)
	require.Len(t, loaded.Events, 2, "both events of a valid turn must survive the backend write")
	require.NotNil(t, loaded.Events[0].Response)
	require.Len(t, loaded.Events[0].Response.Choices, 1)
	require.Equal(t, "hello from "+id, loaded.Events[0].Response.Choices[0].Message.Content)
	require.Equal(t, "reply to "+id, loaded.Events[1].Response.Choices[0].Message.Content)

	require.NoError(t, service.DeleteSession(ctx, key))

	// Postgres deletes softly by default and redis deletes the keys, but both
	// have to disappear from the read path.
	deleted, err := service.GetSession(ctx, key)
	require.NoError(t, err)
	require.Nil(t, deleted)
}

// TestIntegrationCloseIsIdempotent pins the property the resolver relies on: a
// service can be closed on the shutdown path and again on a deferred cleanup
// without panicking or reporting a second failure.
//
// It is its own test rather than a step in the round trip, because closing
// inside a test body runs before that body's cleanups, which would leave the
// round trip deleting its session through a closed pool.
func TestIntegrationCloseIsIdempotent(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		assertCloseIsIdempotent(t, postgresConfig(t))
	})
	t.Run("redis", func(t *testing.T) {
		assertCloseIsIdempotent(t, redisConfig(t, runID(t)))
	})
}

func assertCloseIsIdempotent(t *testing.T, cfg Config) {
	t.Helper()
	service, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, service)
	defer func() { _ = service.Close() }()

	require.NoError(t, service.Close())
	require.NoError(t, service.Close())
}

// TestIntegrationCreateSessionDivergence pins a semantic difference the
// interface hides: postgres treats a second CreateSession on a live session as
// an error, while redis returns the existing session. Code that relies on
// either shape breaks when the backend is swapped.
func TestIntegrationCreateSessionDivergence(t *testing.T) {
	t.Run("postgres rejects an existing session", func(t *testing.T) {
		service := newIntegrationService(t, postgresConfig(t))

		ctx := integrationContext(t)
		id := runID(t)
		key := session.Key{
			AppName:   "spike-dup-" + id,
			UserID:    "spike-user-" + id,
			SessionID: "spike-session-" + id,
		}
		_, err := service.CreateSession(ctx, key, session.StateMap{})
		require.NoError(t, err)
		t.Cleanup(func() {
			cleanupCtx, cancel := cleanupContext(t)
			defer cancel()
			_ = service.DeleteSession(cleanupCtx, key)
		})

		second, err := service.CreateSession(ctx, key, session.StateMap{})
		require.Error(t, err)
		require.Nil(t, second)
		require.Contains(t, err.Error(), "already exists")
	})

	t.Run("redis returns the existing session", func(t *testing.T) {
		id := runID(t)
		service := newIntegrationService(t, redisConfig(t, id))

		ctx := integrationContext(t)
		key := session.Key{
			AppName:   "spike-dup-" + id,
			UserID:    "spike-user-" + id,
			SessionID: "spike-session-" + id,
		}
		first, err := service.CreateSession(ctx, key, session.StateMap{})
		require.NoError(t, err)
		require.NotNil(t, first)
		t.Cleanup(func() {
			cleanupCtx, cancel := cleanupContext(t)
			defer cancel()
			_ = service.DeleteSession(cleanupCtx, key)
		})

		prompt, reply := newTurn("inv-"+id, "first", "first reply")
		require.NoError(t, service.AppendEvent(ctx, first, prompt))
		require.NoError(t, service.AppendEvent(ctx, first, reply))

		second, err := service.CreateSession(ctx, key, session.StateMap{})
		require.NoError(t, err)
		require.NotNil(t, second)
		require.Equal(t, key.SessionID, second.ID)
		require.Len(t, second.Events, 2, "redis returns the existing session, history included")
	})
}

// TestIntegrationKeyPrefixIsolatesRuns proves the redis knob the tests rely on
// actually separates two services pointed at the same server.
func TestIntegrationKeyPrefixIsolatesRuns(t *testing.T) {
	idA, idB := runID(t), runID(t)
	serviceA := newIntegrationService(t, redisConfig(t, idA))
	serviceB := newIntegrationService(t, redisConfig(t, idB))

	ctx := integrationContext(t)
	// The same key under two prefixes must address two different sessions.
	key := session.Key{AppName: "spike-iso", UserID: "spike-user", SessionID: "spike-session"}

	created, err := serviceA.CreateSession(ctx, key, session.StateMap{})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()
		_ = serviceA.DeleteSession(cleanupCtx, key)
	})
	prompt, reply := newTurn("inv-"+idA, "only in A", "reply in A")
	require.NoError(t, serviceA.AppendEvent(ctx, created, prompt))
	require.NoError(t, serviceA.AppendEvent(ctx, created, reply))

	// Read back through A first. Without this the test would still pass if the
	// write never landed at all, which is the same observation as isolation.
	fromA, err := serviceA.GetSession(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, fromA, "the writing service must see its own session")
	require.Len(t, fromA.Events, 2)

	fromB, err := serviceB.GetSession(ctx, key)
	require.NoError(t, err)
	require.Nil(t, fromB, "a different key prefix must not see the other run's session")
}
