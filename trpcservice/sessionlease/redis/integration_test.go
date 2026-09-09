package redis_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease"
	redislease "github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease/sessionleasetest"
)

// These are the only tests in this package that touch a real server, and they
// stay off unless the operator asks for them. `go test ./...` on a machine with
// no redis and no network must stay green, so the gate is checked before any
// client is built.
//
// Bring redis up with deploy/docker-compose.session.yml, then:
//
//	docker compose -f deploy/docker-compose.session.yml up -d --wait
//	TRPC_SERVICE_SESSION_INTEGRATION=1 \
//	TRPC_SERVICE_REDIS_URL='redis://:trpc-local-dev@127.0.0.1:56379/0' \
//	go test -race -count=1 -timeout 300s ./trpcservice/sessionlease/...
//
// Those credentials are the compose file's development defaults; see
// docs/session-lease.md.
const (
	envIntegration = "TRPC_SERVICE_SESSION_INTEGRATION"
	envRedisURL    = "TRPC_SERVICE_REDIS_URL"

	// integrationTimeout bounds setup and teardown. A reachable redis answers
	// in milliseconds; this only stops an unreachable one from hanging until
	// the package timeout.
	integrationTimeout = 30 * time.Second
)

func requireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv(envIntegration) != "1" {
		t.Skipf("set %s=1 to run session lease integration tests", envIntegration)
	}
}

func requireRedisURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv(envRedisURL)
	if url == "" {
		t.Skipf("set %s to run session lease integration tests", envRedisURL)
	}
	return url
}

// setupContext is for setup and teardown. t.Context is cancelled before
// cleanups run, so a cleanup that has to delete keys needs a context of its
// own.
func setupContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), integrationTimeout)
}

// newIntegrationClient dials redis and hands back a client this test owns.
func newIntegrationClient(t *testing.T, url string) goredis.UniversalClient {
	t.Helper()
	opts, err := goredis.ParseURL(url)
	require.NoError(t, err, "parse %s", envRedisURL)
	client := goredis.NewClient(opts)
	t.Cleanup(func() {
		// Break closes this client in some subtests, so a second close is
		// expected to fail and is not a test failure.
		_ = client.Close()
	})

	ctx, cancel := setupContext(t)
	defer cancel()
	require.NoError(t, client.Ping(ctx).Err(), "redis is not reachable")
	return client
}

// uniqueKeyPrefix isolates one subtest's keys inside a shared redis, and makes
// leftovers attributable to the run that created them.
func uniqueKeyPrefix() string {
	return "sessionlease-test:" + strings.ReplaceAll(uuid.NewString(), "-", "")
}

// deleteKeys removes everything a subtest created. Every integration test in
// this package registers it, so a full run leaves the keyspace as it found it.
func deleteKeys(t *testing.T, client goredis.UniversalClient, prefix string) {
	t.Helper()
	ctx, cancel := setupContext(t)
	defer cancel()

	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, prefix+":*", 100).Result()
		require.NoError(t, err)
		if len(keys) > 0 {
			require.NoError(t, client.Del(ctx, keys...).Err())
		}
		if next == 0 {
			return
		}
		cursor = next
	}
}

func scanKeys(t *testing.T, client goredis.UniversalClient, prefix string) []string {
	t.Helper()
	ctx, cancel := setupContext(t)
	defer cancel()

	var (
		cursor uint64
		found  []string
	)
	for {
		keys, next, err := client.Scan(ctx, cursor, prefix+":*", 100).Result()
		require.NoError(t, err)
		found = append(found, keys...)
		if next == 0 {
			return found
		}
		cursor = next
	}
}

func TestRedisCoordinatorSatisfiesTheContract(t *testing.T) {
	requireIntegration(t)
	url := requireRedisURL(t)

	sessionleasetest.RunCoordinatorSuite(t, func(t *testing.T) sessionleasetest.Backend {
		prefix := uniqueKeyPrefix()

		// The janitor is a second connection so that teardown still works after
		// Break has closed the one the coordinators use. Registering it first
		// makes it the last thing closed.
		janitor := newIntegrationClient(t, url)
		t.Cleanup(func() { deleteKeys(t, janitor, prefix) })

		shared := newIntegrationClient(t, url)

		return sessionleasetest.Backend{
			New: func(t *testing.T, cfg sessionlease.Config) sessionlease.Coordinator {
				coordinator, err := redislease.New(shared, redislease.Options{
					KeyPrefix: prefix,
					Lease:     cfg,
				})
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, coordinator.Close()) })
				return coordinator
			},
			Break: func(t *testing.T) {
				require.NoError(t, shared.Close())
			},
		}
	})
}

func TestRedisKeysCarryNoIdentifiers(t *testing.T) {
	requireIntegration(t)
	url := requireRedisURL(t)

	prefix := uniqueKeyPrefix()
	client := newIntegrationClient(t, url)
	t.Cleanup(func() { deleteKeys(t, client, prefix) })

	coordinator, err := redislease.New(client, redislease.Options{
		KeyPrefix: prefix,
		Lease:     sessionleasetest.Timings(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	key := sessionleasetest.Key()
	lease, err := coordinator.Acquire(t.Context(), key)
	require.NoError(t, err)

	keys := scanKeys(t, client, prefix)
	require.Len(t, keys, 2, "one lease is one lock and one fence")

	digest := sessionlease.KeyDigest(key)
	for _, name := range keys {
		require.Contains(t, name, "{"+digest+"}",
			"both keys carry the scope digest as their cluster hash tag")
		for _, identifier := range []string{
			key.TenantID, key.AppID, key.PrincipalID, key.SessionID,
		} {
			require.NotContains(t, name, identifier,
				"an operator reading the keyspace must not learn who is running")
		}
	}

	require.NoError(t, lease.Release(t.Context()))
}

func TestRedisReleaseKeepsTheFence(t *testing.T) {
	requireIntegration(t)
	url := requireRedisURL(t)

	prefix := uniqueKeyPrefix()
	client := newIntegrationClient(t, url)
	t.Cleanup(func() { deleteKeys(t, client, prefix) })

	coordinator, err := redislease.New(client, redislease.Options{
		KeyPrefix: prefix,
		Lease:     sessionleasetest.Timings(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	key := sessionleasetest.Key()
	lease, err := coordinator.Acquire(t.Context(), key)
	require.NoError(t, err)
	require.NoError(t, lease.Release(t.Context()))

	keys := scanKeys(t, client, prefix)
	require.Len(t, keys, 1,
		"the lock is gone and the fence stays: a fence that could be deleted "+
			"and restart at 1 would not be monotonic. Fence keys are therefore "+
			"never collected, which is a documented limitation")
	require.Contains(t, keys[0], ":fence")

	again, err := coordinator.Acquire(t.Context(), key)
	require.NoError(t, err)
	require.Greater(t, again.Fence(), lease.Fence())
	require.NoError(t, again.Release(t.Context()))
}

func TestRedisAbandonedLockExpiresByTTL(t *testing.T) {
	requireIntegration(t)
	url := requireRedisURL(t)

	prefix := uniqueKeyPrefix()
	client := newIntegrationClient(t, url)
	t.Cleanup(func() { deleteKeys(t, client, prefix) })

	cfg := sessionleasetest.Timings()
	coordinator, err := redislease.New(client, redislease.Options{KeyPrefix: prefix, Lease: cfg})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	key := sessionleasetest.Key()
	runCtx, abandon := context.WithCancel(t.Context())
	lease, err := coordinator.Acquire(runCtx, key)
	require.NoError(t, err)
	abandon()

	select {
	case <-lease.Done():
	case <-time.After(2 * cfg.TTL):
		t.Fatal("an abandoned holder must stop claiming the lease")
	}

	lockKey := prefix + ":{" + sessionlease.KeyDigest(key) + "}:lock"
	ttl, err := client.PTTL(t.Context(), lockKey).Result()
	require.NoError(t, err)
	require.Greater(t, ttl, time.Duration(0),
		"the lock is left with an expiry rather than deleted, so the TTL covers "+
			"the tail writes a cancelled Runner still performs")
	require.LessOrEqual(t, ttl, cfg.TTL)
}
