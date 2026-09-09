package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"trpc.group/trpc-go/trpc-agent-go/session"

	platformagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	platformconfig "github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease"
	redislease "github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionrun"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
)

// This file is the evidence behind the run lease. Everything else about it is
// argued from unit tests against a single stack; here two independently built
// Workers — separate storage stacks, separate resolvers, separate coordinators,
// separate HTTP servers — share one PostgreSQL schema and one Redis, and the
// claims are checked over the wire.
//
// "Two Workers" is worth being exact about, because it is less than it sounds.
// They are two stacks inside this one test binary, not two processes, and they
// are handed the same Redis client rather than one each. So nothing here
// exercises process isolation, separate connection pools, or any failure that
// only appears across an OS boundary. What it does exercise is the part the
// lease is about: two independent coordinators, each with its own owner tokens
// and its own renewal loops, contending for one key in one Redis over one
// Session in one schema.
//
// It stays off unless the operator asks for it, and needs both backends:
//
//	docker compose -f deploy/docker-compose.session.yml up -d --wait
//	TRPC_SERVICE_SESSION_INTEGRATION=1 \
//	TRPC_SERVICE_POSTGRES_DSN='postgres://trpc:trpc-local-dev@127.0.0.1:55432/trpc_session?sslmode=disable' \
//	TRPC_SERVICE_REDIS_URL='redis://:trpc-local-dev@127.0.0.1:56379/0' \
//	go test -race -count=1 -timeout 300s -run Integration ./cmd/trpc-service/...
//
// What it does not prove is stated where it is asserted: a Worker that is
// SIGSTOPped or partitioned, and a Runner writing its terminal events after
// cancellation, are not stopped by any of this. See docs/session-lease.md.

// chatCredential is the API key both Workers accept. It grants the demo
// identity, which is the only one SeedDemo creates.
const chatCredential = "dual-worker-integration-key-0123456789"

// adminCredential exists only because a platform cannot be built without an
// admin authenticator. No request in this file carries it; it is long enough to
// satisfy the 32-character admin minimum.
const adminCredential = "dual-worker-admin-key-0123456789abcdef"

// leaseKeyPrefix is per-run, so a shared Redis can serve several runs at once
// and a leftover key is attributable to the run that made it.
func leaseKeyPrefix() string {
	return "sessionlease-dualworker:" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]
}

func requireRedisURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv(redisURLEnvVar)
	if url == "" {
		t.Skipf("set %s to run dual-Worker integration tests", redisURLEnvVar)
	}
	return url
}

// newRedisClient dials Redis and hands back a client the caller owns. The
// coordinator borrows it and never closes it, which is the contract this is
// exercising.
func newRedisClient(t *testing.T, url string) goredis.UniversalClient {
	t.Helper()
	opts, err := goredis.ParseURL(url)
	require.NoError(t, err, "parse %s", redisURLEnvVar)
	client := goredis.NewClient(opts)
	t.Cleanup(func() { require.NoError(t, client.Close()) })

	ctx, cancel := setupContext()
	defer cancel()
	require.NoError(t, client.Ping(ctx).Err(), "redis is not reachable")
	return client
}

// deleteLeaseKeys removes the lock and fence keys of one run. Fence keys are
// never collected by the coordinator itself — that is a documented limitation,
// not something to leave lying in a shared instance after a test.
func deleteLeaseKeys(t *testing.T, client goredis.UniversalClient, prefix string) {
	t.Helper()
	ctx, cancel := setupContext()
	defer cancel()

	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, prefix+":*", 100).Result()
		if err != nil {
			t.Errorf("scan lease keys under %s: %v", prefix, err)
			return
		}
		if len(keys) > 0 {
			if err := client.Del(ctx, keys...).Err(); err != nil {
				t.Errorf("delete lease keys under %s: %v", prefix, err)
				return
			}
		}
		if next == 0 {
			return
		}
		cursor = next
	}
}

// worker is one Worker's stack: its own storage stack over the shared schema,
// its own runtime resolver, its own lease coordinator, and its own HTTP server.
//
// It is not its own process, and its coordinator is built over a Redis client
// it shares with the other Worker's. Everything above that client — owner
// tokens, fences, leases, renewal loops — is this Worker's alone, and that is
// what is under test.
type worker struct {
	url       string
	stack     *storageStack
	resolver  *platformagent.RuntimeResolver
	leases    sessionlease.Coordinator
	directory *parkingDirectory
}

type workerOptions struct {
	// lease overrides the coordinator's timings. The zero value is production's.
	lease sessionlease.Config
	// keyPrefix is shared by both workers of a test and unique to that test.
	keyPrefix string
}

// newWorker builds a Worker the way run() does, one step at a time, so a
// difference between this and the real process would be visible here rather
// than hidden behind a helper.
//
// The lease coordinator is built directly rather than through openStorage
// because these tests need a key prefix of their own and, for the takeover
// scenario, a TTL measured in hundreds of milliseconds. That openStorage builds
// the same thing from TRPC_SERVICE_SESSION_COORDINATION is asserted separately,
// by TestIntegrationBootstrapWiresRedisCoordination below.
func newWorker(
	t *testing.T,
	dsn string,
	schema string,
	client goredis.UniversalClient,
	opts workerOptions,
) *worker {
	t.Helper()

	// A stack per Worker: two pools, two session services, two directories, all
	// pointed at one schema. Coordination is set to inmemory here only because
	// this Worker's coordinator is built below; nothing reads stack.coordinator.
	cfg := storageConfig{
		profile:      profilePostgres,
		dsn:          dsn,
		schema:       schema,
		coordination: coordinationInMemory,
	}
	require.NoError(t, cfg.validate())
	ctx, cancel := setupContext()
	defer cancel()
	stack, err := openStorage(ctx, cfg, defaultStorageDeps())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stack.close()) })

	resolver, err := platformagent.NewRuntimeResolver(
		stack.repository,
		func(
			_ context.Context,
			revision tenant.AgentRevision,
		) (*platformagent.Runtime, error) {
			// The demo revision names no secret and no policy, so the strictest
			// authorizer is the correct one: what this file exercises is the run
			// lease, and it must not be doing so under a permissive stand-in.
			return platformagent.NewRuntimeFromRevision(
				revision, stack.sessions, security.DenyCapabilities())
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resolver.Close()) })

	leases, err := redislease.New(client, redislease.Options{
		KeyPrefix: opts.keyPrefix,
		Lease:     opts.lease,
	})
	require.NoError(t, err)

	authenticator, err := identity.NewStaticAPIKeyAuthenticator(
		map[string]identity.Identity{
			chatCredential: {
				TenantID:      platformconfig.DemoTenantID,
				PrincipalID:   platformconfig.DemoPrincipalID,
				AllowedAppIDs: []string{platformconfig.DemoAgentAppID},
			},
		},
	)
	require.NoError(t, err)

	// Nothing here calls the Admin API, but the server will not be built without
	// an admin authenticator, so this Worker gets a real one rather than a nil.
	adminAuthenticator, err := identity.NewStaticAdminAPIKeyAuthenticator(
		map[string]identity.AdminIdentity{
			adminCredential: {
				Role:        identity.RolePlatformAdmin,
				PrincipalID: "dualworker-admin",
			},
		},
	)
	require.NoError(t, err)

	directory := &parkingDirectory{inner: stack.directory, release: make(chan struct{})}
	runs, err := sessionrun.NewService(resolver, directory, leases)
	require.NoError(t, err)
	api, err := web.NewPlatformServer(
		stack.repository, stack.profiles, runs, authenticator, adminAuthenticator,
		security.DenyCapabilities(),
	)
	require.NoError(t, err)

	server := httptest.NewServer(api.Handler())
	// Registered after the resolver's cleanup so it runs first: draining the
	// server before closing the resolver is the order run() uses, and the
	// reverse would have Close wait on a request that is still being served.
	t.Cleanup(server.Close)
	// The coordinator closes before the client it borrowed, and before anything
	// still running could try to renew through it.
	t.Cleanup(func() { require.NoError(t, leases.Close()) })

	return &worker{
		url:       server.URL,
		stack:     stack,
		resolver:  resolver,
		leases:    leases,
		directory: directory,
	}
}

// chat sends one turn and returns the response. The body is fully read and
// closed before returning, so the run is over by the time the caller sees it.
func (w *worker) chat(
	t *testing.T,
	ctx context.Context,
	sessionID string,
	prompt string,
) (int, http.Header, string) {
	t.Helper()
	body := fmt.Sprintf(
		`{"model":"ignored","messages":[{"role":"user","content":%q}]}`, prompt,
	)
	request, err := http.NewRequestWithContext(
		ctx, http.MethodPost, w.url+"/v1/chat/completions", strings.NewReader(body),
	)
	require.NoError(t, err)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(web.HeaderAuthorization, "Bearer "+chatCredential)
	request.Header.Set(web.HeaderAgentAppID, platformconfig.DemoAgentAppID)
	request.Header.Set(web.HeaderSessionID, sessionID)

	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer func() { require.NoError(t, response.Body.Close()) }()
	payload, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	return response.StatusCode, response.Header, string(payload)
}

// sessionKeyFor is how the conversation is addressed in the shared session
// store. The framework's app and user keys are namespaced by the platform, so
// they are not the bare identifiers a request carries: see
// platformagent.NewRuntimeFromRevision and identity.RunContext.UserID.
func sessionKeyFor(t *testing.T, sessionID string) session.Key {
	t.Helper()
	runContext := identity.RunContext{
		// Any well-formed id will do: this scope is built to compute a session
		// key, not to run anything, and the request id is not part of that key.
		// The real ones are minted per request by the HTTP entry layer.
		RequestID:   uuid.NewString(),
		TenantID:    platformconfig.DemoTenantID,
		AppID:       platformconfig.DemoAgentAppID,
		PrincipalID: platformconfig.DemoPrincipalID,
		SessionID:   sessionID,
		RevisionID:  platformconfig.DemoRevisionID,
	}
	require.NoError(t, runContext.Validate())
	return session.Key{
		AppName: fmt.Sprintf(
			"t/%s/a/%s", platformconfig.DemoTenantID, platformconfig.DemoAgentAppID),
		UserID:    runContext.UserID(),
		SessionID: sessionID,
	}
}

func runKeyFor(sessionID string) sessiondir.Key {
	return sessiondir.Key{
		TenantID:    platformconfig.DemoTenantID,
		AppID:       platformconfig.DemoAgentAppID,
		PrincipalID: platformconfig.DemoPrincipalID,
		SessionID:   sessionID,
	}
}

// parkingDirectory delegates every pin to the real one and, while parking is
// armed, holds the run at the point where it has the lease but has not yet
// resolved a revision. It is the only way to keep a Worker inside a run for as
// long as a test needs without inventing a slow model.
type parkingDirectory struct {
	inner sessiondir.Directory

	mu      sync.Mutex
	armed   bool
	arrived chan context.Context

	release     chan struct{}
	releaseOnce sync.Once
}

// park arms the barrier and returns the channel the parked run's context
// arrives on.
func (d *parkingDirectory) park() <-chan context.Context {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.armed = true
	d.arrived = make(chan context.Context, 1)
	return d.arrived
}

func (d *parkingDirectory) GetPin(
	ctx context.Context,
	key sessiondir.Key,
) (string, bool, error) {
	return d.inner.GetPin(ctx, key)
}

func (d *parkingDirectory) EnsurePin(
	ctx context.Context,
	key sessiondir.Key,
	candidateRevisionID string,
) (string, error) {
	d.mu.Lock()
	armed, arrived := d.armed, d.arrived
	d.armed = false
	d.mu.Unlock()
	if armed {
		select {
		case arrived <- ctx:
		default:
		}
		<-d.release
	}
	return d.inner.EnsurePin(ctx, key, candidateRevisionID)
}

func (d *parkingDirectory) releaseAll() {
	d.releaseOnce.Do(func() { close(d.release) })
}

// awaitParked returns the run context of the parked request.
func awaitParked(t *testing.T, arrived <-chan context.Context) context.Context {
	t.Helper()
	select {
	case ctx := <-arrived:
		return ctx
	case <-time.After(integrationTimeout):
		t.Fatal("no request reached the parking directory")
		return nil
	}
}

// TestIntegrationTwoWorkersShareOneSession is the claim this Commit makes,
// checked over the wire between two independent Worker stacks over one
// PostgreSQL schema and one Redis. See the file comment for what "two Workers"
// does and does not cover here.
func TestIntegrationTwoWorkersShareOneSession(t *testing.T) {
	dsn := requireIntegration(t)
	redisURL := requireRedisURL(t)
	schema := createSchema(t, dsn)
	ctx := integrationContext(t)

	client := newRedisClient(t, redisURL)
	prefix := leaseKeyPrefix()
	t.Cleanup(func() { deleteLeaseKeys(t, client, prefix) })

	opts := workerOptions{keyPrefix: prefix}
	alpha := newWorker(t, dsn, schema, client, opts)
	beta := newWorker(t, dsn, schema, client, opts)
	t.Cleanup(alpha.directory.releaseAll)
	t.Cleanup(beta.directory.releaseAll)
	require.NoError(t, platformconfig.SeedDemo(ctx, alpha.stack.repository))

	sessionID := "shared-" + uuid.NewString()[:8]
	t.Cleanup(func() {
		cleanupCtx, cancel := setupContext()
		defer cancel()
		_ = alpha.stack.sessions.DeleteSession(cleanupCtx, sessionKeyFor(t, sessionID))
	})

	// Alpha starts a run and stops inside it, holding the lease.
	arrived := alpha.directory.park()
	type answer struct {
		status int
		header http.Header
		body   string
	}
	alphaDone := make(chan answer, 1)
	go func() {
		status, header, body := alpha.chat(t, ctx, sessionID, "first turn")
		alphaDone <- answer{status, header, body}
	}()
	awaitParked(t, arrived)

	t.Run("the second Worker is refused", func(t *testing.T) {
		status, header, body := beta.chat(t, ctx, sessionID, "second turn")
		require.Equal(t, http.StatusConflict, status, body)
		require.Contains(t, body, `"code":"session_busy"`)
		require.Equal(t, "2", header.Get(web.HeaderRetryAfter))
		// Refused before it could pin or build a runtime: a rejected request must
		// leave no trace in the session's history.
		require.Equal(t, 0, beta.resolver.CacheSize())
	})

	t.Run("other sessions are unaffected", func(t *testing.T) {
		// Beta is not blocked in general — only on the session alpha holds. Two
		// different conversations run at the same time through both Workers.
		others := []struct {
			runner    *worker
			sessionID string
		}{
			{runner: beta, sessionID: "other-b-" + uuid.NewString()[:8]},
			{runner: alpha, sessionID: "other-a-" + uuid.NewString()[:8]},
		}
		var wg sync.WaitGroup
		results := make([]int, len(others))
		bodies := make([]string, len(others))
		for i, other := range others {
			wg.Add(1)
			go func(i int, runner *worker, sessionID string) {
				defer wg.Done()
				results[i], _, bodies[i] = runner.chat(t, ctx, sessionID, "parallel")
			}(i, other.runner, other.sessionID)
		}
		wg.Wait()
		for i, other := range others {
			require.Equal(t, http.StatusOK, results[i], bodies[i])
			cleanupCtx, cancel := setupContext()
			_ = other.runner.stack.sessions.DeleteSession(
				cleanupCtx, sessionKeyFor(t, other.sessionID))
			cancel()
		}
	})

	// Alpha finishes and releases.
	alpha.directory.releaseAll()
	first := <-alphaDone
	require.Equal(t, http.StatusOK, first.status, first.body)
	require.Equal(t, platformconfig.DemoRevisionID, first.header.Get(web.HeaderAgentRevisionID))

	t.Run("the other Worker continues the conversation", func(t *testing.T) {
		status, header, body := beta.chat(t, ctx, sessionID, "second turn")
		require.Equal(t, http.StatusOK, status, body)
		// Beta serves it under alpha's pin, which it read from the shared
		// directory rather than resolving for itself.
		require.Equal(
			t, platformconfig.DemoRevisionID, header.Get(web.HeaderAgentRevisionID))

		// And it saw alpha's turn: the session it continued is the persisted one,
		// not a fresh one it created because the first Worker's writes were
		// somewhere else.
		loaded, err := beta.stack.sessions.GetSession(ctx, sessionKeyFor(t, sessionID))
		require.NoError(t, err)
		require.NotNil(t, loaded)
		var prompts []string
		for _, recorded := range loaded.Events {
			if recorded.Response == nil {
				continue
			}
			for _, choice := range recorded.Response.Choices {
				prompts = append(prompts, choice.Message.Content)
			}
		}
		require.Contains(t, strings.Join(prompts, "\n"), "first turn",
			"the second Worker must read what the first one wrote")
		require.Contains(t, strings.Join(prompts, "\n"), "second turn")
	})
}

// TestIntegrationTakeoverStopsTheStalledWorker is the cancellation half, and
// the honest one. A Worker that stalls past its TTL loses the session; the
// Worker that takes it over gets in; the stalled run is told, and stops.
//
// "Stops" is all that is asserted. The stalled Worker is not prevented from
// writing — nothing here can do that, and the tail the Runner emits after
// cancellation is expected. What must be true is that the writing ends, rather
// than continuing for as long as the stalled run would have.
func TestIntegrationTakeoverStopsTheStalledWorker(t *testing.T) {
	dsn := requireIntegration(t)
	redisURL := requireRedisURL(t)
	schema := createSchema(t, dsn)
	ctx := integrationContext(t)

	client := newRedisClient(t, redisURL)
	prefix := leaseKeyPrefix()
	t.Cleanup(func() { deleteLeaseKeys(t, client, prefix) })

	// Short enough that a stall becomes a takeover within a test's patience, and
	// long enough that a round trip to a containerised Redis on a machine busy
	// running the rest of this suite is not mistaken for a failed Worker.
	timings := sessionlease.Config{
		TTL:           2 * time.Second,
		RenewInterval: 400 * time.Millisecond,
		SafetyMargin:  400 * time.Millisecond,
	}
	opts := workerOptions{keyPrefix: prefix, lease: timings}
	stalled := newWorker(t, dsn, schema, client, opts)
	successor := newWorker(t, dsn, schema, client, opts)
	t.Cleanup(stalled.directory.releaseAll)
	require.NoError(t, platformconfig.SeedDemo(ctx, stalled.stack.repository))

	sessionID := "takeover-" + uuid.NewString()[:8]
	t.Cleanup(func() {
		cleanupCtx, cancel := setupContext()
		defer cancel()
		_ = stalled.stack.sessions.DeleteSession(cleanupCtx, sessionKeyFor(t, sessionID))
	})

	arrived := stalled.directory.park()
	stalledDone := make(chan int, 1)
	go func() {
		status, _, _ := stalled.chat(t, ctx, sessionID, "stalled turn")
		stalledDone <- status
	}()
	runCtx := awaitParked(t, arrived)
	require.NoError(t, runCtx.Err(), "the parked run holds a live lease")

	// While it renews, the session stays its own: a Worker that is merely slow
	// is not a Worker that has failed.
	_, err := successor.leases.Acquire(ctx, runKeyFor(sessionID))
	require.ErrorIs(t, err, sessionlease.ErrSessionBusy,
		"renewal must hold the session for a run that is still alive")

	// Now it stalls for real: renewal is stopped the way a lost connection or a
	// killed process stops it, by taking the coordinator away.
	require.NoError(t, stalled.leases.Close())

	select {
	case <-runCtx.Done():
	case <-time.After(integrationTimeout):
		t.Fatal("the stalled run was never told it had lost the session")
	}
	require.ErrorIs(t, runCtx.Err(), context.Canceled)

	// The successor gets in, once the abandoned lock has expired. It was never
	// deleted — the TTL is what covers the tail writes of the run being
	// cancelled right now.
	var takenOver sessionlease.Lease
	require.Eventually(t, func() bool {
		lease, err := successor.leases.Acquire(ctx, runKeyFor(sessionID))
		if err != nil {
			return false
		}
		takenOver = lease
		return true
	}, integrationTimeout, 20*time.Millisecond, "the abandoned session never became available")
	require.Greater(t, takenOver.Fence(), uint64(0))

	// The stalled request is released and unwinds, with the successor holding
	// the session throughout. This is where its tail writes, if any, happen.
	stalled.directory.releaseAll()
	<-stalledDone
	// The takeover is what this test claims; holding the session across the
	// sampling below is not, and a lease kept alive through a second of
	// unrelated work is only a way to make this fail on a busy machine.
	require.NoError(t, takenOver.Release(ctx))

	// It stopped. Sampled twice a second apart: a run that was still going would
	// still be appending, and this asserts it is not — not that it never wrote
	// after being cancelled, which it may well have.
	settled := countSessionEvents(t, ctx, successor.stack.sessions, sessionKeyFor(t, sessionID))
	time.Sleep(time.Second)
	require.Equal(t,
		settled,
		countSessionEvents(t, ctx, successor.stack.sessions, sessionKeyFor(t, sessionID)),
		"a cancelled run must stop writing, even though the writes it already "+
			"started could not be rejected")
}

func countSessionEvents(
	t *testing.T,
	ctx context.Context,
	sessions session.Service,
	key session.Key,
) int {
	t.Helper()
	loaded, err := sessions.GetSession(ctx, key)
	require.NoError(t, err)
	if loaded == nil {
		return 0
	}
	return len(loaded.Events)
}

// TestIntegrationBootstrapWiresRedisCoordination closes the gap the tests above
// leave: they build their coordinators directly, so nothing in them would
// notice if TRPC_SERVICE_SESSION_COORDINATION=redis produced a coordinator that
// coordinates with nobody. Two stacks opened the way the process opens one must
// be mutually exclusive.
func TestIntegrationBootstrapWiresRedisCoordination(t *testing.T) {
	dsn := requireIntegration(t)
	redisURL := requireRedisURL(t)
	schema := createSchema(t, dsn)
	ctx := integrationContext(t)

	cfg := storageConfig{
		profile:      profilePostgres,
		dsn:          dsn,
		schema:       schema,
		coordination: coordinationRedis,
		redisURL:     redisURL,
	}
	require.NoError(t, cfg.validate())

	open := func() *storageStack {
		openCtx, cancel := setupContext()
		defer cancel()
		stack, err := openStorage(openCtx, cfg, defaultStorageDeps())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, stack.close()) })
		return stack
	}
	first, second := open(), open()

	// The default key prefix is deliberately not overridden here: this is the
	// keyspace the shipped process uses. The session id is unique per run, so
	// the two keys below belong to this test and are removed with it.
	key := runKeyFor("bootstrap-" + uuid.NewString()[:8])
	janitor := newRedisClient(t, redisURL)
	t.Cleanup(func() {
		deleteLeaseKeys(
			t, janitor,
			redislease.DefaultKeyPrefix+":{"+sessionlease.KeyDigest(key)+"}",
		)
	})

	lease, err := first.coordinator.Acquire(ctx, key)
	require.NoError(t, err)
	_, err = second.coordinator.Acquire(ctx, key)
	require.ErrorIs(t, err, sessionlease.ErrSessionBusy,
		"two Workers configured for redis coordination must exclude each other")

	require.NoError(t, lease.Release(ctx))
	inherited, err := second.coordinator.Acquire(ctx, key)
	require.NoError(t, err)
	require.Greater(t, inherited.Fence(), lease.Fence(),
		"the fence advances across Workers, even though nothing enforces it")
	require.NoError(t, inherited.Release(ctx))
}
