package main

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"trpc.group/trpc-go/trpc-agent-go/session"

	platformconfig "github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionbackend"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storagebundle"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// This file closes the BackendProfile loop against real servers. Everything
// else about a dynamic profile is argued from unit tests with injected seams —
// a fake constructor, a fake probe, a map for an environment. Those prove the
// order of the steps; they cannot prove that the arrangement a tenant wrote
// down actually reaches a database, that the conversation lands in the tenant's
// own tables rather than the process's, or that the profile is still there
// after a restart.
//
// It reuses the gate and the environment variables of the bootstrap suite
// rather than adding a third set:
//
//	docker compose -f deploy/docker-compose.session.yml up -d --wait
//	TRPC_SERVICE_SESSION_INTEGRATION=1 \
//	TRPC_SERVICE_POSTGRES_DSN='postgres://trpc:trpc-local-dev@127.0.0.1:55432/trpc_session?sslmode=disable' \
//	TRPC_SERVICE_REDIS_URL='redis://:trpc-local-dev@127.0.0.1:56379/0' \
//	go test -race -count=1 -timeout 300s -run Integration ./cmd/trpc-service/...
//
// The tenant's credentials are handed to the Factory through a getenv this file
// builds, not through the real environment, so nothing here depends on a
// variable an operator has to remember to export beyond the two above.

const (
	// tenantDSNEnvVar and tenantRedisEnvVar are the names a tenant's profile
	// references. They are deliberately outside the TRPC_SERVICE_ namespace:
	// that prefix is reserved for the platform's own credentials and
	// security.NewEntitlements refuses to grant anything under it, so a profile
	// naming one could never be built. Pointing a tenant profile at the
	// platform's own DSN variable is exactly the confusion the reservation
	// exists to prevent, and these tests would not notice it if they used a
	// name that could never be entitled in the first place.
	tenantDSNEnvVar   = "INTEGRATION_TENANT_SESSION_DSN"
	tenantRedisEnvVar = "INTEGRATION_TENANT_SESSION_REDIS_URL"
)

// tenantEnv returns the getenv a Factory reads secret references through.
//
// It answers exactly the names given and records every other lookup as a
// failure. That tripwire is the point: the Factory must resolve the reference
// the profile names and nothing else, and a Factory that fell back to the
// process's own TRPC_SERVICE_POSTGRES_DSN would otherwise produce a passing
// round trip against the wrong database.
//
// The names are collected under a mutex and reported from a cleanup rather than
// from the lookup itself, because an abandoned build can outlive the test body
// and t.Errorf after that point panics.
func tenantEnv(t *testing.T, values map[string]string) func(string) string {
	t.Helper()
	var (
		mu         sync.Mutex
		unexpected []string
	)
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		if len(unexpected) > 0 {
			t.Errorf("the factory read %v, which no profile in this test names", unexpected)
		}
	})
	return func(name string) string {
		mu.Lock()
		defer mu.Unlock()
		value, found := values[name]
		if !found {
			unexpected = append(unexpected, name)
			return ""
		}
		return value
	}
}

// tenantCapabilities entitles the demo tenant to exactly the secret references
// given, the way a deployment's manifest would.
func tenantCapabilities(t *testing.T, refs ...string) security.CapabilityAuthorizer {
	t.Helper()
	entitlements, err := security.NewEntitlements(security.Grant{
		TenantID:   platformconfig.DemoTenantID,
		SecretRefs: refs,
	})
	require.NoError(t, err)
	return entitlements
}

// dynamicStack opens the Router and the RuntimeResolver over a booted storage
// stack, exactly as the process does, and returns a shutdown the caller may run
// early.
//
// The shutdown is guarded by a sync.Once so a test that restarts mid-body can
// close explicitly without the registered cleanup closing a second time. The
// cleanup itself is what keeps the order right: it is registered after
// bootstrap's, so LIFO runs Runtimes, then Bundles, then storage — the order
// runtimeStack.close and storageStack.close were written to be used in.
func dynamicStack(
	t *testing.T,
	dsn, schema string,
	stack *storageStack,
	getenv func(string) string,
	capabilities security.CapabilityAuthorizer,
) (*runtimeStack, func()) {
	t.Helper()
	runtimes, err := openRuntimeStack(bootstrapConfig(dsn, schema), stack, getenv, capabilities)
	require.NoError(t, err)
	require.NotNil(t, runtimes)

	var once sync.Once
	shutdown := func() {
		once.Do(func() {
			if err := runtimes.close(); err != nil {
				t.Errorf("close runtime stack: %v", err)
			}
		})
	}
	t.Cleanup(shutdown)
	return runtimes, shutdown
}

// resolveBundle resolves a profile through the Router and returns the session
// store it names, with a release the caller may run early.
//
// Release is guarded and registered as a cleanup for one reason: Router.Close
// waits for every outstanding lease, so a lease left held by a failed assertion
// would turn one failure into a run that hangs until the package timeout.
func resolveBundle(
	ctx context.Context,
	t *testing.T,
	runtimes *runtimeStack,
	scope tenant.TenantContext,
	profileID string,
) (session.Service, func()) {
	t.Helper()
	lease, err := runtimes.router.Resolve(ctx, scope, profileID)
	require.NoError(t, err)
	require.NotNil(t, lease)

	var once sync.Once
	release := func() {
		once.Do(func() {
			if err := lease.Release(); err != nil {
				t.Errorf("release bundle lease: %v", err)
			}
		})
	}
	t.Cleanup(release)

	sessions := lease.Bundle().Session
	require.NotNil(t, sessions, "a resolved bundle must carry a session store")
	return sessions, release
}

// tenantSessionKey is one conversation of the demo tenant, named per run so a
// shared server can serve several at once.
func tenantSessionKey(prefix string) session.Key {
	return session.Key{
		AppName:   platformconfig.DemoAgentAppID,
		UserID:    platformconfig.DemoPrincipalID,
		SessionID: prefix + "-" + uuid.New().String()[:8],
	}
}

// writeTurn records one complete turn and reads it back through the same store.
func writeTurn(ctx context.Context, t *testing.T, sessions session.Service, key session.Key, text string) {
	t.Helper()
	created, err := sessions.CreateSession(ctx, key, session.StateMap{})
	require.NoError(t, err)
	require.NotNil(t, created)

	// Both events of a turn: upstream drops an assistant message with no user
	// message before it, so a one-event session reads back empty.
	prompt, reply := newTurn("inv-"+key.SessionID, text, "reply to "+text)
	require.NoError(t, sessions.AppendEvent(ctx, created, prompt))
	require.NoError(t, sessions.AppendEvent(ctx, created, reply))
	requireTurn(ctx, t, sessions, key, text)
}

// requireTurn asserts the turn writeTurn recorded is readable through this
// store.
func requireTurn(ctx context.Context, t *testing.T, sessions session.Service, key session.Key, text string) {
	t.Helper()
	loaded, err := sessions.GetSession(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, loaded, "the conversation is not in the store it was written to")
	require.Len(t, loaded.Events, 2, "both events of a valid turn must survive the backend write")
	require.NotNil(t, loaded.Events[0].Response)
	require.Len(t, loaded.Events[0].Response.Choices, 1)
	require.Equal(t, text, loaded.Events[0].Response.Choices[0].Message.Content)
	require.Equal(t, "reply to "+text, loaded.Events[1].Response.Choices[0].Message.Content)
}

// requireNotInDefaultStore is the isolation claim, and it is the one that makes
// the rest of these tests mean anything: a round trip through a Bundle proves
// only that some store works. A miss in this process's own store is what proves
// the conversation went to the tenant's.
//
// Both backends report a session that was never created as (nil, nil), so this
// is a miss rather than an error.
func requireNotInDefaultStore(ctx context.Context, t *testing.T, stack *storageStack, key session.Key) {
	t.Helper()
	missing, err := stack.sessions.GetSession(ctx, key)
	require.NoError(t, err)
	require.Nil(t, missing, "the conversation reached this process's default store")
}

// deleteSession removes a conversation on a context that does not inherit the
// test body's, so it still runs from a cleanup. Best effort: it is reported,
// not required, because a failure here must not mask the body's own result.
func deleteSession(t *testing.T, sessions session.Service, key session.Key) {
	t.Helper()
	ctx, cancel := setupContext()
	defer cancel()
	if err := sessions.DeleteSession(ctx, key); err != nil {
		t.Logf("delete session %s: %v", key.SessionID, err)
	}
}

// postgresProfileFor is the arrangement a tenant would publish to keep its
// conversations in tables of its own: the same server the platform runs on,
// named through a reference, with a prefix nothing else writes under.
func postgresProfileFor(profileID, schema, tablePrefix string) storagebundle.Profile {
	return storagebundle.Profile{
		TenantID: platformconfig.DemoTenantID,
		ID:       profileID,
		Session: storagebundle.SessionSpec{
			Backend: sessionbackend.BackendPostgres,
			Postgres: &storagebundle.PostgresSpec{
				DSNRef:      "env:" + tenantDSNEnvVar,
				Schema:      schema,
				TablePrefix: tablePrefix,
			},
		},
	}
}

// TestIntegrationDynamicPostgresSessionRoundTrip is the production loop end to
// end: a profile is created through the control plane's repository, resolved
// through the Router the serving process uses, and the store it produces is a
// different store from the one this process booted with.
func TestIntegrationDynamicPostgresSessionRoundTrip(t *testing.T) {
	dsn := requireIntegration(t)
	schema := createSchemaNamed(t, dsn, profileSchemaPrefix)
	ctx := integrationContext(t)
	scope := tenant.TenantContext{TenantID: platformconfig.DemoTenantID}

	const (
		profileID   = "tenant-owned-postgres"
		tablePrefix = "tenant"
	)

	stack := bootstrap(t, dsn, schema)
	require.NoError(t, platformconfig.SeedDemo(ctx, stack.repository))

	// Written through the same repository the Admin API writes through. There
	// is no second view of it: the Router below resolves out of this value.
	record, err := stack.profiles.CreateProfile(
		ctx, scope, postgresProfileFor(profileID, schema, tablePrefix), "integration-operator")
	require.NoError(t, err)
	require.Equal(t, profileID, record.ID)
	require.Equal(t, "integration-operator", record.CreatedBy)

	runtimes, _ := dynamicStack(t, dsn, schema, stack,
		tenantEnv(t, map[string]string{tenantDSNEnvVar: dsn}),
		tenantCapabilities(t, "env:"+tenantDSNEnvVar))

	sessions, _ := resolveBundle(ctx, t, runtimes, scope, profileID)
	require.NotSame(t, stack.sessions, sessions,
		"a profile that names its own storage must not resolve to the process default")

	key := tenantSessionKey("dynamic-postgres")
	t.Cleanup(func() { deleteSession(t, sessions, key) })
	writeTurn(ctx, t, sessions, key, "hello from the tenant's own tables")
	requireNotInDefaultStore(ctx, t, stack, key)

	t.Run("the tenant's tables exist beside the platform's", func(t *testing.T) {
		// Upstream builds "<prefix>_<base>" for every one of its tables, so the
		// prefixed set and the unprefixed set live in one schema without
		// colliding. Both have to be there: only the prefixed one proves the
		// profile's TablePrefix reached the constructor, and only the
		// unprefixed one proves the process's own store is still intact.
		inspector := openInspector(t, dsn)
		require.True(t, tableExists(t, ctx, inspector, schema, tablePrefix+"_session_events"),
			"the profile's table prefix never reached the upstream constructor")
		require.True(t, tableExists(t, ctx, inspector, schema, "session_events"),
			"the process's own session tables are missing")
	})

	t.Run("a second resolution is the same bundle", func(t *testing.T) {
		// One Bundle per profile, not one per resolution: every session pinned
		// to a revision that names this profile has to share the connection
		// pool behind it, and a second build would double the pools for as long
		// as both were cached.
		again, release := resolveBundle(ctx, t, runtimes, scope, profileID)
		defer release()
		require.Same(t, sessions, again)
		require.Equal(t, 1, runtimes.router.CacheSize())
		requireTurn(ctx, t, again, key, "hello from the tenant's own tables")
	})
}

// TestIntegrationDynamicProfileSurvivesARestart is the other half of "the
// tenant published it": an arrangement that has to be re-entered after every
// restart is not published, it is configured.
//
// It restarts the whole process the way the bootstrap suite does — close the
// stack, open a new one on the same schema — and requires the profile, its
// fingerprint, its provenance and the conversation written through it to all
// still be there and to still agree with each other.
func TestIntegrationDynamicProfileSurvivesARestart(t *testing.T) {
	dsn := requireIntegration(t)
	schema := createSchemaNamed(t, dsn, profileSchemaPrefix)
	ctx := integrationContext(t)
	scope := tenant.TenantContext{TenantID: platformconfig.DemoTenantID}

	const (
		profileID   = "tenant-owned-durable"
		tablePrefix = "durable"
	)
	getenv := tenantEnv(t, map[string]string{tenantDSNEnvVar: dsn})
	capabilities := tenantCapabilities(t, "env:"+tenantDSNEnvVar)
	key := tenantSessionKey("restart-profile")

	// First boot: publish the arrangement and hold one conversation in it.
	before := bootstrap(t, dsn, schema)
	require.NoError(t, platformconfig.SeedDemo(ctx, before.repository))
	published, err := before.profiles.CreateProfile(
		ctx, scope, postgresProfileFor(profileID, schema, tablePrefix), "integration-operator")
	require.NoError(t, err)

	beforeRuntimes, closeBeforeRuntimes := dynamicStack(t, dsn, schema, before, getenv, capabilities)
	beforeSessions, releaseBefore := resolveBundle(ctx, t, beforeRuntimes, scope, profileID)
	writeTurn(ctx, t, beforeSessions, key, "hello before the restart")

	// The restart. The lease goes first because the Router waits for it, then
	// the Runtimes and Bundles, then the storage underneath them — nothing of
	// this stack is used again.
	releaseBefore()
	closeBeforeRuntimes()
	require.NoError(t, before.close())

	after := bootstrap(t, dsn, schema)
	// A restarted process seeds again, which must not disturb what is there.
	require.NoError(t, platformconfig.SeedDemo(ctx, after.repository))

	t.Run("the profile survived", func(t *testing.T) {
		found, err := after.profiles.GetProfile(ctx, scope, profileID)
		require.NoError(t, err)
		require.Equal(t, published.Fingerprint, found.Fingerprint,
			"a profile that came back with a different fingerprint is different storage")
		require.Equal(t, published.CreatedBy, found.CreatedBy)
		require.Equal(t, published.CreatedAt, found.CreatedAt)
		require.Equal(t, published.Profile, found.Profile)

		// GetProfile verifies the row against its recorded fingerprint, so this
		// also proves the round trip through PostgreSQL did not change the
		// content: a spec that came back with a dropped optional field would be
		// reported as an integrity fault rather than reaching here.
		require.NotNil(t, found.Session.Postgres)
		require.Equal(t, tablePrefix, found.Session.Postgres.TablePrefix)
		require.Equal(t, schema, found.Session.Postgres.Schema)
		require.Equal(t, "env:"+tenantDSNEnvVar, found.Session.Postgres.DSNRef)
	})

	t.Run("the conversation survived", func(t *testing.T) {
		afterRuntimes, _ := dynamicStack(t, dsn, schema, after, getenv, capabilities)
		afterSessions, _ := resolveBundle(ctx, t, afterRuntimes, scope, profileID)
		t.Cleanup(func() { deleteSession(t, afterSessions, key) })

		require.NotSame(t, after.sessions, afterSessions)
		requireTurn(ctx, t, afterSessions, key, "hello before the restart")
		requireNotInDefaultStore(ctx, t, after, key)
	})
}

// TestIntegrationDynamicRedisSessionRoundTrip is the same loop against the
// other backend, and it is not redundant: PostgreSQL and Redis reach upstream
// through different constructors, different probes and different namespacing
// rules, and this process's own store is PostgreSQL either way. A tenant on
// Redis is therefore the case where the Bundle shares nothing at all with the
// process it runs in.
func TestIntegrationDynamicRedisSessionRoundTrip(t *testing.T) {
	dsn := requireIntegration(t)
	redisURL := requireRedisURL(t)
	schema := createSchema(t, dsn)
	ctx := integrationContext(t)
	scope := tenant.TenantContext{TenantID: platformconfig.DemoTenantID}

	const profileID = "tenant-owned-redis"
	// Per-run, so a shared Redis can serve several runs at once and a leftover
	// key is attributable to the run that made it. Within the 32 characters and
	// the character set a key prefix is allowed.
	keyPrefix := "tenantsess-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:16]

	stack := bootstrap(t, dsn, schema)
	require.NoError(t, platformconfig.SeedDemo(ctx, stack.repository))

	_, err := stack.profiles.CreateProfile(ctx, scope, storagebundle.Profile{
		TenantID: scope.TenantID,
		ID:       profileID,
		Session: storagebundle.SessionSpec{
			Backend: sessionbackend.BackendRedis,
			Redis: &storagebundle.RedisSpec{
				URLRef:    "env:" + tenantRedisEnvVar,
				KeyPrefix: keyPrefix,
			},
		},
	}, "integration-operator")
	require.NoError(t, err)

	client := newRedisClient(t, redisURL)
	t.Cleanup(func() { deleteKeysUnder(t, client, keyPrefix) })

	runtimes, _ := dynamicStack(t, dsn, schema, stack,
		tenantEnv(t, map[string]string{tenantRedisEnvVar: redisURL}),
		tenantCapabilities(t, "env:"+tenantRedisEnvVar))

	sessions, _ := resolveBundle(ctx, t, runtimes, scope, profileID)
	require.NotSame(t, stack.sessions, sessions)

	key := tenantSessionKey("dynamic-redis")
	t.Cleanup(func() { deleteSession(t, sessions, key) })
	writeTurn(ctx, t, sessions, key, "hello from the tenant's own redis")

	// The process is on PostgreSQL and the tenant is on Redis, so the miss here
	// is across two different servers.
	requireNotInDefaultStore(ctx, t, stack, key)

	// And the keys are under the prefix the profile named. Without this, a
	// Redis service built with the prefix dropped would pass every assertion
	// above while writing where another tenant reads.
	require.NotEmpty(t, keysUnder(t, client, keyPrefix),
		"the profile's key prefix never reached the upstream constructor")
}

// keysUnder returns every key of one namespace. It scans rather than using KEYS
// so it stays usable against a shared instance.
func keysUnder(t *testing.T, client goredis.UniversalClient, prefix string) []string {
	t.Helper()
	ctx, cancel := setupContext()
	defer cancel()

	var (
		found  []string
		cursor uint64
	)
	for {
		keys, next, err := client.Scan(ctx, cursor, prefix+"*", 100).Result()
		require.NoErrorf(t, err, "scan keys under %s", prefix)
		found = append(found, keys...)
		if next == 0 {
			return found
		}
		cursor = next
	}
}

// deleteKeysUnder removes one run's session keys. Upstream expires them
// eventually, but "eventually" is not something to leave lying in a shared
// instance after a test.
func deleteKeysUnder(t *testing.T, client goredis.UniversalClient, prefix string) {
	t.Helper()
	keys := keysUnder(t, client, prefix)
	if len(keys) == 0 {
		return
	}
	ctx, cancel := setupContext()
	defer cancel()
	if err := client.Del(ctx, keys...).Err(); err != nil {
		t.Errorf("delete keys under %s: %v", prefix, err)
	}
}
