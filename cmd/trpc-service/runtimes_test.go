package main

import (
	"context"
	"sync"
	"testing"
	"time"

	platformagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	platformconfig "github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storagebundle"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

// countingSessions is the process session store, instrumented.
//
// It wraps a working service rather than stubbing one out: these tests build
// real Runtimes on it, and what has to be observable is only who closes it —
// the storageStack owns that store, and the Router must never take it.
type countingSessions struct {
	session.Service

	mu     sync.Mutex
	closes int
}

func (s *countingSessions) Close() error {
	s.mu.Lock()
	s.closes++
	s.mu.Unlock()
	return s.Service.Close()
}

func (s *countingSessions) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closes
}

// seededStack builds the storage a Worker runs on, in memory, with the demo
// tenant seeded so a revision can actually be resolved through it.
//
// The profile repository is gated by the repository beside it, which is what
// openInMemoryStorage does: a profile cannot be created here for a tenant this
// control plane never created.
func seededStack(t *testing.T) (*storageStack, *countingSessions) {
	t.Helper()
	sessions := &countingSessions{Service: sessioninmemory.NewSessionService()}
	repository := tenant.NewMemoryRepository()
	profiles, err := storagebundle.NewMemoryProfileRepository(repository)
	require.NoError(t, err)
	stack := &storageStack{
		repository: repository,
		profiles:   profiles,
		directory:  sessiondir.NewMemoryDirectory(),
		sessions:   sessions,
	}
	require.NoError(t, platformconfig.SeedDemo(context.Background(), stack.repository))
	return stack, sessions
}

func demoScope() tenant.TenantContext {
	return tenant.TenantContext{TenantID: platformconfig.DemoTenantID}
}

// noEnv is the environment a build that must not reach one is handed: every
// lookup fails the test.
//
// It is an assertion, not a stub. The Factory resolves credentials from
// whatever getenv the process handed it, and "this refusal came before any
// environment variable was read" is only checkable if reading one is
// observable — which is the whole substance of the entitlement being checked
// first.
func noEnv(t *testing.T) func(string) string {
	t.Helper()
	return func(name string) string {
		t.Errorf("the session factory read environment variable %q", name)
		return ""
	}
}

// recordingEnv answers nothing and remembers what was asked for. Nothing it
// holds is a credential: what matters is which variable the Factory reached
// for, and that it reached for it through the function this process was
// configured with rather than through os.Getenv.
type recordingEnv struct {
	mu    sync.Mutex
	asked []string
}

func (e *recordingEnv) getenv(name string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.asked = append(e.asked, name)
	return ""
}

func (e *recordingEnv) names() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.asked...)
}

// durableTestConfig is the configuration of a single Worker on a persistent
// store. Only processConstraints reads it here, so it is what a test uses to
// stage a process whose pins survive a restart without opening one.
func durableTestConfig() storageConfig {
	return storageConfig{profile: profilePostgres, coordination: coordinationInMemory}
}

// The Router this process assembles borrows the store the process opened, and
// resolves the empty profile id to it. Nothing else is reachable.
func TestOpenRuntimeStackServesTheProcessDefaultStore(t *testing.T) {
	stack, sessions := seededStack(t)
	t.Cleanup(func() { require.NoError(t, stack.close()) })

	runtimes, err := openRuntimeStack(
		inMemoryTestConfig(), stack, noEnv(t), security.DenyCapabilities())
	require.NoError(t, err)

	lease, err := runtimes.router.Resolve(context.Background(), demoScope(), "")
	require.NoError(t, err)
	require.Same(t, stack.sessions, lease.Bundle().Session)
	require.NoError(t, lease.Release())

	require.NoError(t, runtimes.close())
	require.Zero(t, sessions.closeCount(), "the Router closed a store it only borrowed")
}

// A profile id this tenant never created cannot be honoured, and a revision
// that names one is refused rather than served by the default store it did not
// ask for.
func TestOpenRuntimeStackRefusesEveryNamedProfile(t *testing.T) {
	stack, _ := seededStack(t)
	t.Cleanup(func() { require.NoError(t, stack.close()) })

	runtimes, err := openRuntimeStack(
		inMemoryTestConfig(), stack, noEnv(t), security.DenyCapabilities())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtimes.close()) })

	lease, err := runtimes.router.Resolve(context.Background(), demoScope(), "tenant-postgres")
	require.ErrorIs(t, err, storagebundle.ErrProfileNotFound)
	require.Nil(t, lease)

	// And the same refusal reaches a Runtime built the way the process builds
	// them, so the revision is refused rather than quietly re-pointed.
	revision, err := stack.repository.ResolveRevision(
		context.Background(), demoScope(), platformconfig.DemoAgentAppID, "")
	require.NoError(t, err)
	revision.Config.BackendProfileID = "tenant-postgres"
	digest, err := revision.Config.Digest()
	require.NoError(t, err)
	revision.ConfigDigest = digest

	runtime, err := platformagent.NewRuntime(
		context.Background(), revision, runtimes.router, security.DenyCapabilities())
	require.ErrorIs(t, err, storagebundle.ErrProfileNotFound)
	require.Nil(t, runtime)
}

// The Router resolves through the same repository the Admin API writes
// through, so a profile the control plane just accepted is reachable from the
// data plane without anything in between being told about it.
//
// This is the assembly claim of openRuntimeStack, and it cannot be made by
// either half alone: a process that built a second view over the same database
// would pass every test of both halves and still have a window in which the two
// disagreed about what a tenant published.
func TestOpenRuntimeStackResolvesProfilesTheControlPlaneCreated(t *testing.T) {
	stack, sessions := seededStack(t)
	t.Cleanup(func() { require.NoError(t, stack.close()) })

	runtimes, err := openRuntimeStack(
		inMemoryTestConfig(), stack, noEnv(t), security.DenyCapabilities())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtimes.close()) })

	// An in-process store, which is the one backend this process's constraints
	// allow and the one that needs no credential — so the build is real without
	// this test reaching a network or an environment.
	created, err := stack.profiles.CreateProfile(
		context.Background(),
		demoScope(),
		storagebundle.Profile{
			TenantID: platformconfig.DemoTenantID,
			ID:       "isolated",
			Session:  storagebundle.SessionSpec{Backend: "inmemory"},
		},
		"runtimes-test",
	)
	require.NoError(t, err)
	require.Equal(t, "isolated", created.ID)

	lease, err := runtimes.router.Resolve(context.Background(), demoScope(), "isolated")
	require.NoError(t, err)
	require.NotNil(t, lease.Bundle().Session)
	// A store of its own, not the process default under another name. A Router
	// that quietly fell back would be indistinguishable from one that worked,
	// right up to the point where two tenants shared one conversation history.
	require.NotSame(t, stack.sessions, lease.Bundle().Session)
	require.NoError(t, lease.Release())

	// And a Runtime built the way the process builds them lands on that same
	// store, rather than on the default the revision did not ask for.
	revision, err := stack.repository.ResolveRevision(
		context.Background(), demoScope(), platformconfig.DemoAgentAppID, "")
	require.NoError(t, err)
	revision.Config.BackendProfileID = "isolated"
	digest, err := revision.Config.Digest()
	require.NoError(t, err)
	revision.ConfigDigest = digest

	runtime, err := platformagent.NewRuntime(
		context.Background(), revision, runtimes.router, security.DenyCapabilities())
	require.NoError(t, err)
	require.NotSame(t, stack.sessions, runtime.SessionService)
	require.NoError(t, runtime.Close())

	require.NoError(t, runtimes.close())
	require.Zero(t, sessions.closeCount(), "the Router closed a store it only borrowed")
}

// The Factory asks the process's own entitlement table, and it asks it before
// it reads a single environment variable.
//
// Both halves matter. A Factory wired to a second, more permissive authorizer
// would resolve credentials the Admin API never agreed this tenant could name;
// and one that looked the variable up first would answer "not set" for a
// variable a tenant may not name, which is a probe for the process environment
// built out of nothing but refusals.
func TestOpenRuntimeStackFactoryRefusesAnUnentitledProfileBeforeReadingTheEnvironment(t *testing.T) {
	stack, _ := seededStack(t)
	t.Cleanup(func() { require.NoError(t, stack.close()) })

	// Durable pins, so the profile below is refused for the reason under test
	// rather than by the process constraints, which are checked first.
	runtimes, err := openRuntimeStack(
		durableTestConfig(), stack, noEnv(t), security.DenyCapabilities())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtimes.close()) })

	_, err = stack.profiles.CreateProfile(
		context.Background(),
		demoScope(),
		storagebundle.Profile{
			TenantID: platformconfig.DemoTenantID,
			ID:       "durable",
			Session: storagebundle.SessionSpec{
				Backend:  "postgres",
				Postgres: &storagebundle.PostgresSpec{DSNRef: "env:DEMO_SESSION_DSN"},
			},
		},
		"runtimes-test",
	)
	require.NoError(t, err)

	lease, err := runtimes.router.Resolve(context.Background(), demoScope(), "durable")
	require.ErrorIs(t, err, security.ErrNotEntitled)
	require.Nil(t, lease)
	require.NotContains(t, err.Error(), "DEMO_SESSION_DSN")
}

// Once the tenant is entitled, the credential is read through the function this
// process was configured with — not through os.Getenv.
//
// The distinction is the whole reason getenv is a parameter: a Factory reading
// the real environment inside a process configured from a different one would
// resolve credentials nobody in that configuration granted.
func TestOpenRuntimeStackFactoryReadsTheProcessEnvironment(t *testing.T) {
	stack, _ := seededStack(t)
	t.Cleanup(func() { require.NoError(t, stack.close()) })

	entitlements, err := security.NewEntitlements(security.Grant{
		TenantID:   platformconfig.DemoTenantID,
		SecretRefs: []string{"env:DEMO_SESSION_DSN"},
	})
	require.NoError(t, err)

	env := &recordingEnv{}
	runtimes, err := openRuntimeStack(durableTestConfig(), stack, env.getenv, entitlements)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtimes.close()) })

	_, err = stack.profiles.CreateProfile(
		context.Background(),
		demoScope(),
		storagebundle.Profile{
			TenantID: platformconfig.DemoTenantID,
			ID:       "durable",
			Session: storagebundle.SessionSpec{
				Backend:  "postgres",
				Postgres: &storagebundle.PostgresSpec{DSNRef: "env:DEMO_SESSION_DSN"},
			},
		},
		"runtimes-test",
	)
	require.NoError(t, err)

	// The variable is unset in this environment, so the build stops there —
	// which is what keeps this test off the network while still proving which
	// environment was consulted.
	lease, err := runtimes.router.Resolve(context.Background(), demoScope(), "durable")
	require.ErrorContains(t, err, "environment variable DEMO_SESSION_DSN is not set")
	require.Nil(t, lease)
	require.Equal(t, []string{"DEMO_SESSION_DSN"}, env.names())
}

// A Runtime the resolver cached holds a lease on its Bundle for its whole life,
// so the Router cannot finish closing until every Runtime has been closed.
//
// That makes the order inside runtimeStack.close a liveness property rather
// than a preference: closing the Router first would block here until the
// process was killed. The assertion is a deadline, because a deadlock has no
// other symptom.
func TestRuntimeStackCloseReleasesRuntimesBeforeTheirBundles(t *testing.T) {
	stack, sessions := seededStack(t)
	t.Cleanup(func() { require.NoError(t, stack.close()) })

	runtimes, err := openRuntimeStack(
		inMemoryTestConfig(), stack, noEnv(t), security.DenyCapabilities())
	require.NoError(t, err)

	resolved, err := runtimes.resolver.Resolve(
		context.Background(), demoScope(), platformconfig.DemoAgentAppID, "")
	require.NoError(t, err)
	require.Same(t, stack.sessions, resolved.Runtime.SessionService)
	// The run is over, but the Runtime stays cached — and keeps holding its
	// Bundle lease. This is the state a live process is in between requests.
	resolved.Release()

	closed := make(chan error, 1)
	go func() { closed <- runtimes.close() }()
	select {
	case closeErr := <-closed:
		require.NoError(t, closeErr)
	case <-time.After(10 * time.Second):
		t.Fatal("runtimeStack.close deadlocked: the Router waited for a Runtime " +
			"that had not been closed yet")
	}

	require.Zero(t, sessions.closeCount(), "the process store is the stack's to close")
}

// Closing twice is what a partial startup failure and the shutdown path do
// between them, so it must be safe and must report the same thing.
func TestRuntimeStackCloseIsSafeToRepeatAndOnNil(t *testing.T) {
	stack, _ := seededStack(t)
	t.Cleanup(func() { require.NoError(t, stack.close()) })

	runtimes, err := openRuntimeStack(
		inMemoryTestConfig(), stack, noEnv(t), security.DenyCapabilities())
	require.NoError(t, err)

	require.NoError(t, runtimes.close())
	require.NoError(t, runtimes.close())

	var absent *runtimeStack
	require.NoError(t, absent.close())
}

// What a tenant profile may not do in this process is derived from what this
// process is, rather than configured a second time. A second switch for these
// two would be a second answer to one question.
func TestProcessConstraintsFollowTheStorageConfiguration(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  storageConfig
		want storagebundle.ProcessConstraints
	}{
		{
			name: "inmemory alone",
			cfg:  storageConfig{profile: profileInMemory, coordination: coordinationInMemory},
			want: storagebundle.ProcessConstraints{},
		},
		{
			name: "postgres alone",
			cfg:  storageConfig{profile: profilePostgres, coordination: coordinationInMemory},
			want: storagebundle.ProcessConstraints{DurablePins: true},
		},
		{
			name: "postgres with redis coordination",
			cfg:  storageConfig{profile: profilePostgres, coordination: coordinationRedis},
			want: storagebundle.ProcessConstraints{DurablePins: true, MultiWorker: true},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, processConstraints(tc.cfg))
		})
	}
}

// The derivation is not decoration: it decides what the Factory refuses.
//
// Under the in-memory profile a tenant profile asking for a durable backend is
// refused, because the pin that names its revision would not survive the
// restart the sessions would. That is the same invariant storage.go refuses to
// boot without, reaching the one place that could otherwise reopen it.
func TestProcessConstraintsReachTheFactory(t *testing.T) {
	durable := storagebundle.Profile{
		TenantID: platformconfig.DemoTenantID,
		ID:       "durable",
		Session: storagebundle.SessionSpec{
			Backend:  "postgres",
			Postgres: &storagebundle.PostgresSpec{DSNRef: "env:DEMO_SESSION_DSN"},
		},
	}

	// The environment is a tripwire in both halves: the constraint is what
	// refuses these profiles, and it refuses them before the credential the
	// first one names is looked up.
	factory, err := storagebundle.NewSessionFactory(storagebundle.FactoryOptions{
		Constraints: processConstraints(inMemoryTestConfig()),
		Secrets:     security.DenyCapabilities(),
		Getenv:      noEnv(t),
	})
	require.NoError(t, err)
	_, _, err = factory.Build(context.Background(), durable)
	require.ErrorIs(t, err, storagebundle.ErrPinsNotDurable)

	// A Worker that coordinates with others refuses the opposite arrangement:
	// an in-process store behind a lock its peers cannot see anything through.
	multiWorker, err := storagebundle.NewSessionFactory(storagebundle.FactoryOptions{
		Constraints: processConstraints(storageConfig{
			profile:      profilePostgres,
			coordination: coordinationRedis,
		}),
		Secrets: security.DenyCapabilities(),
		Getenv:  noEnv(t),
	})
	require.NoError(t, err)
	_, _, err = multiWorker.Build(context.Background(), storagebundle.Profile{
		TenantID: platformconfig.DemoTenantID,
		ID:       "shared",
		Session:  storagebundle.SessionSpec{Backend: "inmemory"},
	})
	require.ErrorIs(t, err, storagebundle.ErrNotSharedAcrossWorkers)
}

// inMemoryTestConfig is the configuration a Worker with no external
// dependencies boots on.
func inMemoryTestConfig() storageConfig {
	return storageConfig{profile: profileInMemory, coordination: coordinationInMemory}
}
