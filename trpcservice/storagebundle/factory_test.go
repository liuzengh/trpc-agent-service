package storagebundle

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionbackend"
	"github.com/stretchr/testify/require"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// The connection strings the fake environment answers with. They carry a
// password because the thing most of these tests assert is that it never comes
// back out.
const (
	testDSN      = "postgres://sessions:hunter2@db.internal:5432/sessions?sslmode=disable"
	testRedisURL = "redis://sessions:hunter2@cache.internal:6379/3"
	testPassword = "hunter2"
)

const (
	testDSNVar   = "TENANT_A_SESSION_DSN"
	testURLVar   = "TENANT_A_SESSION_URL"
	testDSNRef   = "env:" + testDSNVar
	testURLRef   = "env:" + testURLVar
	testTenantID = "tenant-a"
)

// errTestNotEntitled stands in for security.ErrNotEntitled. The factory is
// wired with an interface, so the test asserts that whatever the authorizer
// refused with survives the wrapping — not that this package knows the
// security package's sentinel.
var errTestNotEntitled = errors.New("test: not entitled")

// testEnv is a fake process environment that records every read.
//
// The reads are what several tests are actually about: a factory that
// authorized after resolving would let an unentitled tenant learn whether a
// variable is set, and the only way to pin that is to count.
type testEnv struct {
	mu    sync.Mutex
	vars  map[string]string
	reads []string
}

func newTestEnv(vars map[string]string) *testEnv {
	return &testEnv{vars: vars}
}

func (e *testEnv) getenv(name string) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.reads = append(e.reads, name)
	return e.vars[name]
}

func (e *testEnv) readNames() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.reads...)
}

// testSecrets entitles exactly the (tenant, reference) pairs it was built with.
type testSecrets map[string]struct{}

func entitle(pairs ...string) testSecrets {
	granted := make(testSecrets, len(pairs))
	for _, pair := range pairs {
		granted[pair] = struct{}{}
	}
	return granted
}

func (s testSecrets) AuthorizeSecretRef(tenantID string, ref string) error {
	if _, allowed := s[tenantID+"\x00"+ref]; !allowed {
		return errTestNotEntitled
	}
	return nil
}

// fakeUpstream stands in for everything a build touches outside this process:
// the two probes and the upstream session constructor.
//
// It records the order it was called in, because the order is the contract —
// the probe exists so that an unreachable target fails where it can be
// cancelled, and a constructor that ran first would make it decoration.
type fakeUpstream struct {
	// Knobs, set before the build starts and not written afterwards.
	blockProbe    chan struct{}
	blockSessions chan struct{}
	probeErr      error
	sessionsErr   error
	releaseErr    error

	// building is closed when the upstream constructor is entered, so a test
	// can cancel at a known point rather than after a guessed delay.
	building     chan struct{}
	buildingOnce sync.Once

	mu       sync.Mutex
	steps    []string
	configs  []sessionbackend.Config
	lockKeys []int64
	services []*countingSessionService
	releases int
}

func (u *fakeUpstream) record(step string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.steps = append(u.steps, step)
}

func (u *fakeUpstream) recorded() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.steps...)
}

func (u *fakeUpstream) releaseCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.releases
}

func (u *fakeUpstream) built() []*countingSessionService {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]*countingSessionService(nil), u.services...)
}

func (u *fakeUpstream) release() error {
	u.mu.Lock()
	u.releases++
	u.mu.Unlock()
	u.record("release")
	return u.releaseErr
}

func (u *fakeUpstream) probe(ctx context.Context, step string) (func() error, error) {
	u.record(step)
	if u.blockProbe != nil {
		select {
		case <-u.blockProbe:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if u.probeErr != nil {
		return nil, u.probeErr
	}
	return u.release, nil
}

func (u *fakeUpstream) probePostgres(
	ctx context.Context, dsn string, lockKey int64,
) (func() error, error) {
	u.mu.Lock()
	u.lockKeys = append(u.lockKeys, lockKey)
	u.mu.Unlock()
	return u.probe(ctx, "probe postgres "+dsn)
}

func (u *fakeUpstream) probeRedis(ctx context.Context, url string) (func() error, error) {
	return u.probe(ctx, "probe redis "+url)
}

func (u *fakeUpstream) newSessions(cfg sessionbackend.Config) (session.Service, error) {
	u.mu.Lock()
	u.configs = append(u.configs, cfg)
	u.mu.Unlock()
	u.record("build " + string(cfg.Backend))
	if u.building != nil {
		u.buildingOnce.Do(func() { close(u.building) })
	}
	if u.blockSessions != nil {
		<-u.blockSessions
	}
	if u.sessionsErr != nil {
		return nil, u.sessionsErr
	}
	service := &countingSessionService{}
	u.mu.Lock()
	u.services = append(u.services, service)
	u.mu.Unlock()
	return service, nil
}

// fakeLocks stands in for PostgreSQL's advisory lock space: one holder per key,
// and a second caller waits until the holder gives it back or its own context
// ends. It is the smallest thing that can tell "the lock was held while the
// constructor ran" apart from "the lock was taken and given back".
type fakeLocks struct {
	mu   sync.Mutex
	held map[int64]chan struct{}
}

func newFakeLocks() *fakeLocks {
	return &fakeLocks{held: make(map[int64]chan struct{})}
}

// acquire has the signature of the probePostgres seam.
func (l *fakeLocks) acquire(ctx context.Context, dsn string, key int64) (func() error, error) {
	for {
		l.mu.Lock()
		if _, taken := l.held[key]; !taken {
			freed := make(chan struct{})
			l.held[key] = freed
			l.mu.Unlock()
			var once sync.Once
			return func() error {
				once.Do(func() {
					l.mu.Lock()
					delete(l.held, key)
					l.mu.Unlock()
					close(freed)
				})
				return nil
			}, nil
		}
		waitFor := l.held[key]
		l.mu.Unlock()
		select {
		case <-waitFor:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (l *fakeLocks) free() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.held) == 0
}

// testFactory builds the factory an in-memory test runs under: nothing
// entitled, and an environment with nothing in it. An in-memory profile names
// no credential, so it builds anyway — which is the point of checking the
// entitlement per reference rather than per build.
func testFactory(t *testing.T, constraints ProcessConstraints) Factory {
	t.Helper()
	factory, err := NewSessionFactory(FactoryOptions{
		Constraints: constraints,
		Secrets:     entitle(),
		Getenv:      newTestEnv(nil).getenv,
	})
	require.NoError(t, err)
	return factory
}

// durableFactory is the factory a durable profile builds under: pins that
// survive a restart, both references entitled, and every upstream replaced by
// the seams so the test decides what the network does.
func durableFactory(
	t *testing.T, upstream *fakeUpstream, env *testEnv, timeout time.Duration,
) Factory {
	t.Helper()
	factory, err := NewSessionFactory(FactoryOptions{
		Constraints:   ProcessConstraints{DurablePins: true},
		Secrets:       entitle(testTenantID+"\x00"+testDSNRef, testTenantID+"\x00"+testURLRef),
		Getenv:        env.getenv,
		BuildTimeout:  timeout,
		newSessions:   upstream.newSessions,
		probePostgres: upstream.probePostgres,
		probeRedis:    upstream.probeRedis,
	})
	require.NoError(t, err)
	return factory
}

func durableEnv() *testEnv {
	return newTestEnv(map[string]string{testDSNVar: testDSN, testURLVar: testRedisURL})
}

func TestNewSessionFactoryRequiresItsDependencies(t *testing.T) {
	getenv := newTestEnv(nil).getenv

	factory, err := NewSessionFactory(FactoryOptions{Getenv: getenv})
	require.ErrorContains(t, err, "secret authorizer")
	require.Nil(t, factory)

	factory, err = NewSessionFactory(FactoryOptions{Secrets: entitle()})
	require.ErrorContains(t, err, "environment lookup")
	require.Nil(t, factory)

	factory, err = NewSessionFactory(FactoryOptions{
		Secrets: entitle(), Getenv: getenv, BuildTimeout: -time.Second,
	})
	require.ErrorContains(t, err, "build timeout")
	require.Nil(t, factory)

	factory, err = NewSessionFactory(FactoryOptions{Secrets: entitle(), Getenv: getenv})
	require.NoError(t, err)
	require.NotNil(t, factory)
}

// The default timeout is not a detail: it is what stops an upstream
// constructor that cannot be cancelled from wedging Router.Close.
func TestNewSessionFactoryDefaultsTheBuildTimeout(t *testing.T) {
	factory, err := NewSessionFactory(FactoryOptions{
		Secrets: entitle(), Getenv: newTestEnv(nil).getenv,
	})
	require.NoError(t, err)
	require.Equal(t, DefaultBuildTimeout, factory.(sessionFactory).options.BuildTimeout)
	require.Equal(t, 15*time.Second, DefaultBuildTimeout)
}

// The simplest backend, and the contract every build is under: a Bundle and its
// close, both non-nil, and a store that actually works.
func TestSessionFactoryBuildsInMemoryWithItsClose(t *testing.T) {
	factory := testFactory(t, ProcessConstraints{})

	bundle, closeBundle, err := factory.Build(
		context.Background(), inMemoryProfile(testTenantID, "p1"))
	require.NoError(t, err)
	require.NotNil(t, closeBundle, "a Bundle without its close can never be released")
	require.NoError(t, bundle.Validate())

	require.NoError(t, closeBundle())
}

// Two builds of the same profile are two independent stores. The Router is what
// makes one profile one Bundle; a Factory that cached would be a second, hidden
// answer to the same question, and closing one would take the other down.
func TestSessionFactoryBuildsIndependentStores(t *testing.T) {
	factory := testFactory(t, ProcessConstraints{})
	profile := inMemoryProfile(testTenantID, "p1")

	first, closeFirst, err := factory.Build(context.Background(), profile)
	require.NoError(t, err)
	second, closeSecond, err := factory.Build(context.Background(), profile)
	require.NoError(t, err)
	require.NotSame(t, first.Session, second.Session)

	require.NoError(t, closeFirst())
	require.NoError(t, closeSecond())
}

// The two durable backends, end to end through the seams: the credential is
// resolved from the environment the process was configured with, the target is
// probed before the constructor is allowed near it, the constructor receives
// the connection string and the namespacing the profile asked for, and whatever
// the probe held is released before Build returns.
func TestSessionFactoryBuildsDurableBackendsThroughTheProbe(t *testing.T) {
	t.Run("postgres", func(t *testing.T) {
		upstream := &fakeUpstream{}
		env := durableEnv()
		profile := postgresProfile(testTenantID, "p1")
		profile.Session.Postgres.Schema = "sessions"
		profile.Session.Postgres.TablePrefix = "tenant_a"

		bundle, closeBundle, err := durableFactory(t, upstream, env, 0).
			Build(context.Background(), profile)
		require.NoError(t, err)
		require.NoError(t, bundle.Validate())

		require.Equal(t, []string{
			"probe postgres " + testDSN,
			"build postgres",
			"release",
		}, upstream.recorded())
		require.Equal(t, []string{testDSNVar}, env.readNames())
		require.Equal(t, sessionbackend.Config{
			Backend: sessionbackend.BackendPostgres,
			Postgres: sessionbackend.PostgresConfig{
				DSN:         testDSN,
				Schema:      "sessions",
				TablePrefix: "tenant_a",
			},
		}, upstream.configs[0])

		// The close that came back is the store's, and it is the only one.
		require.Equal(t, 0, upstream.built()[0].closeCount())
		require.NoError(t, closeBundle())
		require.Equal(t, 1, upstream.built()[0].closeCount())
	})

	t.Run("redis", func(t *testing.T) {
		upstream := &fakeUpstream{}
		env := durableEnv()
		profile := redisProfile(testTenantID, "p2")
		profile.Session.Redis.KeyPrefix = "tenant-a"

		bundle, closeBundle, err := durableFactory(t, upstream, env, 0).
			Build(context.Background(), profile)
		require.NoError(t, err)
		require.NoError(t, bundle.Validate())

		require.Equal(t, []string{
			"probe redis " + testRedisURL,
			"build redis",
			"release",
		}, upstream.recorded())
		require.Equal(t, []string{testURLVar}, env.readNames())
		require.Equal(t, sessionbackend.Config{
			Backend: sessionbackend.BackendRedis,
			Redis: sessionbackend.RedisConfig{
				URL:       testRedisURL,
				KeyPrefix: "tenant-a",
			},
		}, upstream.configs[0])

		require.NoError(t, closeBundle())
		require.Equal(t, 1, upstream.built()[0].closeCount())
	})

	t.Run("inmemory reaches no probe and no environment", func(t *testing.T) {
		upstream := &fakeUpstream{}
		env := durableEnv()

		_, closeBundle, err := durableFactory(t, upstream, env, 0).
			Build(context.Background(), inMemoryProfile(testTenantID, "p3"))
		require.NoError(t, err)
		require.Equal(t, []string{"build inmemory"}, upstream.recorded())
		require.Empty(t, env.readNames())
		require.NoError(t, closeBundle())
	})
}

// The entitlement is checked before the environment is read, and this is the
// test that says so. A factory that resolved first and authorized second would
// answer "variable not set" for a reference the tenant was never granted, which
// is a probe for the process environment built out of refusals.
func TestSessionFactoryAuthorizesBeforeReadingTheEnvironment(t *testing.T) {
	for _, profile := range []Profile{
		postgresProfile(testTenantID, "p1"),
		redisProfile(testTenantID, "p2"),
	} {
		upstream := &fakeUpstream{}
		env := durableEnv()
		factory, err := NewSessionFactory(FactoryOptions{
			Constraints:   ProcessConstraints{DurablePins: true},
			Secrets:       entitle(), // nothing granted to anybody
			Getenv:        env.getenv,
			newSessions:   upstream.newSessions,
			probePostgres: upstream.probePostgres,
			probeRedis:    upstream.probeRedis,
		})
		require.NoError(t, err)

		bundle, closeBundle, err := factory.Build(context.Background(), profile)
		require.ErrorIs(t, err, errTestNotEntitled)
		require.Nil(t, closeBundle)
		require.Equal(t, Bundle{}, bundle)

		require.Empty(t, env.readNames(), "an unentitled reference must not be resolved")
		require.Empty(t, upstream.recorded(), "and nothing may be contacted on its behalf")
		// The refusal names the profile and its tenant, and nothing else.
		require.ErrorContains(t, err, profile.ID)
		require.ErrorContains(t, err, testTenantID)
	}

	// Entitling the other tenant is not entitling this one.
	upstream := &fakeUpstream{}
	env := durableEnv()
	factory, err := NewSessionFactory(FactoryOptions{
		Constraints:   ProcessConstraints{DurablePins: true},
		Secrets:       entitle("tenant-b\x00" + testDSNRef),
		Getenv:        env.getenv,
		newSessions:   upstream.newSessions,
		probePostgres: upstream.probePostgres,
	})
	require.NoError(t, err)
	_, _, err = factory.Build(context.Background(), postgresProfile(testTenantID, "p1"))
	require.ErrorIs(t, err, errTestNotEntitled)
	require.Empty(t, env.readNames())
}

// A durable session store in a process whose session directory is not durable
// keeps the conversation and loses the revision it was pinned to. It is refused
// before the entitlement is consulted and before anything is resolved, because
// the arrangement is wrong whatever the tenant is allowed to name.
func TestSessionFactoryRefusesDurableBackendsWithoutDurablePins(t *testing.T) {
	for _, profile := range []Profile{
		postgresProfile(testTenantID, "p1"),
		redisProfile(testTenantID, "p2"),
	} {
		upstream := &fakeUpstream{}
		env := durableEnv()
		factory, err := NewSessionFactory(FactoryOptions{
			Constraints:   ProcessConstraints{DurablePins: false},
			Secrets:       entitle(testTenantID+"\x00"+testDSNRef, testTenantID+"\x00"+testURLRef),
			Getenv:        env.getenv,
			newSessions:   upstream.newSessions,
			probePostgres: upstream.probePostgres,
			probeRedis:    upstream.probeRedis,
		})
		require.NoError(t, err)

		bundle, closeBundle, err := factory.Build(context.Background(), profile)
		require.ErrorIs(t, err, ErrPinsNotDurable)
		require.NotErrorIs(t, err, ErrUnsupportedBackend)
		require.Nil(t, closeBundle)
		require.Equal(t, Bundle{}, bundle)
		require.Empty(t, env.readNames())
		require.Empty(t, upstream.recorded())
		// The refusal names the profile and its tenant, so an operator reading
		// one line of log knows which revision stopped working.
		require.ErrorContains(t, err, profile.ID)
		require.ErrorContains(t, err, testTenantID)
	}
}

// ErrUnsupportedBackend now means one thing: a backend this factory has never
// heard of. Build cannot reach it — Validate rejects an unknown backend first —
// so the question is asked of the method that answers it.
func TestSessionFactoryReportsAnUnknownBackendAsUnsupported(t *testing.T) {
	factory := testFactory(t, ProcessConstraints{DurablePins: true}).(sessionFactory)

	err := factory.allows("cassandra")
	require.ErrorIs(t, err, ErrUnsupportedBackend)
	require.ErrorContains(t, err, "cassandra")

	require.NoError(t, factory.allows(sessionbackend.BackendPostgres))
	require.NoError(t, factory.allows(sessionbackend.BackendRedis))
	require.NoError(t, factory.allows(sessionbackend.BackendInMemory))
}

// An in-process store under a shared run lease is unshared state behind a lock
// its peers cannot see anything through. The process-level configuration
// refuses that combination at startup; a per-tenant profile must not be able to
// reintroduce it.
func TestSessionFactoryRefusesInMemoryAcrossWorkers(t *testing.T) {
	factory := testFactory(t, ProcessConstraints{DurablePins: true, MultiWorker: true})

	bundle, closeBundle, err := factory.Build(
		context.Background(), inMemoryProfile(testTenantID, "p1"))
	require.ErrorIs(t, err, ErrNotSharedAcrossWorkers)
	require.Nil(t, closeBundle)
	require.Equal(t, Bundle{}, bundle)

	// And with a single worker the same profile builds, so the refusal above is
	// about the arrangement and not about the profile.
	single := testFactory(t, ProcessConstraints{DurablePins: true})
	bundle, closeBundle, err = single.Build(
		context.Background(), inMemoryProfile(testTenantID, "p1"))
	require.NoError(t, err)
	require.NoError(t, bundle.Validate())
	require.NoError(t, closeBundle())
}

// Validation is repeated here rather than assumed from the Router: this is the
// function that builds, and the upstream namespacing options panic rather than
// return on input they dislike.
func TestSessionFactoryRevalidatesTheProfile(t *testing.T) {
	factory := testFactory(t, ProcessConstraints{})

	for _, tc := range []struct {
		name    string
		profile Profile
	}{
		{"no tenant", Profile{ID: "p1", Session: SessionSpec{Backend: sessionbackend.BackendInMemory}}},
		{"no id", Profile{TenantID: testTenantID, Session: SessionSpec{Backend: sessionbackend.BackendInMemory}}},
		{"no backend", Profile{TenantID: testTenantID, ID: "p1"}},
		{
			"unknown backend",
			Profile{TenantID: testTenantID, ID: "p1", Session: SessionSpec{Backend: "cassandra"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bundle, closeBundle, err := factory.Build(context.Background(), tc.profile)
			require.ErrorIs(t, err, ErrInvalidProfile)
			require.Nil(t, closeBundle)
			require.Equal(t, Bundle{}, bundle)
		})
	}
}

// The context is checked before anything is constructed. A Router that is
// already shutting down must not open a store nobody will ever reach — and one
// nobody will ever close, since the Router has passed the point where it waits
// for builds.
func TestSessionFactoryRefusesADoneContextBeforeBuilding(t *testing.T) {
	factory := testFactory(t, ProcessConstraints{})
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	bundle, closeBundle, err := factory.Build(cancelled, inMemoryProfile(testTenantID, "p1"))
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, closeBundle)
	require.Equal(t, Bundle{}, bundle)

	//nolint:staticcheck // a nil context is exactly what is under test here.
	bundle, closeBundle, err = factory.Build(nil, inMemoryProfile(testTenantID, "p1"))
	require.Error(t, err)
	require.Nil(t, closeBundle)
	require.Equal(t, Bundle{}, bundle)
}

// A variable that is not set is a build failure that names the variable and
// nothing else. The name is what an operator needs; the value is what nobody
// needs, and in the missing case there is nothing to disclose anyway — which is
// exactly why the empty check must not fall through to a driver parse error
// built from the empty string.
func TestSessionFactoryReportsAMissingVariableByName(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile Profile
		vars    map[string]string
		wantVar string
	}{
		{"unset dsn", postgresProfile(testTenantID, "p1"), nil, testDSNVar},
		{"unset url", redisProfile(testTenantID, "p2"), nil, testURLVar},
		{
			"blank dsn", postgresProfile(testTenantID, "p1"),
			map[string]string{testDSNVar: "   \t"}, testDSNVar,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := &fakeUpstream{}
			env := newTestEnv(tc.vars)

			bundle, closeBundle, err := durableFactory(t, upstream, env, 0).
				Build(context.Background(), tc.profile)
			require.ErrorContains(t, err, tc.wantVar)
			require.ErrorContains(t, err, "is not set")
			require.Nil(t, closeBundle)
			require.Equal(t, Bundle{}, bundle)
			require.Empty(t, upstream.recorded(), "nothing may be contacted without a credential")
		})
	}
}

// A credential that is present but is not a connection string is refused in
// fixed wording, and the driver's own error — which quotes the string it failed
// to parse — is discarded rather than wrapped.
func TestSessionFactoryRefusesAnUnusableCredentialWithoutEchoingIt(t *testing.T) {
	// A value that is not a DSN and not a URL, and that contains something that
	// looks like a credential, because a mistyped DSN is a real one.
	const pasted = "postgres//sessions:hunter2@db.internal:5432/sessions"

	for _, tc := range []struct {
		name    string
		profile Profile
		vars    map[string]string
		wantVar string
	}{
		{
			"not a dsn", postgresProfile(testTenantID, "p1"),
			map[string]string{testDSNVar: pasted}, testDSNVar,
		},
		{
			"not a redis url", redisProfile(testTenantID, "p2"),
			map[string]string{testURLVar: pasted}, testURLVar,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := &fakeUpstream{}
			env := newTestEnv(tc.vars)

			bundle, closeBundle, err := durableFactory(t, upstream, env, 0).
				Build(context.Background(), tc.profile)
			require.Error(t, err)
			require.Nil(t, closeBundle)
			require.Equal(t, Bundle{}, bundle)

			require.ErrorContains(t, err, tc.wantVar)
			require.ErrorContains(t, err, "does not hold")
			require.NotContains(t, err.Error(), pasted)
			require.NotContains(t, err.Error(), testPassword)
			require.Empty(t, upstream.recorded())
		})
	}
}

// No error from a build may carry the resolved connection string, the password
// inside it, or the key prefix a caller could have pasted one into. This is the
// end of the road for a value that started as a reference: everything past the
// resolve step has the real thing in hand.
func TestSessionFactoryErrorsCarryNoCredential(t *testing.T) {
	unreachable := errors.New("dial tcp 10.0.0.1:5432: connect: connection refused")

	for _, tc := range []struct {
		name     string
		profile  Profile
		upstream *fakeUpstream
	}{
		{"probe fails", postgresProfile(testTenantID, "p1"), &fakeUpstream{probeErr: unreachable}},
		{"build fails", postgresProfile(testTenantID, "p1"), &fakeUpstream{sessionsErr: unreachable}},
		{"redis probe fails", redisProfile(testTenantID, "p2"), &fakeUpstream{probeErr: unreachable}},
		{"redis build fails", redisProfile(testTenantID, "p2"), &fakeUpstream{sessionsErr: unreachable}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env := durableEnv()
			bundle, closeBundle, err := durableFactory(t, tc.upstream, env, 0).
				Build(context.Background(), tc.profile)
			require.Error(t, err)
			require.Nil(t, closeBundle)
			require.Equal(t, Bundle{}, bundle)

			require.NotContains(t, err.Error(), testPassword)
			require.NotContains(t, err.Error(), testDSN)
			require.NotContains(t, err.Error(), testRedisURL)
			// It still says which profile of which tenant failed.
			require.ErrorContains(t, err, tc.profile.ID)
			require.ErrorContains(t, err, testTenantID)
			// And it is still the error the upstream returned. Nothing was
			// removed from this one, and redaction that flattened every error
			// on its way out would take the sentinel a caller matches on with
			// it — a connection refused would stop being distinguishable from
			// a deadline.
			require.ErrorIs(t, err, unreachable)
		})
	}
}

// The password is not the only thing a connection string gives away, and it is
// the only thing Scrub can find: a target that needs no password has no secret
// to extract, so an upstream that echoes back what it was handed would print
// the whole string — host, user, database and all — into an error the Admin API
// returns to whoever asked. The resolved value is removed as a value.
func TestSessionFactoryErrorsCarryNoConnectionString(t *testing.T) {
	const (
		plainDSN   = "postgres://sessions@db.internal:5432/sessions?sslmode=disable"
		plainRedis = "redis://cache.internal:6379/3"
	)
	// Shaped like the errors the drivers really produce: they quote the string
	// they were given, verbatim.
	echoed := func(value string) error {
		return fmt.Errorf("failed to connect to `%s`: server closed the connection", value)
	}

	for _, tc := range []struct {
		name     string
		profile  Profile
		envVar   string
		value    string
		fragment string
		probe    bool
	}{
		{
			"a passwordless dsn echoed by the constructor",
			postgresProfile(testTenantID, "p1"), testDSNVar, plainDSN, "db.internal", false,
		},
		{
			"a passwordless dsn echoed by the probe",
			postgresProfile(testTenantID, "p1"), testDSNVar, plainDSN, "db.internal", true,
		},
		{
			"a passwordless redis url echoed by the constructor",
			redisProfile(testTenantID, "p2"), testURLVar, plainRedis, "cache.internal", false,
		},
		{
			"a passwordless redis url echoed by the probe",
			redisProfile(testTenantID, "p2"), testURLVar, plainRedis, "cache.internal", true,
		},
		{
			// Scrub already removed the password here. Everything around it is
			// what this pass adds.
			"a dsn that does have a password, echoed whole",
			postgresProfile(testTenantID, "p1"), testDSNVar, testDSN, "db.internal", false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			carrier := echoed(tc.value)
			upstream := &fakeUpstream{}
			if tc.probe {
				upstream.probeErr = carrier
			} else {
				upstream.sessionsErr = carrier
			}
			env := newTestEnv(map[string]string{tc.envVar: tc.value})

			_, closeBundle, err := durableFactory(t, upstream, env, 0).
				Build(context.Background(), tc.profile)
			require.Error(t, err)
			require.Nil(t, closeBundle)

			require.NotContains(t, err.Error(), tc.value)
			require.NotContains(t, err.Error(), tc.fragment)
			require.NotContains(t, err.Error(), testPassword)
			require.Contains(t, err.Error(), redactedCredential,
				"the operator is told something was removed")

			// The chain is cut where the value was: an error that still wrapped
			// the original would hand it straight back to anyone who unwrapped
			// it, which is the whole point of removing it from the message.
			require.NotErrorIs(t, err, carrier)

			// It still says which profile of which tenant failed.
			require.ErrorContains(t, err, tc.profile.ID)
			require.ErrorContains(t, err, testTenantID)
		})
	}
}

// Every failure branch after the probe releases what the probe held. For
// PostgreSQL that is the advisory lock: a build that failed and kept it would
// block every other process's first build against that database.
func TestSessionFactoryReleasesTheProbeOnEveryBranch(t *testing.T) {
	t.Run("build fails", func(t *testing.T) {
		upstream := &fakeUpstream{sessionsErr: errors.New("upstream refused")}
		_, closeBundle, err := durableFactory(t, upstream, durableEnv(), 0).
			Build(context.Background(), postgresProfile(testTenantID, "p1"))
		require.Error(t, err)
		require.Nil(t, closeBundle)
		require.Equal(t, 1, upstream.releaseCount())
		require.Equal(t, []string{
			"probe postgres " + testDSN, "build postgres", "release",
		}, upstream.recorded())
	})

	t.Run("build succeeds", func(t *testing.T) {
		upstream := &fakeUpstream{}
		_, closeBundle, err := durableFactory(t, upstream, durableEnv(), 0).
			Build(context.Background(), postgresProfile(testTenantID, "p1"))
		require.NoError(t, err)
		require.Equal(t, 1, upstream.releaseCount(), "the lock is given back on success too")
		require.NoError(t, closeBundle())
	})

	t.Run("release fails after a successful build", func(t *testing.T) {
		// The lock may still be held, so the build is reported as failed — and
		// the store nobody was handed a close for is closed here.
		releaseErr := errors.New("connection reset while releasing")
		upstream := &fakeUpstream{releaseErr: releaseErr}
		bundle, closeBundle, err := durableFactory(t, upstream, durableEnv(), 0).
			Build(context.Background(), postgresProfile(testTenantID, "p1"))
		require.ErrorIs(t, err, releaseErr)
		require.Nil(t, closeBundle)
		require.Equal(t, Bundle{}, bundle)
		require.Equal(t, 1, upstream.built()[0].closeCount())
	})

	t.Run("probe fails", func(t *testing.T) {
		upstream := &fakeUpstream{probeErr: errors.New("connection refused")}
		_, closeBundle, err := durableFactory(t, upstream, durableEnv(), 0).
			Build(context.Background(), postgresProfile(testTenantID, "p1"))
		require.Error(t, err)
		require.Nil(t, closeBundle)
		require.Zero(t, upstream.releaseCount(), "a probe that failed holds nothing")
		require.Equal(t, []string{"probe postgres " + testDSN}, upstream.recorded())
	})
}

// The upstream constructor takes no context and the PostgreSQL one connects
// while it runs, so a target that accepts a connection and then goes quiet
// would block forever. Build stops waiting; the store that arrives afterwards
// has no owner and closes itself.
func TestSessionFactoryBoundsAConstructorThatNeverReturns(t *testing.T) {
	upstream := &fakeUpstream{blockSessions: make(chan struct{})}
	factory := durableFactory(t, upstream, durableEnv(), 50*time.Millisecond)

	started := time.Now()
	bundle, closeBundle, err := factory.Build(
		context.Background(), postgresProfile(testTenantID, "p1"))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, closeBundle)
	require.Equal(t, Bundle{}, bundle)
	require.Less(t, time.Since(started), 5*time.Second,
		"the build timeout, not the test timeout, is what ended this")
	// The lock is still held. The constructor is still running, and it is what
	// creates the tables the lock protects: giving it back here would let
	// another Worker start creating the same tables underneath it.
	require.Zero(t, upstream.releaseCount(),
		"the lock belongs to the constructor, which has not finished")

	// Now let the constructor finish. Its store was never returned to anyone, so
	// the goroutine that produced it closes it — and gives the lock back, once.
	close(upstream.blockSessions)
	require.Eventually(t, func() bool {
		built := upstream.built()
		return len(built) == 1 && built[0].closeCount() == 1 && upstream.releaseCount() == 1
	}, 5*time.Second, 5*time.Millisecond,
		"a store that arrived late must close itself and release the lock, exactly once")
	require.Equal(t, []string{
		"probe postgres " + testDSN, "build postgres", "release",
	}, upstream.recorded())
}

// The lock is held for as long as the constructor it protects is running, even
// when nobody is waiting for that constructor any more. This is the property
// the advisory lock exists for: upstream creates its tables with CREATE TABLE
// IF NOT EXISTS, which races against itself, and a build that timed out has not
// stopped creating them.
func TestSessionFactoryHoldsTheBuildLockUntilTheConstructorReturns(t *testing.T) {
	locks := newFakeLocks()
	upstream := &fakeUpstream{blockSessions: make(chan struct{})}
	profile := postgresProfile(testTenantID, "p1")

	// Two Workers, each with its own factory, against one target — so they
	// agree on the lock key and on nothing else.
	worker := func(timeout time.Duration) Factory {
		factory, err := NewSessionFactory(FactoryOptions{
			Constraints:   ProcessConstraints{DurablePins: true},
			Secrets:       entitle(testTenantID + "\x00" + testDSNRef),
			Getenv:        durableEnv().getenv,
			BuildTimeout:  timeout,
			newSessions:   upstream.newSessions,
			probePostgres: locks.acquire,
		})
		require.NoError(t, err)
		return factory
	}

	_, _, err := worker(50*time.Millisecond).Build(context.Background(), profile)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// The second Worker cannot get past the probe: the first one's constructor
	// still holds the lock, so this is a wait, not a race.
	_, _, err = worker(50*time.Millisecond).Build(context.Background(), profile)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorContains(t, err, "unreachable")
	require.Equal(t, []string{"build postgres"}, upstream.recorded(),
		"the second build never reached the constructor")

	// Once the first constructor is done, the lock is free and the second
	// Worker builds.
	close(upstream.blockSessions)
	require.Eventually(t, locks.free, 5*time.Second, 5*time.Millisecond,
		"the constructor gives the lock back when it returns")

	bundle, closeBundle, err := worker(time.Minute).Build(context.Background(), profile)
	require.NoError(t, err)
	require.NotNil(t, bundle.Session)
	require.NoError(t, closeBundle())
}

// The same abandonment for the probe, which is where a build first touches the
// network. A probe that never returns must not hold the build, and what it was
// holding must be released when it finally does.
func TestSessionFactoryBoundsAProbeThatNeverReturns(t *testing.T) {
	upstream := &fakeUpstream{blockProbe: make(chan struct{})}
	// The probe here ignores its context on purpose: a real one respects it,
	// and this is the case where it does not.
	factory, err := NewSessionFactory(FactoryOptions{
		Constraints:  ProcessConstraints{DurablePins: true},
		Secrets:      entitle(testTenantID + "\x00" + testDSNRef),
		Getenv:       durableEnv().getenv,
		BuildTimeout: 50 * time.Millisecond,
		newSessions:  upstream.newSessions,
		probePostgres: func(ctx context.Context, dsn string, lockKey int64) (func() error, error) {
			<-upstream.blockProbe
			return upstream.release, nil
		},
	})
	require.NoError(t, err)

	bundle, closeBundle, err := factory.Build(
		context.Background(), postgresProfile(testTenantID, "p1"))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, closeBundle)
	require.Equal(t, Bundle{}, bundle)
	require.Empty(t, upstream.recorded(), "the constructor is never reached")

	close(upstream.blockProbe)
	require.Eventually(t, func() bool { return upstream.releaseCount() == 1 },
		5*time.Second, 5*time.Millisecond,
		"a probe that arrived late must release what it took")
}

// Cancelling the caller's context ends the build with that cancellation rather
// than with a deadline, and releases everything the same way.
func TestSessionFactoryStopsWhenItsContextIsCancelled(t *testing.T) {
	upstream := &fakeUpstream{
		blockSessions: make(chan struct{}),
		building:      make(chan struct{}),
	}
	factory := durableFactory(t, upstream, durableEnv(), time.Minute)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-upstream.building
		cancel()
	}()

	bundle, closeBundle, err := factory.Build(ctx, postgresProfile(testTenantID, "p1"))
	require.ErrorIs(t, err, context.Canceled)
	require.NotErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, closeBundle)
	require.Equal(t, Bundle{}, bundle)
	require.Zero(t, upstream.releaseCount(), "the constructor still holds the lock")

	close(upstream.blockSessions)
	require.Eventually(t, func() bool {
		built := upstream.built()
		return len(built) == 1 && built[0].closeCount() == 1 && upstream.releaseCount() == 1
	}, 5*time.Second, 5*time.Millisecond)
}

// Abandoning must not cost a goroutine. The one that runs the work is the one
// that cleans up after it, so a work that never returns leaves exactly one
// behind — where a separate receiver would leave two, every time a build timed
// out, for the life of the process.
func TestAwaitOrAbandonLeavesOnlyTheWorkBehind(t *testing.T) {
	settleGoroutines(t)
	before := runtime.NumGoroutine()

	block := make(chan struct{})
	discarded := make(chan int, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	value, err := awaitOrAbandon(ctx, func() (int, error) {
		<-block
		return 7, nil
	}, func(late int) { discarded <- late })
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, value)
	require.LessOrEqual(t, runtime.NumGoroutine(), before+1,
		"only the work itself may still be running")

	// And what it produces still reaches discard, from that same goroutine.
	close(block)
	require.Equal(t, 7, <-discarded)
	settleGoroutines(t)
	require.LessOrEqual(t, runtime.NumGoroutine(), before)
}

// One outcome has exactly one owner: it is either returned to the caller or
// handed to discard, never both and never neither. The window that decides
// which is the work finishing as the context ends, which is too narrow to stage
// — so it is asserted by repetition, under the race detector.
func TestAwaitOrAbandonGivesOneOutcomeToOneOwner(t *testing.T) {
	for range 200 {
		ctx, cancel := context.WithCancel(context.Background())
		racing := make(chan struct{})
		discarded := make(chan int, 1)
		go func() {
			<-racing
			cancel()
		}()

		value, err := awaitOrAbandon(ctx, func() (int, error) {
			close(racing)
			return 7, nil
		}, func(late int) { discarded <- late })

		if err != nil {
			require.ErrorIs(t, err, context.Canceled)
			require.Zero(t, value)
			require.Equal(t, 7, <-discarded, "an abandoned outcome is cleaned up")
			cancel()
			continue
		}
		require.Equal(t, 7, value)
		// The work had already handed its result over before it returned, so
		// there is nothing left that could discard it too.
		select {
		case <-discarded:
			t.Fatal("a value that was returned must not also be discarded")
		default:
		}
		cancel()
	}
}

// settleGoroutines waits until the count stops moving, so that a measurement
// taken after it is this test's own rather than the tail of an earlier one —
// several tests here deliberately leave a constructor parked and let it finish
// on its own time.
func settleGoroutines(t *testing.T) {
	t.Helper()
	var (
		last   int
		stable int
	)
	for range 400 {
		switch current := runtime.NumGoroutine(); current {
		case last:
			if stable++; stable == 3 {
				return
			}
		default:
			last, stable = current, 0
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Log("goroutine count never settled; the measurement below may be noisy")
}

// The lock key is derived offline, so what it is keyed by can be pinned without
// a database.
//
// It is keyed by what decides which tables get created — target, schema, table
// prefix — and by nothing else. Keying it by tenant or profile would let two
// profiles pointing at one database race each other while each held a lock
// nobody else wanted, which is the race the lock exists to prevent.
func TestAdvisoryLockKeyIsKeyedByTheTargetNotByTheProfile(t *testing.T) {
	keyFor := func(t *testing.T, profile Profile, dsn string) int64 {
		t.Helper()
		upstream := &fakeUpstream{}
		factory, err := NewSessionFactory(FactoryOptions{
			Constraints:   ProcessConstraints{DurablePins: true},
			Secrets:       entitle(profile.TenantID + "\x00" + testDSNRef),
			Getenv:        newTestEnv(map[string]string{testDSNVar: dsn}).getenv,
			newSessions:   upstream.newSessions,
			probePostgres: upstream.probePostgres,
		})
		require.NoError(t, err)

		_, closeBundle, err := factory.Build(context.Background(), profile)
		require.NoError(t, err)
		require.NoError(t, closeBundle())
		require.Len(t, upstream.lockKeys, 1)
		return upstream.lockKeys[0]
	}

	base := postgresProfile(testTenantID, "p1")
	base.Session.Postgres.Schema = "sessions"
	base.Session.Postgres.TablePrefix = "shared"

	baseline := keyFor(t, base, testDSN)

	t.Run("a different profile of a different tenant on the same target", func(t *testing.T) {
		other := postgresProfile("tenant-b", "p9")
		other.Session.Postgres.Schema = "sessions"
		other.Session.Postgres.TablePrefix = "shared"
		require.Equal(t, baseline, keyFor(t, other, testDSN),
			"two profiles that create the same tables must serialize against each other")
	})

	t.Run("a different user and password on the same target", func(t *testing.T) {
		require.Equal(t, baseline, keyFor(t, base,
			"postgres://someone:else@db.internal:5432/sessions?sslmode=disable"),
			"the same tables are created whoever connects")
	})

	for _, tc := range []struct {
		name string
		with func(Profile) Profile
		dsn  string
	}{
		{
			name: "another database",
			with: func(p Profile) Profile { return p },
			dsn:  "postgres://sessions:hunter2@db.internal:5432/other?sslmode=disable",
		},
		{
			name: "another host",
			with: func(p Profile) Profile { return p },
			dsn:  "postgres://sessions:hunter2@other.internal:5432/sessions?sslmode=disable",
		},
		{
			name: "another port",
			with: func(p Profile) Profile { return p },
			dsn:  "postgres://sessions:hunter2@db.internal:5433/sessions?sslmode=disable",
		},
		{
			name: "another schema",
			with: func(p Profile) Profile {
				p = p.clone()
				p.Session.Postgres.Schema = "other"
				return p
			},
			dsn: testDSN,
		},
		{
			name: "another table prefix",
			with: func(p Profile) Profile {
				p = p.clone()
				p.Session.Postgres.TablePrefix = "other"
				return p
			},
			dsn: testDSN,
		},
	} {
		t.Run(tc.name+" is a different key", func(t *testing.T) {
			require.NotEqual(t, baseline, keyFor(t, tc.with(base), tc.dsn))
		})
	}

	// The same inputs give the same key in another process, which is the only
	// property that makes it a lock at all.
	require.Equal(t, baseline, keyFor(t, base, testDSN))
}

// The advisory lock space is one flat 64-bit namespace per database, shared
// with everything else that connects — including this project's control-plane
// migration lock. The namespace prefix is what keeps that from being a
// hand-picked-constant collision.
func TestAdvisoryLockKeyIsNamespaced(t *testing.T) {
	// tenant/postgres takes this key while it migrates the control plane. If a
	// session build ever derived it, a first build and a migration on one
	// database would deadlock against each other for no reason either of them
	// could see.
	const controlPlaneMigrationLockKey = int64(0x7472_7063_7401_0001)

	require.NotEqual(t, controlPlaneMigrationLockKey,
		advisoryLockKey("db.internal\x005432\x00sessions", "", ""))
	require.NotEqual(t, controlPlaneMigrationLockKey,
		advisoryLockKey("db.internal\x005432\x00sessions", "sessions", "shared"))

	// The separators are real: three fields packed into one hash must not be
	// re-orderable into the same key.
	require.NotEqual(t,
		advisoryLockKey("target", "ab", "c"),
		advisoryLockKey("target", "a", "bc"))
	require.NotEqual(t,
		advisoryLockKey("targeta", "b", "c"),
		advisoryLockKey("target", "ab", "c"))
}

// parsePostgresTarget feeds the lock key, so what it keeps and what it drops is
// part of the key's contract.
func TestParsePostgresTargetKeepsTheTargetAndDropsTheCredential(t *testing.T) {
	target, err := parsePostgresTarget(testDSN)
	require.NoError(t, err)
	require.Contains(t, target, "db.internal")
	require.Contains(t, target, "5432")
	require.Contains(t, target, "sessions")
	require.NotContains(t, target, testPassword)

	// libpq keyword form describes the same target and must reach the same key.
	keyword, err := parsePostgresTarget(
		"host=db.internal port=5432 dbname=sessions user=sessions password=hunter2")
	require.NoError(t, err)
	require.Equal(t, target, keyword)

	_, err = parsePostgresTarget("postgres//not-a-dsn")
	require.Error(t, err)
}

func TestCheckRedisURLAcceptsWhatGoRedisAccepts(t *testing.T) {
	require.NoError(t, checkRedisURL(testRedisURL))
	require.NoError(t, checkRedisURL("redis://127.0.0.1:6379/0"))
	require.NoError(t, checkRedisURL("rediss://cache.internal:6380"))
	require.Error(t, checkRedisURL("postgres://db.internal:5432/sessions"))
	require.Error(t, checkRedisURL("cache.internal:6379"))
}
