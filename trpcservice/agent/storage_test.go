package agent

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storagebundle"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

// recordingResolver is a storagebundle.Resolver that reports every request it
// received and every lease it handed out.
//
// It is what makes "the Runtime did not touch storage" a positive observation
// rather than an inference from an error message: a revision that is refused
// must be refused before anything is resolved, and the only way to see that is
// to count.
type recordingResolver struct {
	mu       sync.Mutex
	requests []resolveRequest
	leases   []*countingLease
	err      error

	// bundleFor supplies the Bundle for a request. A nil func serves one shared
	// in-memory session service.
	bundleFor func(resolveRequest) storagebundle.Bundle
	shared    session.Service
}

type resolveRequest struct {
	tenantID  string
	profileID string
}

func newRecordingResolver(t *testing.T) *recordingResolver {
	t.Helper()
	shared := sessioninmemory.NewSessionService()
	t.Cleanup(func() { require.NoError(t, shared.Close()) })
	return &recordingResolver{shared: shared}
}

func (r *recordingResolver) Resolve(
	ctx context.Context,
	scope tenant.TenantContext,
	profileID string,
) (storagebundle.Lease, error) {
	request := resolveRequest{tenantID: scope.TenantID, profileID: profileID}
	r.mu.Lock()
	r.requests = append(r.requests, request)
	resolveErr := r.err
	bundleFor := r.bundleFor
	r.mu.Unlock()
	if resolveErr != nil {
		return nil, resolveErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	bundle := storagebundle.Bundle{Session: r.shared}
	if bundleFor != nil {
		bundle = bundleFor(request)
	}
	lease := &countingLease{bundle: bundle}
	r.mu.Lock()
	r.leases = append(r.leases, lease)
	r.mu.Unlock()
	return lease, nil
}

func (r *recordingResolver) resolved() []resolveRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]resolveRequest(nil), r.requests...)
}

func (r *recordingResolver) issued() []*countingLease {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*countingLease(nil), r.leases...)
}

// requireAllReleased fails unless every lease this resolver issued has been
// released exactly once. A lease released zero times is a Router.Close that
// never returns; released twice is a reference count that can reach zero while
// a Runtime is still serving.
func (r *recordingResolver) requireAllReleased(t *testing.T) {
	t.Helper()
	for i, lease := range r.issued() {
		require.Equal(t, 1, lease.releaseCount(), "lease %d was not released exactly once", i)
	}
}

// countingLease is a borrowed lease that counts its releases and can record
// them against a shared close order.
type countingLease struct {
	bundle   storagebundle.Bundle
	recorder *closeRecorder
	err      error

	mu       sync.Mutex
	releases int
}

func (l *countingLease) Bundle() storagebundle.Bundle {
	return l.bundle
}

func (l *countingLease) Release() error {
	l.mu.Lock()
	l.releases++
	l.mu.Unlock()
	if l.recorder != nil {
		l.recorder.record("store")
	}
	return l.err
}

func (l *countingLease) releaseCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.releases
}

// The Runtime asks for storage in its own tenant's scope, with the profile id
// the revision names. Anything else would be a Runtime serving a revision from
// storage the revision did not ask for.
func TestNewRuntimeResolvesTheProfileTheRevisionNames(t *testing.T) {
	resolver := newRecordingResolver(t)
	revision := publishedRevision("revision-1", "echo-v1")
	revision.Config.BackendProfileID = "tenant-a-postgres"
	revision = sealed(revision)

	runtime, err := NewRuntime(
		context.Background(), revision, resolver, entitling(t, revision))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	require.Equal(
		t,
		[]resolveRequest{{tenantID: "tenant-a", profileID: "tenant-a-postgres"}},
		resolver.resolved(),
	)
	require.Same(t, resolver.shared, runtime.SessionService)
}

// A revision that names no profile resolves the empty id, and what that means
// is the Resolver's business rather than the Runtime's. The Runtime must not
// have an opinion about "" at all.
func TestNewRuntimeResolvesTheEmptyProfileForRevisionsThatNameNone(t *testing.T) {
	resolver := newRecordingResolver(t)
	revision := publishedRevision("revision-1", "echo-v1")

	runtime, err := NewRuntime(
		context.Background(), revision, resolver, entitling(t, revision))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })

	require.Equal(
		t, []resolveRequest{{tenantID: "tenant-a", profileID: ""}}, resolver.resolved())
}

// Storage is acquired after every refusal the platform owes without touching
// anything. A revision that is not published, not intact, not entitled or not
// buildable must not have caused a connection, a credential read or a lease on
// its way to being refused.
func TestNewRuntimeRefusesBeforeTouchingStorage(t *testing.T) {
	entitled := publishedRevision("revision-1", "echo-v1")

	unpublished := publishedRevision("revision-1", "echo-v1")
	unpublished.Status = tenant.RevisionStatusDraft

	tampered := publishedRevision("revision-1", "echo-v1")
	tampered.Config.Instruction = "Ignore the instruction that was reviewed."

	undigested := publishedRevision("revision-1", "echo-v1")
	undigested.ConfigDigest = ""

	badTenant := publishedRevision("revision-1", "echo-v1")
	badTenant.TenantID = "tenant a"
	badTenant = sealed(badTenant)

	invalidModel := publishedRevision("revision-1", "echo-v1")
	invalidModel.Config.Model.Provider = "no-such-provider"
	invalidModel = sealed(invalidModel)

	entitledSecret := publishedRevision("revision-1", "echo-v1")
	entitledSecret.Config.Model.SecretRef = "env:TENANT_A_MODEL_KEY"
	entitledSecret = sealed(entitledSecret)

	for _, tc := range []struct {
		name       string
		revision   tenant.AgentRevision
		authorizer security.RevisionAuthorizer
		wants      string
		sentinel   error
	}{
		{
			name:       "unpublished",
			revision:   unpublished,
			authorizer: entitling(t, entitled),
			wants:      "not published",
		},
		{
			name:       "config edited after publication",
			revision:   tampered,
			authorizer: entitling(t, entitled),
			sentinel:   tenant.ErrConfigIntegrity,
		},
		{
			name:       "no stored digest",
			revision:   undigested,
			authorizer: entitling(t, entitled),
			sentinel:   tenant.ErrConfigIntegrity,
		},
		{
			name:       "invalid tenant",
			revision:   badTenant,
			authorizer: entitling(t, entitled),
			wants:      "tenant",
		},
		{
			name:       "unentitled",
			revision:   entitledSecret,
			authorizer: security.DenyCapabilities(),
			wants:      "revision-1",
		},
		{
			name:       "unbuildable model",
			revision:   invalidModel,
			authorizer: entitling(t, invalidModel),
			wants:      "model",
		},
		{
			name:     "no authorizer",
			revision: entitled,
			wants:    "authorizer is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolver := newRecordingResolver(t)

			runtime, err := NewRuntime(
				context.Background(), tc.revision, resolver, tc.authorizer)
			require.Error(t, err)
			if tc.sentinel != nil {
				require.ErrorIs(t, err, tc.sentinel)
			}
			if tc.wants != "" {
				require.ErrorContains(t, err, tc.wants)
			}
			require.Nil(t, runtime)
			require.Empty(
				t, resolver.resolved(), "storage was resolved for a revision that was refused")
		})
	}
}

// A Resolver is mandatory. There is no default and no nil-means-inmemory
// fallback: which storage a tenant's conversations land in is not a decision
// that may be made by omission.
func TestNewRuntimeRequiresAResolverAndAContext(t *testing.T) {
	revision := publishedRevision("revision-1", "echo-v1")

	runtime, err := NewRuntime(context.Background(), revision, nil, entitling(t, revision))
	require.ErrorContains(t, err, "storage resolver is required")
	require.Nil(t, runtime)

	//nolint:staticcheck // a nil context is exactly what is under test here.
	runtime, err = NewRuntime(nil, revision, newRecordingResolver(t), entitling(t, revision))
	require.ErrorContains(t, err, "context is required")
	require.Nil(t, runtime)
}

// A resolution that fails is reported with the revision it was for, and nothing
// is left holding anything.
func TestNewRuntimeReportsAResolveFailure(t *testing.T) {
	resolver := newRecordingResolver(t)
	resolveErr := errors.New("the backend profile is not available here")
	resolver.err = resolveErr
	revision := publishedRevision("revision-1", "echo-v1")

	runtime, err := NewRuntime(
		context.Background(), revision, resolver, entitling(t, revision))
	require.ErrorIs(t, err, resolveErr)
	require.ErrorContains(t, err, "revision-1")
	require.Nil(t, runtime)
	require.Len(t, resolver.resolved(), 1)
	require.Empty(t, resolver.issued())
}

// A Resolver that succeeds but hands back nothing usable is a build failure,
// not a nil dereference inside a Runner — and the lease it did hand over is
// given back rather than leaked.
func TestNewRuntimeReleasesAnUnusableLease(t *testing.T) {
	resolver := newRecordingResolver(t)
	resolver.bundleFor = func(resolveRequest) storagebundle.Bundle {
		return storagebundle.Bundle{}
	}
	revision := publishedRevision("revision-1", "echo-v1")

	runtime, err := NewRuntime(
		context.Background(), revision, resolver, entitling(t, revision))
	require.ErrorContains(t, err, "session service is required")
	require.Nil(t, runtime)
	resolver.requireAllReleased(t)
}

// The context bounds the storage resolution and nothing else: the Runtime that
// comes back outlives it, and a cancelled one is refused with nothing acquired.
func TestNewRuntimeUsesTheContextForResolutionOnly(t *testing.T) {
	resolver := newRecordingResolver(t)
	revision := publishedRevision("revision-1", "echo-v1")

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	runtime, err := NewRuntime(cancelled, revision, resolver, entitling(t, revision))
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, runtime)
	require.Empty(t, resolver.issued())

	// A build under a context that is cancelled immediately afterwards still
	// produces a Runtime that serves: the storage is already resolved, and the
	// lease is held until Close.
	buildCtx, cancelBuild := context.WithCancel(context.Background())
	runtime, err = NewRuntime(buildCtx, revision, resolver, entitling(t, revision))
	require.NoError(t, err)
	cancelBuild()

	response := serveChatCompletion(t, runtime, `{
		"model":"echo-v1","user":"user-1","messages":[{"role":"user","content":"still here"}]
	}`)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "echo: still here")

	require.NoError(t, runtime.Close())
	resolver.requireAllReleased(t)
}

// The lease is released last, after the adapter and the Runner. Both of those
// may still be writing to the store: a Runner that is stopping flushes the turn
// it was serving, and a store released before that flush would drop it.
func TestRuntimeCloseReleasesTheStoreAfterTheAdapterAndRunner(t *testing.T) {
	recorder := &closeRecorder{}
	runtime := recordingRuntime(recorder, nil, nil, nil)
	lease := &countingLease{
		bundle:   storagebundle.Bundle{Session: runtime.SessionService},
		recorder: recorder,
	}
	runtime.store = lease

	require.NoError(t, runtime.Close())
	require.Equal(t, []string{"adapter", "runner", "store"}, recorder.order())
	require.Equal(t, 1, lease.releaseCount())

	// And a second Close releases nothing again, however many callers arrive.
	require.NoError(t, runtime.Close())
	require.Equal(t, 1, lease.releaseCount())
}

// A failure to release is a failure to close, reported rather than dropped.
func TestRuntimeCloseReportsAReleaseFailure(t *testing.T) {
	releaseErr := errors.New("the router refused the release")
	recorder := &closeRecorder{}
	runtime := recordingRuntime(recorder, nil, nil, nil)
	runtime.store = &countingLease{
		bundle:   storagebundle.Bundle{Session: runtime.SessionService},
		recorder: recorder,
		err:      releaseErr,
	}

	require.ErrorIs(t, runtime.Close(), releaseErr)
	require.ErrorIs(t, runtime.Close(), releaseErr)
}

// Two revisions on one profile share one store, and a revision on another
// profile gets its own. The Runtime does not decide that — the Resolver does —
// but the Runtime has to pass the profile through faithfully for it to be
// decidable at all.
func TestRuntimesShareAndSeparateStoresByProfile(t *testing.T) {
	resolver := newRecordingResolver(t)
	perProfile := map[string]session.Service{}
	var mu sync.Mutex
	resolver.bundleFor = func(request resolveRequest) storagebundle.Bundle {
		mu.Lock()
		defer mu.Unlock()
		key := request.tenantID + "/" + request.profileID
		if _, built := perProfile[key]; !built {
			sessions := sessioninmemory.NewSessionService()
			t.Cleanup(func() { require.NoError(t, sessions.Close()) })
			perProfile[key] = sessions
		}
		return storagebundle.Bundle{Session: perProfile[key]}
	}

	build := func(revisionID string, profileID string) *Runtime {
		revision := publishedRevision(revisionID, "echo-"+revisionID)
		revision.Config.BackendProfileID = profileID
		revision = sealed(revision)
		runtime, err := NewRuntime(
			context.Background(), revision, resolver, entitling(t, revision))
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, runtime.Close()) })
		return runtime
	}

	first := build("revision-1", "shared")
	second := build("revision-2", "shared")
	separate := build("revision-3", "other")

	require.Same(t, first.SessionService, second.SessionService)
	require.NotSame(t, first.SessionService, separate.SessionService)

	// Closing one holder of a shared store must not take it away from the
	// other: the lease is a claim, not ownership.
	require.NoError(t, first.Close())
	response := serveChatCompletion(t, second, `{
		"model":"echo-revision-2","user":"user-1",
		"messages":[{"role":"user","content":"after close"}]
	}`)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "echo: after close")
}

// The compatibility entry point cannot honour a profile reference, so it
// refuses one. Ignoring it is how a tenant's conversations end up in storage its
// revision did not name, and that has to fail closed rather than quietly.
func TestNewRuntimeFromRevisionRefusesANamedProfile(t *testing.T) {
	sessions := sessioninmemory.NewSessionService()
	t.Cleanup(func() { require.NoError(t, sessions.Close()) })
	revision := publishedRevision("revision-1", "echo-v1")
	revision.Config.BackendProfileID = "tenant-a-postgres"
	revision = sealed(revision)

	runtime, err := NewRuntimeFromRevision(revision, sessions, entitling(t, revision))
	require.ErrorIs(t, err, storagebundle.ErrProfileNotFound)
	require.ErrorContains(t, err, "tenant-a-postgres")
	require.Nil(t, runtime)
}

// And with no profile named it keeps working exactly as it did: the caller
// still owns the store, and closing the Runtime does not close it.
func TestNewRuntimeFromRevisionBorrowsTheCallersStore(t *testing.T) {
	sessions := &closeCountingSessionService{Service: sessioninmemory.NewSessionService()}
	t.Cleanup(func() { require.NoError(t, sessions.Service.Close()) })
	revision := publishedRevision("revision-1", "echo-v1")

	runtime, err := NewRuntimeFromRevision(revision, sessions, entitling(t, revision))
	require.NoError(t, err)
	require.Same(t, sessions, runtime.SessionService)

	require.NoError(t, runtime.Close())
	require.NoError(t, runtime.Close())
	require.Zero(t, sessions.closeCount(), "a borrowed store was closed by its borrower")
}

func TestNewRuntimeFromRevisionStillRequiresAStore(t *testing.T) {
	revision := publishedRevision("revision-1", "echo-v1")

	runtime, err := NewRuntimeFromRevision(revision, nil, entitling(t, revision))
	require.ErrorIs(t, err, storagebundle.ErrIncompleteBundle)
	require.Nil(t, runtime)
}

// Holding a lease for its whole life is what a Runtime is, so it is part of
// what makes one complete.
//
// A Runtime without one is not a Runtime missing a field. Close is the only
// thing that releases a lease, so a Runtime that never had one is a Runtime
// whose storage lifetime nothing bounds: Router.Close stops waiting the moment
// the last counted holder lets go, and the store this Runtime is still serving
// from gets closed underneath it.
//
// Nothing in this package can build one — assembleRuntime refuses a nil lease
// and sets the field on every success — and outside it the unexported adapter
// fields already make a hand-built Runtime fail validation. This covers the
// case between the two: a build func inside this package, or a later field
// added to the struct literal without the lease. It has to be refused loudly
// rather than cached and served.
func TestRuntimeWithoutAStorageLeaseIsRefusedAndNeverCached(t *testing.T) {
	revision := publishedRevision("revision-1", "echo-v1")
	sessions := &closeCountingSessionService{Service: sessioninmemory.NewSessionService()}
	runtime, err := newOwnedRuntime(revision, sessions, nil, entitling(t, revision))
	require.NoError(t, err)

	// Complete in every other respect: identity, agent, runner, session service
	// and protocol adapter all came from the real build.
	require.NoError(t, runtime.validate())

	// Taken rather than dropped, so this test does not leak the store it is
	// about. That the Runtime can no longer release it is the point.
	lease := runtime.store
	t.Cleanup(func() { require.NoError(t, lease.Release()) })
	runtime.store = nil

	require.ErrorContains(t, runtime.validate(), "runtime holds no storage lease")
	handler, err := runtime.OpenAIHandler()
	require.ErrorContains(t, err, "runtime holds no storage lease")
	require.Nil(t, handler, "a Runtime nothing is counting must not serve traffic")
	require.Zero(t, sessions.closeCount())
}

// And the resolver refuses it rather than caching it, which is where it would
// otherwise become the process's storage lifetime problem: a cached Runtime is
// the one thing a Router waits for at Close.
func TestRuntimeResolverRefusesARuntimeWithNoStorageLease(t *testing.T) {
	repository, scope := resolverRepository(t, "tenant-a", "assistant", 1)
	stripped := make(chan storagebundle.Lease, 1)
	resolver, err := NewRuntimeResolver(repository, func(
		_ context.Context,
		revision tenant.AgentRevision,
	) (*Runtime, error) {
		built, err := newOwnedRuntime(
			revision, sessioninmemory.NewSessionService(), nil, security.DenyCapabilities())
		if err != nil {
			return nil, err
		}
		stripped <- built.store
		built.store = nil
		return built, nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resolver.Close()) })

	resolved, err := resolver.Resolve(context.Background(), scope, "assistant", "")
	// Released before anything is asserted. If the refusal ever regresses, this
	// call hands the lease back so the assertions below report a failure — a
	// retained lease would instead deadlock resolver.Close in the cleanup, and a
	// hung suite says much less than a failing one.
	resolved.Release()

	require.ErrorContains(t, err, "runtime holds no storage lease")
	require.Nil(t, resolved.Runtime)
	require.Zero(t, resolver.CacheSize(), "a Runtime nothing is counting was cached")

	// The store the build func opened is this test's to release now: refusing the
	// Runtime is what took it out of anyone else's hands.
	require.NoError(t, (<-stripped).Release())
}

// The owning path closes its store exactly once, and takes ownership on call
// rather than on success: a caller whose build failed must not be left guessing
// whether it still has a store to close.
func TestNewOwnedRuntimeOwnsTheStoreFromTheCall(t *testing.T) {
	sessions := &closeCountingSessionService{Service: sessioninmemory.NewSessionService()}
	revision := publishedRevision("revision-1", "echo-v1")

	runtime, err := newOwnedRuntime(revision, sessions, nil, entitling(t, revision))
	require.NoError(t, err)
	require.NoError(t, runtime.Close())
	require.NoError(t, runtime.Close())
	require.Equal(t, 1, sessions.closeCount())

	// A revision that cannot be built releases the store on the way out, so
	// nothing is leaked and nothing is closed twice.
	refused := &closeCountingSessionService{Service: sessioninmemory.NewSessionService()}
	unpublished := publishedRevision("revision-2", "echo-v2")
	unpublished.Status = tenant.RevisionStatusDraft

	runtime, err = newOwnedRuntime(unpublished, refused, nil, entitling(t, unpublished))
	require.Error(t, err)
	require.Nil(t, runtime)
	require.Equal(t, 1, refused.closeCount())
}

// Each demo runtime builds and owns a store of its own, and closing one
// releases only that one.
//
// The release is not asserted through the store itself: the upstream in-memory
// service keeps serving after Close and guards its own Close with a sync.Once,
// so it would absorb a double close rather than report one. Ownership is
// asserted where it is observable, in TestNewOwnedRuntimeOwnsTheStoreFromTheCall
// — what belongs here is that the demo has no shared static store for that
// ownership to be wrong about.
func TestDemoRuntimeOwnsAStoreOfItsOwn(t *testing.T) {
	first := NewDemoRuntime()
	second := NewDemoRuntime()
	t.Cleanup(func() { require.NoError(t, second.Close()) })

	require.NotSame(t, first.SessionService, second.SessionService)

	require.NoError(t, first.Close())
	require.NoError(t, first.Close())

	// The surviving demo runtime still serves: closing the first took nothing
	// away from it.
	events, err := second.Runner.Run(
		context.Background(),
		"u/demo-user",
		"c/demo-session",
		model.NewUserMessage("after the other one closed"),
	)
	require.NoError(t, err)
	var completed bool
	for evt := range events {
		completed = completed || evt.IsRunnerCompletion()
	}
	require.True(t, completed)
}

// closeCountingSessionService wraps a real session service and counts closes,
// so "closed exactly once" is observable on a store that also has to work.
type closeCountingSessionService struct {
	session.Service

	mu     sync.Mutex
	closes int
}

func (s *closeCountingSessionService) Close() error {
	s.mu.Lock()
	s.closes++
	s.mu.Unlock()
	return nil
}

func (s *closeCountingSessionService) closeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closes
}
