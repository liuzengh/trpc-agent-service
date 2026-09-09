package sessionrun

import (
	"context"
	"sync"
	"testing"
	"time"

	platformagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	platformconfig "github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/security"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/stretchr/testify/require"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

// These tests run against the real resolver and the real coordinator wherever
// the claim is about them. A fake resolver cannot hold a Runtime lease — the
// lease is unexported in the agent package — so "the Runtime was released"
// would become "Release was called", which is a claim about this file rather
// than about the platform. Against the real resolver it is observable from
// outside: Close waits for every lease, so a leaked one hangs it.

const (
	testRequestID  = "request-1"
	secondRevision = "echo-v2"
)

// fixture is one platform's worth of run machinery: the same objects main.go
// wires, minus the HTTP server.
type fixture struct {
	service    *Service
	resolver   *platformagent.RuntimeResolver
	repository *tenant.MemoryRepository
	directory  sessiondir.Directory
	pins       *sessiondir.MemoryDirectory
	leases     sessionlease.Coordinator
	store      *sessionlease.MemoryStore
}

type fixtureOptions struct {
	// directory replaces the pin store, for the races and failures a working
	// one cannot be asked to produce.
	directory sessiondir.Directory

	// coordinator replaces the lease coordinator, for the same reason.
	coordinator sessionlease.Coordinator

	// lease tunes the default coordinator. The zero value is the production
	// TTL, which is what most tests want: a lease that cannot expire under them.
	lease sessionlease.Config
}

func newFixture(t *testing.T, opts fixtureOptions) *fixture {
	t.Helper()
	repository := tenant.NewMemoryRepository()
	require.NoError(t, platformconfig.SeedDemo(context.Background(), repository))

	sessions := sessioninmemory.NewSessionService()
	t.Cleanup(func() { require.NoError(t, sessions.Close()) })

	resolver, err := platformagent.NewRuntimeResolver(
		repository,
		func(
			_ context.Context,
			revision tenant.AgentRevision,
		) (*platformagent.Runtime, error) {
			// The demo revision names no secret and no policy, so the strictest
			// authorizer is also the accurate one.
			return platformagent.NewRuntimeFromRevision(
				revision, sessions, security.DenyCapabilities())
		},
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, resolver.Close()) })

	pins := sessiondir.NewMemoryDirectory()
	var directory sessiondir.Directory = pins
	if opts.directory != nil {
		directory = opts.directory
	}

	store := sessionlease.NewMemoryStore()
	coordinator := opts.coordinator
	if coordinator == nil {
		concrete, coordErr := sessionlease.NewMemoryCoordinator(store, opts.lease)
		require.NoError(t, coordErr)
		coordinator = concrete
	}
	// Registered after the resolver's cleanup, so it runs before it: a run that
	// is only still alive because it holds a lease is cut loose before Close
	// waits for the Runtime it is holding.
	t.Cleanup(func() { require.NoError(t, coordinator.Close()) })

	service, err := NewService(resolver, directory, coordinator)
	require.NoError(t, err)
	return &fixture{
		service:    service,
		resolver:   resolver,
		repository: repository,
		directory:  directory,
		pins:       pins,
		leases:     coordinator,
		store:      store,
	}
}

// publishSecondRevision adds a revision and makes it the app's default route,
// so a run with no hint resolves to it rather than to the seeded one.
func (f *fixture) publishSecondRevision(t *testing.T) {
	t.Helper()
	scope := tenant.TenantContext{TenantID: platformconfig.DemoTenantID}
	seeded, err := f.repository.GetRevision(
		context.Background(), scope, platformconfig.DemoAgentAppID, platformconfig.DemoRevisionID,
	)
	require.NoError(t, err)
	config := seeded.Config
	config.Description = "Second revision"
	_, err = f.repository.CreateRevision(context.Background(), scope, tenant.AgentRevision{
		ID:         secondRevision,
		TenantID:   platformconfig.DemoTenantID,
		AgentAppID: platformconfig.DemoAgentAppID,
		RevisionNo: 2,
		CreatedBy:  "test",
		Config:     config,
	})
	require.NoError(t, err)
	_, _, err = f.repository.PublishRevision(
		context.Background(), scope, platformconfig.DemoAgentAppID, secondRevision,
	)
	require.NoError(t, err)
}

// demoRequest is one authenticated run of the seeded demo agent.
func demoRequest(sessionID string, revisionHint string) Request {
	return Request{
		RequestID:    testRequestID,
		TenantID:     platformconfig.DemoTenantID,
		AppID:        platformconfig.DemoAgentAppID,
		PrincipalID:  platformconfig.DemoPrincipalID,
		SessionID:    sessionID,
		RevisionHint: revisionHint,
	}
}

func TestNewServiceRequiresEveryDependency(t *testing.T) {
	fixture := newFixture(t, fixtureOptions{})

	_, err := NewService(nil, fixture.directory, fixture.leases)
	require.ErrorContains(t, err, "runtime resolver is required")
	_, err = NewService(fixture.resolver, nil, fixture.leases)
	require.ErrorContains(t, err, "session directory is required")
	// No fallback to a process-wide coordinator: a service that silently
	// coordinated through its own memory would be exclusive against nothing but
	// itself, which is worse than refusing to start.
	_, err = NewService(fixture.resolver, fixture.directory, nil)
	require.ErrorContains(t, err, "session lease coordinator is required")
}

// A normal run: the lease is taken, the session adopts a revision, the scope is
// the platform's own, and closing gives the session straight back.
func TestStartLeasesPinsAndScopesARun(t *testing.T) {
	fixture := newFixture(t, fixtureOptions{})

	handle, err := fixture.service.Start(context.Background(), demoRequest("conversation-1", ""))
	require.NoError(t, err)

	scope := handle.Scope()
	require.Equal(t, testRequestID, scope.RequestID)
	require.Equal(t, platformconfig.DemoTenantID, scope.TenantID)
	require.Equal(t, platformconfig.DemoAgentAppID, scope.AppID)
	require.Equal(t, platformconfig.DemoPrincipalID, scope.PrincipalID)
	require.Equal(t, "conversation-1", scope.SessionID)
	require.Equal(t, platformconfig.DemoRevisionID, scope.RevisionID)
	// The framework user key is namespaced by the platform: a client-supplied
	// user field can never collide with it.
	require.Equal(t, "u/"+platformconfig.DemoPrincipalID, scope.UserID())
	require.NoError(t, handle.Context().Err())

	// The scope is on the run context, not merely on the handle: everything
	// downstream — the tool audit trail, the contextRunner guarding the adapter
	// — reads identity from the context, so a handle that reported a scope it
	// had not attached would be reporting one nothing enforces.
	attached, err := identity.RunContextFrom(handle.Context())
	require.NoError(t, err)
	require.Equal(t, scope, attached)

	// The session adopted the revision of its first run.
	pinned, found, err := fixture.pins.GetPin(context.Background(), demoRequest("conversation-1", "").Key())
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, platformconfig.DemoRevisionID, pinned)

	handler, err := handle.OpenAIHandler()
	require.NoError(t, err)
	require.NotNil(t, handler)

	handle.Close()
	require.ErrorIs(t, handle.Context().Err(), context.Canceled)

	// A clean finish released, so the next turn is not refused.
	next, err := fixture.service.Start(context.Background(), demoRequest("conversation-1", ""))
	require.NoError(t, err)
	next.Close()
}

// A session another Worker is already running is refused as busy, and nothing
// else happened on the way to the refusal: a request that is going to be turned
// away must not have decided the session's revision or built a Runtime.
func TestStartRefusesASessionAnotherWorkerHolds(t *testing.T) {
	fixture := newFixture(t, fixtureOptions{})
	peer, err := sessionlease.NewMemoryCoordinator(fixture.store, sessionlease.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, peer.Close()) })

	key := demoRequest("conversation-1", "").Key()
	held, err := peer.Acquire(context.Background(), key)
	require.NoError(t, err)

	handle, err := fixture.service.Start(context.Background(), demoRequest("conversation-1", ""))
	require.Nil(t, handle)
	require.ErrorIs(t, err, sessionlease.ErrSessionBusy)
	// Busy is not coordination failing: the platform is healthy and the same
	// request will work shortly. A caller that cannot tell them apart cannot
	// decide whether retrying is sensible.
	require.NotErrorIs(t, err, ErrCoordinationUnavailable)
	require.Equal(t, 0, fixture.pins.Size())
	require.Equal(t, 0, fixture.resolver.CacheSize())

	require.NoError(t, held.Release(context.Background()))
	recovered, err := fixture.service.Start(context.Background(), demoRequest("conversation-1", ""))
	require.NoError(t, err)
	recovered.Close()
}

// Coordination that cannot answer is not permission to run. Everything that is
// not a definite "busy" fails closed, including a reply this build cannot
// classify: guessing that an unreadable answer meant "free" is how two Workers
// end up in one Session.
func TestStartFailsClosedWhenCoordinationCannotAnswer(t *testing.T) {
	failures := map[string]error{
		"unavailable": sessionlease.ErrUnavailable,
		"closed":      sessionlease.ErrClosed,
		"unknown":     errUnrecognisedByThisBuild,
	}
	for name, failure := range failures {
		t.Run(name, func(t *testing.T) {
			fixture := newFixture(t, fixtureOptions{
				coordinator: &stubCoordinator{err: failure},
			})

			handle, err := fixture.service.Start(
				context.Background(), demoRequest("conversation-1", ""))
			require.Nil(t, handle)
			require.ErrorIs(t, err, ErrCoordinationUnavailable)
			require.NotErrorIs(t, err, sessionlease.ErrSessionBusy)
			// The cause survives for a log line, without a caller having to
			// match on it to answer.
			require.ErrorIs(t, err, failure)
			require.Equal(t, 0, fixture.pins.Size())
			require.Equal(t, 0, fixture.resolver.CacheSize())
		})
	}

	// A caller that went away before the lease was decided shares this answer.
	// The run was not started, which is the only fact the answer states.
	fixture := newFixture(t, fixtureOptions{})
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	handle, err := fixture.service.Start(cancelled, demoRequest("conversation-1", ""))
	require.Nil(t, handle)
	require.ErrorIs(t, err, ErrCoordinationUnavailable)
}

// A revision hint can choose a session's first revision. It can never move a
// session that is already pinned — that is a conflict, not a switch — and the
// refusal names the revision the session is actually on, because that is the
// one fact that lets a caller fix it.
func TestStartRefusesARevisionOtherThanThePin(t *testing.T) {
	fixture := newFixture(t, fixtureOptions{})
	fixture.publishSecondRevision(t)

	// The session's first run pins it to the older revision by naming it.
	first, err := fixture.service.Start(
		context.Background(), demoRequest("conversation-1", platformconfig.DemoRevisionID))
	require.NoError(t, err)
	require.Equal(t, platformconfig.DemoRevisionID, first.Scope().RevisionID)
	first.Close()

	handle, err := fixture.service.Start(
		context.Background(), demoRequest("conversation-1", secondRevision))
	require.Nil(t, handle)
	require.ErrorIs(t, err, ErrPinConflict)

	var conflict *PinConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, "conversation-1", conflict.SessionID)
	require.Equal(t, platformconfig.DemoRevisionID, conflict.PinnedRevisionID)
	require.Equal(t, secondRevision, conflict.RequestedRevisionID)

	// The conflict is refused after the lease was taken, which is the path that
	// would strand it. A caller's mistake must not lock the conversation until
	// the TTL expires.
	recovered, err := fixture.service.Start(context.Background(), demoRequest("conversation-1", ""))
	require.NoError(t, err)
	require.Equal(t, platformconfig.DemoRevisionID, recovered.Scope().RevisionID)
	recovered.Close()
}

// Two first runs of one session cannot pin it to two revisions. The directory
// decides a single winner, and the loser adopts it — including the Runtime it
// had already leased for the revision it is not going to run.
func TestStartAdoptsTheWinnerOfAFirstPinRace(t *testing.T) {
	pins := sessiondir.NewMemoryDirectory()
	// EnsurePin answers as it would after a concurrent first run has already
	// pinned this session to the older revision.
	racing := &racingDirectory{inner: pins, winner: platformconfig.DemoRevisionID}
	fixture := newFixture(t, fixtureOptions{directory: racing})
	fixture.publishSecondRevision(t)

	// No hint, so the candidate is the app's current default: the revision this
	// run is about to lose the race with.
	handle, err := fixture.service.Start(context.Background(), demoRequest("conversation-1", ""))
	require.NoError(t, err)
	require.Equal(t, secondRevision, racing.candidate())
	// The run executes as the winner, not as the revision it proposed. A run
	// that kept its candidate would be answering from a revision the session is
	// not pinned to.
	require.Equal(t, platformconfig.DemoRevisionID, handle.Scope().RevisionID)
	require.Equal(t, platformconfig.DemoRevisionID, handle.runtime.Revision.ID)

	handle.Close()

	// Both revisions were built, and neither is still leased. A losing candidate
	// that kept its lease would keep a Runtime alive that nothing will ever use,
	// and would hang shutdown: Close waits for every lease.
	require.Equal(t, 2, fixture.resolver.CacheSize())
	requireResolverClosesPromptly(t, fixture.resolver)
}

// Losing the lease cancels the run. That is cooperative cancellation and
// nothing more: it stops this Worker from carrying on, it does not stop the
// writes already in flight. See the sessionlease package documentation.
func TestStartCancelsARunThatLostItsLease(t *testing.T) {
	fixture := newFixture(t, fixtureOptions{})

	handle, err := fixture.service.Start(context.Background(), demoRequest("conversation-1", ""))
	require.NoError(t, err)
	require.NoError(t, handle.Context().Err())

	// Closing the coordinator is how a process loses every lease it holds at
	// once, which is what a shutdown or a takeover looks like from inside a run.
	require.NoError(t, fixture.leases.Close())
	select {
	case <-handle.Context().Done():
	case <-time.After(10 * time.Second):
		t.Fatal("losing the lease did not cancel the run it belonged to")
	}
	require.ErrorIs(t, handle.Context().Err(), context.Canceled)

	handle.Close()
}

// A run that ended because the caller went away does not release. The Runner
// keeps writing terminal events for about a second after cancellation, on a
// context this process cannot reach, so handing the session to the next Worker
// at that moment would hand it to one writing against those tail writes. The
// TTL covers the gap instead.
func TestCloseDoesNotReleaseWhenTheCallerWentAway(t *testing.T) {
	released := make(chan struct{}, 1)
	coordinator := &stubCoordinator{lease: &stubLease{
		done:      make(chan struct{}),
		onRelease: func() { released <- struct{}{} },
	}}
	fixture := newFixture(t, fixtureOptions{coordinator: coordinator})

	callerCtx, disconnect := context.WithCancel(context.Background())
	handle, err := fixture.service.Start(callerCtx, demoRequest("conversation-1", ""))
	require.NoError(t, err)

	disconnect()
	handle.Close()
	select {
	case <-released:
		t.Fatal("a run that ended because the caller went away released the lease")
	default:
	}
}

// A lease that was already lost is not released either. Releasing is
// owner-matched so it could not disturb the new owner, but asking at all would
// be this build claiming an authority over the session it no longer has.
func TestCloseDoesNotReleaseALeaseItAlreadyLost(t *testing.T) {
	released := make(chan struct{}, 1)
	lost := make(chan struct{})
	coordinator := &stubCoordinator{lease: &stubLease{
		done:      lost,
		onRelease: func() { released <- struct{}{} },
	}}
	fixture := newFixture(t, fixtureOptions{coordinator: coordinator})

	handle, err := fixture.service.Start(context.Background(), demoRequest("conversation-1", ""))
	require.NoError(t, err)

	close(lost)
	select {
	case <-handle.Context().Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the run was never told it lost the lease")
	}

	handle.Close()
	select {
	case <-released:
		t.Fatal("a run released a lease it had already lost")
	default:
	}
}

// Close is the only correct way to finish a run, so it has to survive being the
// deferred close of a handler that also closed it explicitly. It releases once,
// and it leaves no watcher behind: one goroutine per run that outlived the run
// is a leak that only shows up under load.
//
// The repeats are sequential because that is the shape the platform produces —
// an explicit Close followed by a deferred one, on the one goroutine serving the
// request — and because a repeat is not a barrier: it returns the moment it sees
// the closed mark, without waiting for the call that is actually releasing. Only
// the call that does the work can be relied on to have finished it, so firing
// several at once and then asserting the release happened would be asserting a
// scheduling outcome.
func TestCloseIsIdempotentAndLeavesNoWatcher(t *testing.T) {
	releases := make(chan struct{}, 8)
	coordinator := &stubCoordinator{lease: &stubLease{
		done:      make(chan struct{}),
		onRelease: func() { releases <- struct{}{} },
	}}
	fixture := newFixture(t, fixtureOptions{coordinator: coordinator})

	handle, err := fixture.service.Start(context.Background(), demoRequest("conversation-1", ""))
	require.NoError(t, err)
	watching := handle.watching

	for range 5 {
		handle.Close()
	}

	require.Len(t, releases, 1)
	// Close waits for the watcher, so by the time it returns the goroutine is
	// gone rather than merely on its way out.
	select {
	case <-watching:
	default:
		t.Fatal("Close returned while the lease watcher was still running")
	}

	// A nil handle is closable too: a caller that deferred Close before checking
	// the error would otherwise panic on the failure path.
	var missing *Handle
	require.NotPanics(t, missing.Close)
}

// The Runtime lease outlives the run's own cleanup and is given back only when
// the caller says the run is over. Until then shutdown waits: a Runtime closed
// while its Runner is still emitting would cut an answer in half.
func TestCloseReleasesTheRuntimeLease(t *testing.T) {
	fixture := newFixture(t, fixtureOptions{})

	handle, err := fixture.service.Start(context.Background(), demoRequest("conversation-1", ""))
	require.NoError(t, err)

	closed := make(chan error, 1)
	go func() { closed <- fixture.resolver.Close() }()
	select {
	case <-closed:
		t.Fatal("the resolver closed while a started run still held its Runtime")
	case <-time.After(100 * time.Millisecond):
	}

	handle.Close()
	select {
	case closeErr := <-closed:
		require.NoError(t, closeErr)
	case <-time.After(10 * time.Second):
		t.Fatal("closing the run did not release the Runtime lease")
	}
}

// A failure after the lease was taken gives everything back through the same
// Close a successful run uses, so there is one release path rather than one per
// failure. A pin the directory cannot answer is that failure.
func TestStartReleasesEverythingWhenTheDirectoryFails(t *testing.T) {
	failing := &failingDirectory{err: errUnrecognisedByThisBuild}
	fixture := newFixture(t, fixtureOptions{directory: failing})

	handle, err := fixture.service.Start(context.Background(), demoRequest("conversation-1", ""))
	require.Nil(t, handle)
	require.ErrorIs(t, err, errUnrecognisedByThisBuild)

	// The lease came back: a second attempt reaches the directory again rather
	// than being refused as busy, which is what a stranded lease would look
	// like. A failure that had nothing to do with the session must not lock it.
	next, err := fixture.service.Start(context.Background(), demoRequest("conversation-1", ""))
	require.Nil(t, next)
	require.ErrorIs(t, err, errUnrecognisedByThisBuild)
	require.NotErrorIs(t, err, sessionlease.ErrSessionBusy)
	requireResolverClosesPromptly(t, fixture.resolver)
}

// The direct Runner entry is for callers with no protocol adapter — an IM
// channel, or a scheduler. It takes the identity from the platform, not from a
// parameter, and the platform's request id reaches the framework Events.
func TestRunUsesTheTrustedIdentityAndThePlatformRequestID(t *testing.T) {
	fixture := newFixture(t, fixtureOptions{})

	handle, err := fixture.service.Start(context.Background(), demoRequest("conversation-1", ""))
	require.NoError(t, err)
	defer handle.Close()

	events, err := handle.Run(model.NewUserMessage("hello"))
	require.NoError(t, err)

	seen := 0
	for received := range events {
		seen++
		// Every event of this run is labelled with the id the platform minted,
		// which is the whole point of having one: a correlation id that is only
		// on some of the events correlates nothing.
		require.Equal(t, testRequestID, received.RequestID)
	}
	require.NotZero(t, seen, "the run produced no events to correlate")
}

// A caller of the direct entry cannot reconfigure the run it was given. The
// guard is the signature itself, so this is an assignment rather than a test
// body: there is no runtime observation to make when the override cannot be
// expressed in the first place.
//
// A variadic trpcagent.RunOption parameter is what this rules out. That type is
// an opaque function over the framework's whole RunOptions struct, so the shape
// alone would be enough — an IM caller holding a Handle for a session already
// pinned to an immutable revision could pass WithAppName and land in another
// app's session partition, or WithAgent, WithModel, WithInstruction,
// WithAdditionalTools, WithToolPermissionPolicy and run something other than
// what the revision and the tool policy describe. Widening Run back to accept
// them stops this package compiling.
//
// The OpenAI adapter path is unaffected: contextRunner has to satisfy the
// framework's Runner interface and keeps taking the adapter's options, and its
// own tests cover what happens to them.
var _ func(model.Message) (<-chan *event.Event, error) = (*Handle)(nil).Run

// startEntry is one of the two ways to begin executing on a Handle. Both are
// covered by the same claim, so the tests below drive them through one type
// rather than asserting the rule twice and hoping the two stay in step.
type startEntry struct {
	name  string
	start func(*Handle) error
}

func startEntries() []startEntry {
	return []startEntry{
		{"Run", func(h *Handle) error {
			events, err := h.Run(model.NewUserMessage("hello"))
			if err == nil {
				// Drain, so a successful first entry does not leave a run in
				// flight while the second one is attempted.
				for range events {
				}
			}
			return err
		}},
		{"OpenAIHandler", func(h *Handle) error {
			_, err := h.OpenAIHandler()
			return err
		}},
	}
}

// One Handle is one execution, whichever entry is used and whichever entry is
// tried second.
//
// The pairing matters as much as the repetition. A Handle holds one run lease
// and one pin, and a caller that took the HTTP adapter and then also called Run
// would have two executions writing to one session transcript under a lease that
// says a single Worker is running it — the interleaving nothing downstream is
// built to expect. So the claim is shared between the entries rather than being
// one guard per entry.
func TestHandleStartsExactlyOneExecution(t *testing.T) {
	for _, first := range startEntries() {
		for _, second := range startEntries() {
			t.Run(first.name+" then "+second.name, func(t *testing.T) {
				fixture := newFixture(t, fixtureOptions{})
				handle, err := fixture.service.Start(
					context.Background(), demoRequest("conversation-1", ""),
				)
				require.NoError(t, err)
				defer handle.Close()

				require.NoError(t, first.start(handle))

				err = second.start(handle)
				require.ErrorIs(t, err, ErrRunAlreadyStarted)
				require.NotErrorIs(t, err, ErrRunClosed)
			})
		}
	}
}

// A closed Handle refuses to start anything, and says so with an error a caller
// can match rather than a nil channel or a handler pointing at a Runtime whose
// lease has already been given back.
func TestHandleRefusesToStartAfterClose(t *testing.T) {
	for _, entry := range startEntries() {
		t.Run(entry.name, func(t *testing.T) {
			fixture := newFixture(t, fixtureOptions{})
			handle, err := fixture.service.Start(
				context.Background(), demoRequest("conversation-1", ""),
			)
			require.NoError(t, err)

			handle.Close()
			require.ErrorIs(t, entry.start(handle), ErrRunClosed)

			// Still idempotent, and still refusing after the extra Close.
			handle.Close()
			require.ErrorIs(t, entry.start(handle), ErrRunClosed)
			requireResolverClosesPromptly(t, fixture.resolver)
		})
	}
}

// There is deliberately no test here racing an entry against Close. The claim
// does not make that safe and this package does not promise it does: claiming
// the slot and using the leases are two steps, so a Close landing between them
// releases the Runtime lease and the run lease while an execution is starting
// against them. Nothing in the atomic orders those two steps against each other,
// and the race detector would stay quiet because the fields involved are written
// once in Start and only read afterwards — the conflict is over ownership, not
// over memory. A test asserting otherwise would pass on scheduling luck and
// document a guarantee that is not there. See the contract on Handle: entries
// and Close do not overlap.

// A request that cannot address exactly one run is refused before anything is
// taken. The revision hint is checked here rather than after the lease, so a
// malformed one costs the session nothing.
func TestStartValidatesTheRequestBeforeTakingAnything(t *testing.T) {
	fixture := newFixture(t, fixtureOptions{})
	complete := demoRequest("conversation-1", platformconfig.DemoRevisionID)

	for name, mutate := range map[string]func(*Request){
		"request":   func(r *Request) { r.RequestID = "" },
		"tenant":    func(r *Request) { r.TenantID = "" },
		"app":       func(r *Request) { r.AppID = "" },
		"principal": func(r *Request) { r.PrincipalID = "" },
		"session":   func(r *Request) { r.SessionID = "not a session" },
		"revision":  func(r *Request) { r.RevisionHint = "not a revision" },
	} {
		t.Run(name, func(t *testing.T) {
			request := complete
			mutate(&request)
			handle, err := fixture.service.Start(context.Background(), request)
			require.Nil(t, handle)
			require.ErrorIs(t, err, tenant.ErrInvalidArgument)
		})
	}

	// Nothing was taken on the way to any of those refusals. The revision hint
	// in particular is now checked before the lease rather than after it, so a
	// caller's typo costs the session nothing at all.
	require.Equal(t, 0, fixture.pins.Size())
	require.Equal(t, 0, fixture.resolver.CacheSize())

	// An empty hint is not a malformed one: it means "whatever this app routes
	// to", which is how every session that names no revision starts.
	handle, err := fixture.service.Start(context.Background(), demoRequest("conversation-1", ""))
	require.NoError(t, err)
	handle.Close()

	var missingContext context.Context
	handle, err = fixture.service.Start(missingContext, complete)
	require.Nil(t, handle)
	require.ErrorIs(t, err, tenant.ErrInvalidArgument)
}

// requireResolverClosesPromptly asserts that no Runtime lease was leaked. Close
// waits for every outstanding lease, so a leak is not a slow close: it is one
// that never returns.
func requireResolverClosesPromptly(t *testing.T, resolver *platformagent.RuntimeResolver) {
	t.Helper()
	closed := make(chan error, 1)
	go func() { closed <- resolver.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the resolver never closed: a Runtime lease was leaked")
	}
}

// errUnrecognisedByThisBuild stands for a backend failure that is neither busy
// nor one of the documented sentinels.
var errUnrecognisedByThisBuild = errStub("the backend said something unexpected")

type errStub string

func (e errStub) Error() string { return string(e) }

// stubCoordinator produces the Acquire results a working coordinator cannot be
// asked for.
type stubCoordinator struct {
	lease sessionlease.Lease
	err   error
}

func (c *stubCoordinator) Acquire(
	context.Context,
	sessiondir.Key,
) (sessionlease.Lease, error) {
	return c.lease, c.err
}

func (c *stubCoordinator) Close() error { return nil }

type stubLease struct {
	done      chan struct{}
	onRelease func()
}

func (l *stubLease) Fence() uint64 { return 1 }

func (l *stubLease) Done() <-chan struct{} { return l.done }

func (l *stubLease) Release(context.Context) error {
	if l.onRelease != nil {
		l.onRelease()
	}
	return nil
}

// racingDirectory answers EnsurePin as it would after a concurrent first run of
// the same session has already won, which is the one outcome a single-threaded
// test cannot otherwise produce.
type racingDirectory struct {
	inner  *sessiondir.MemoryDirectory
	winner string

	mu         sync.Mutex
	candidates []string
}

func (d *racingDirectory) GetPin(
	ctx context.Context,
	key sessiondir.Key,
) (string, bool, error) {
	return d.inner.GetPin(ctx, key)
}

func (d *racingDirectory) EnsurePin(
	ctx context.Context,
	key sessiondir.Key,
	candidateRevisionID string,
) (string, error) {
	d.mu.Lock()
	d.candidates = append(d.candidates, candidateRevisionID)
	d.mu.Unlock()
	return d.inner.EnsurePin(ctx, key, d.winner)
}

// candidate is the revision the losing run proposed.
func (d *racingDirectory) candidate() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.candidates) == 0 {
		return ""
	}
	return d.candidates[0]
}

// failingDirectory is a pin store that cannot answer.
type failingDirectory struct {
	err error
}

func (d *failingDirectory) GetPin(
	context.Context,
	sessiondir.Key,
) (string, bool, error) {
	return "", false, d.err
}

func (d *failingDirectory) EnsurePin(
	context.Context,
	sessiondir.Key,
	string,
) (string, error) {
	return "", d.err
}
