// Package sessionrun starts and ends one run of one conversation, for every
// protocol this platform speaks.
//
// A run is the span between "this caller may talk to this session" and "this
// process is no longer writing to it". Four things have to happen inside that
// span, in an order that is not a matter of taste:
//
//   - the run lease is taken, so no second Worker is running this session;
//   - the session's revision pin is read, or adopted on a first run, so a
//     conversation keeps the behaviour it started with;
//   - the Runtime for that revision is leased, so shutdown cannot close it
//     while the run is still using it;
//   - a trusted [identity.RunContext] is attached, so nothing downstream has to
//     take identity from a request body.
//
// Cleanup then runs in reverse, under rules stricter than "undo what was
// done": see [Handle.Close].
//
// This package is protocol-independent on purpose. There is no status code, no
// header and no wire error in it, because the same sequence has to serve the
// OpenAI-compatible HTTP endpoint and the IM channels that come after it — and
// a second implementation of the lease/pin/runtime dance is exactly where the
// two would drift apart. A caller maps the errors returned here onto whatever
// its own protocol says; the web package does that for HTTP.
package sessionrun

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	platformagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionlease"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

// releaseTimeout bounds giving the run lease back on a clean finish. By then
// the caller has already delivered whatever it was going to deliver, so this
// cannot make anyone wait for something they can see — it only stops a stalled
// coordinator from holding a connection open. A release that does not make it
// inside the budget costs nothing but the remainder of the lease's TTL.
const releaseTimeout = 2 * time.Second

// Errors a caller has to be able to tell apart, because the answers they
// deserve are different. Everything else — an unknown app, an unpublished
// revision, a revision this tenant may not run — surfaces as the domain error
// the control plane already defines, unchanged.
var (
	// ErrCoordinationUnavailable reports that the run lease could not be
	// decided: the backend was unreachable, answered something this build
	// refuses to interpret, or the caller's context ended first. The run was
	// not started. It is deliberately not the same error as a busy session:
	// busy means the platform is healthy and someone else is running this
	// conversation, this means nobody knows.
	ErrCoordinationUnavailable = errors.New("sessionrun: run coordination is unavailable")

	// ErrPinConflict reports a run that named a revision other than the one its
	// session is already pinned to. Match it with errors.Is; read the ids off
	// [PinConflictError] with errors.As.
	ErrPinConflict = errors.New("sessionrun: session is pinned to another revision")

	// ErrRunAlreadyStarted reports a second attempt to start execution on a
	// Handle that has already started once — see [Handle] for why one Handle is
	// one execution. A caller that genuinely wants another turn calls
	// [Service.Start] again and takes the leases again.
	ErrRunAlreadyStarted = errors.New("sessionrun: run already started on this handle")

	// ErrRunClosed reports an attempt to start execution on a Handle whose Close
	// has already begun. The leases behind it are gone or going, so there is
	// nothing left to execute under.
	ErrRunClosed = errors.New("sessionrun: run is closed")
)

// PinConflictError names both revisions, so a caller can say which is which
// without parsing a message. The wording of what reaches a client belongs to
// that client's protocol, not here.
type PinConflictError struct {
	SessionID           string
	PinnedRevisionID    string
	RequestedRevisionID string
}

func (e *PinConflictError) Error() string {
	return fmt.Sprintf(
		"%s: session %q is pinned to revision %q, not %q",
		ErrPinConflict.Error(),
		e.SessionID,
		e.PinnedRevisionID,
		e.RequestedRevisionID,
	)
}

// Unwrap makes errors.Is(err, ErrPinConflict) work on a value carrying ids.
func (e *PinConflictError) Unwrap() error { return ErrPinConflict }

// runtimeResolver is the part of the agent resolver a run needs: hand it a
// revision id, get back a leased Runtime. Callers pass *agent.RuntimeResolver.
type runtimeResolver interface {
	Resolve(
		context.Context,
		tenant.TenantContext,
		string,
		string,
	) (platformagent.ResolvedRuntime, error)
}

// Request is one run of one conversation, as the entry layer has already
// authenticated and normalised it.
//
// Every field is trusted by the time it arrives. This package neither
// authenticates nor parses: an entry layer that took the principal from a
// request body would produce a Request this package could not tell from a real
// one, so that decision stays where the credential is.
type Request struct {
	// RequestID is the platform's own identifier for this run. It is minted by
	// the entry layer and never taken from the caller, because it labels the
	// framework events of the run and the tool audit records it produces: a
	// client-supplied value would let one caller file its traffic under
	// another's identifier.
	RequestID string

	TenantID    string
	AppID       string
	PrincipalID string
	SessionID   string

	// RevisionHint selects the revision of a session's *first* run only. It can
	// never move a session that is already pinned — that is a conflict, not a
	// switch — and an empty hint takes the app's current default route.
	RevisionHint string
}

// Validate rejects a request that cannot address exactly one run.
func (r Request) Validate() error {
	if err := tenant.ValidateResourceID("request id", r.RequestID); err != nil {
		return err
	}
	if err := r.Key().Validate(); err != nil {
		return err
	}
	if r.RevisionHint == "" {
		return nil
	}
	return tenant.ValidateResourceID("revision id", r.RevisionHint)
}

// Key is the conversation this run belongs to: the full identity, never a
// session id on its own.
func (r Request) Key() sessiondir.Key {
	return sessiondir.Key{
		TenantID:    r.TenantID,
		AppID:       r.AppID,
		PrincipalID: r.PrincipalID,
		SessionID:   r.SessionID,
	}
}

// Service starts runs. It is safe for concurrent use; everything it holds
// already is.
type Service struct {
	resolver runtimeResolver
	sessions sessiondir.Directory
	leases   sessionlease.Coordinator
}

// NewService builds the service. Every dependency is a parameter and none has
// a default: a process that forgot to configure coordination must fail to
// build rather than fall back to something that coordinates nothing, which is
// precisely the deployment where two Workers end up running one Session.
func NewService(
	resolver runtimeResolver,
	sessions sessiondir.Directory,
	leases sessionlease.Coordinator,
) (*Service, error) {
	if resolver == nil {
		return nil, fmt.Errorf("sessionrun: runtime resolver is required")
	}
	if sessions == nil {
		return nil, fmt.Errorf("sessionrun: session directory is required")
	}
	if leases == nil {
		return nil, fmt.Errorf("sessionrun: session lease coordinator is required")
	}
	return &Service{resolver: resolver, sessions: sessions, leases: leases}, nil
}

// Start takes the run lease, resolves the revision this session is pinned to,
// and returns a handle the caller must Close exactly once.
//
// The order is the safety property. The lease is taken before the pin is read
// and before a Runtime is built, because everything after it writes to the
// session: a request that is going to be refused must not have created a pin or
// leased a Runtime on the way to being refused. Every failure after the lease
// is taken gives it back through the same [Handle.Close] a successful run uses,
// so there is one release path rather than one per failure.
//
// ctx is the caller's: when it ends, so does the run. The returned run context
// has one more way to end, losing the lease.
func (s *Service) Start(ctx context.Context, request Request) (*Handle, error) {
	if s == nil || s.resolver == nil || s.sessions == nil || s.leases == nil {
		return nil, fmt.Errorf("sessionrun: service is not configured")
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: context is required", tenant.ErrInvalidArgument)
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	key := request.Key()
	lease, err := s.leases.Acquire(ctx, key)
	if err != nil {
		// Busy is passed through as itself: it is the one "the system is
		// working, come back later" answer, and a caller has to be able to say
		// so. Everything else is coordination failing closed, including a
		// cancelled context and a backend reply this build cannot classify —
		// guessing that an unreadable answer meant "free" is how two Workers
		// end up in one Session.
		if errors.Is(err, sessionlease.ErrSessionBusy) {
			return nil, fmt.Errorf("sessionrun: acquire run lease: %w", err)
		}
		return nil, fmt.Errorf("%w: %w", ErrCoordinationUnavailable, err)
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	// An independent watcher rather than a check inside the run. A protocol
	// adapter can block for as long as its client takes to read, and renewal
	// runs in the coordinator's own goroutine, so neither of them is anywhere a
	// lost lease could be noticed promptly. It ends with the run, and Close
	// waits for it, so no run leaves a goroutine behind.
	watching := make(chan struct{})
	go func() {
		defer close(watching)
		select {
		case <-lease.Done():
			cancelRun()
		case <-runCtx.Done():
		}
	}()
	handle := &Handle{
		runCtx:    runCtx,
		cancelRun: cancelRun,
		lease:     lease,
		watching:  watching,
	}
	resolved, err := s.pinnedRuntime(runCtx, key, request.RevisionHint)
	if err != nil {
		handle.Close()
		return nil, err
	}
	handle.runtime = resolved
	scope := identity.RunContext{
		RequestID:   request.RequestID,
		TenantID:    key.TenantID,
		AppID:       key.AppID,
		PrincipalID: key.PrincipalID,
		SessionID:   key.SessionID,
		RevisionID:  resolved.Revision.ID,
	}
	scoped, err := identity.WithRunContext(runCtx, scope)
	if err != nil {
		handle.Close()
		return nil, err
	}
	handle.scope = scope
	// Replaced before the handle is returned, and read by nothing else in the
	// meantime: the watcher above holds its own reference to runCtx rather than
	// reading this field.
	handle.runCtx = scoped
	return handle, nil
}

// pinnedRuntime resolves the revision this session is pinned to. A session
// without a pin adopts the revision of its first run, and the hint applies to
// that first run only. Every path that resolved a Runtime it will not use
// releases it before returning.
func (s *Service) pinnedRuntime(
	ctx context.Context,
	key sessiondir.Key,
	revisionHint string,
) (platformagent.ResolvedRuntime, error) {
	scope := tenant.TenantContext{TenantID: key.TenantID}
	pinned, found, err := s.sessions.GetPin(ctx, key)
	if err != nil {
		return platformagent.ResolvedRuntime{}, err
	}
	if found {
		if revisionHint != "" && revisionHint != pinned {
			return platformagent.ResolvedRuntime{}, &PinConflictError{
				SessionID:           key.SessionID,
				PinnedRevisionID:    pinned,
				RequestedRevisionID: revisionHint,
			}
		}
		return s.resolver.Resolve(ctx, scope, key.AppID, pinned)
	}

	// Only a revision that actually resolved may become a candidate, so a
	// session can never be pinned to a revision this platform cannot serve.
	candidate, err := s.resolver.Resolve(ctx, scope, key.AppID, revisionHint)
	if err != nil {
		return platformagent.ResolvedRuntime{}, err
	}
	winner, err := s.sessions.EnsurePin(ctx, key, candidate.Revision.ID)
	if err != nil {
		candidate.Release()
		return platformagent.ResolvedRuntime{}, err
	}
	if winner == candidate.Revision.ID {
		return candidate, nil
	}
	// A concurrent first run won the pin. Drop the losing lease before taking
	// the winner's, otherwise this run keeps a Runtime alive that it will never
	// use.
	candidate.Release()
	return s.resolver.Resolve(ctx, scope, key.AppID, winner)
}

// Handle is one started run: the scope it may act under, the Runtime it acts
// through, and the two leases it holds.
//
// One Handle is one execution. Exactly one of [Handle.Run] and
// [Handle.OpenAIHandler] may be called, and only once; the second call — of
// either — fails with [ErrRunAlreadyStarted], and any call after [Handle.Close]
// has begun fails with [ErrRunClosed]. The reason is that a Handle is not a
// permit to use a session, it is one leased slot in it: the run lease says one
// Worker is running this conversation, and two executions sharing it would both
// write to the same session transcript while the lease and the pin describe one
// run. A caller wanting a second turn calls [Service.Start] again.
//
// The claim guards handing an entry out, not each use of what was handed out.
// [Handle.OpenAIHandler] returns the Runtime's own http.Handler, and this
// package does not wrap it, so serving that value again after the handler
// returned — or after Close — is a caller error this type does not detect. The
// contract is that the handler is borrowed for the duration of one request and
// not retained: after Close the run context is cancelled and the Runtime lease
// is gone, so what a retained handler would execute against is undefined.
//
// A Handle is used by one goroutine at a time. Close is idempotent — a deferred
// Close after an explicit one is a no-op, which is what the HTTP path relies on —
// but the repeat returns as soon as it sees the closed mark rather than waiting
// for the first call's release to finish, so only the call that did the work can
// be relied on to have completed it.
//
// Nothing here is safe to call concurrently: not two entries, not an entry and
// Close, not two Closes. The claim cannot make them safe, because it
// publishes that a slot was taken, and the entry then goes on to use the leases
// in a separate step, so a Close interleaved between those two would release the
// Runtime lease and the run lease while an execution was starting against them —
// no data race, an ownership one, and one the race detector has no reason to
// report. Making that interleaving safe would mean tracking the lifetime of the
// http.Handler's ServeHTTP and of the Runner's event channel, which is a
// different design from the one this contract describes.
//
// So Close comes strictly after the execution has finished, and finished means:
//
//   - [Handle.Run] — the returned event channel has been drained to completion;
//   - [Handle.OpenAIHandler] — the borrowed handler's ServeHTTP has returned.
//
// That is what the HTTP entry layer does: it serves, and its deferred Close runs
// once ServeHTTP is back.
type Handle struct {
	runCtx    context.Context
	cancelRun context.CancelFunc
	lease     sessionlease.Lease
	watching  <-chan struct{}
	runtime   platformagent.ResolvedRuntime
	scope     identity.RunContext
	state     atomic.Int32
}

// The states of a Handle. The zero value is handleOpen, so a Handle is ready as
// soon as Start has built it.
const (
	handleOpen int32 = iota
	handleClaimed
	handleClosed
)

// claim takes the single execution slot, or says why it could not.
//
// Under the sequential contract in [Handle] the answer is exact: the state can
// only have been moved by an earlier call on this goroutine. A caller that uses
// a Handle from two goroutines may see either refusal, but that caller is
// already outside the contract and the ambiguity is the least of what it has.
func (h *Handle) claim() error {
	switch {
	case h.state.CompareAndSwap(handleOpen, handleClaimed):
		return nil
	case h.state.Load() == handleClosed:
		return ErrRunClosed
	default:
		return ErrRunAlreadyStarted
	}
}

// Context is what the run executes under. It carries the trusted scope as a
// context value and ends when the caller's context ends, when the lease is
// lost, or when the handle is closed.
func (h *Handle) Context() context.Context {
	if h == nil {
		return nil
	}
	return h.runCtx
}

// Scope is the authenticated identity of this run, including the revision the
// session is pinned to and the platform request id.
func (h *Handle) Scope() identity.RunContext {
	if h == nil {
		return identity.RunContext{}
	}
	return h.scope
}

// OpenAIHandler is the protocol adapter of the pinned Runtime. It takes this
// Handle's single execution slot — see [Handle].
//
// This is the one place the package names an HTTP type, and it names it only
// because that is what the framework's adapter is. Nothing here decides a
// status, writes a header or shapes an error body: the handler is handed back
// exactly as the Runtime owns it, and what a protocol does with it is the
// caller's business.
//
// The slot is spent even when the Runtime turns out to have no adapter to give.
// Choosing the entry is what is claimed, not succeeding at it: a run that asked
// for the HTTP path and could not get it is over, not free to try the other one.
func (h *Handle) OpenAIHandler() (http.Handler, error) {
	if h == nil || h.runtime.Runtime == nil {
		return nil, fmt.Errorf("sessionrun: run has no runtime")
	}
	if err := h.claim(); err != nil {
		return nil, err
	}
	return h.runtime.Runtime.OpenAIHandler()
}

// Run executes one turn on the real Runner, for a caller that has no protocol
// adapter to go through — an IM channel, or a scheduler.
//
// Everything about the run except the message comes from the platform: there is
// no userID or sessionID parameter to get wrong, and the only run option applied
// is the platform request id.
//
// There is deliberately no options parameter. A trpcagent.RunOption is an opaque
// function over the framework's entire RunOptions struct, so accepting them here
// would hand the caller WithAppName — which the Runner takes as the session
// partition key, overriding the app name this platform authenticated the caller
// against — as well as WithAgent, WithModel, WithInstruction, WithAdditionalTools
// and WithToolPermissionPolicy, each of which runs something other than the
// revision this session is pinned to. Appending the request id last defends the
// request id; it defends nothing else. When this entry needs a timeout or a
// stream mode, give it an explicit option type owned by this package instead of
// reopening the upstream surface.
//
// It takes this Handle's single execution slot — see [Handle] — so a second Run,
// or a Run after OpenAIHandler, is refused rather than started.
//
// The returned channel must be drained before Close: the Runtime lease is what
// keeps the Runner from being closed underneath it.
func (h *Handle) Run(message model.Message) (<-chan *event.Event, error) {
	if h == nil || h.runtime.Runtime == nil || h.runtime.Runtime.Runner == nil {
		return nil, fmt.Errorf("sessionrun: run has no runner")
	}
	if err := h.claim(); err != nil {
		return nil, err
	}
	return h.runtime.Runtime.Runner.Run(
		h.runCtx,
		h.scope.UserID(),
		h.scope.SessionID,
		message,
		platformagent.TrustedRunOptions(h.scope, nil)...,
	)
}

// Close ends the run and gives back everything it held. It is idempotent, and
// it is the only correct way to finish a started run.
//
// Closing is marked before anything is released, not after, so that a Handle
// reads as closed from the moment it begins closing rather than once it has
// finished. That is also why the state is a single atomic rather than a
// sync.Once: Once publishes completion only after the work it guards returns, so
// a Handle guarded by one would still report itself open throughout the release.
//
// This ordering is not a concurrency guarantee, and it is not what makes the
// entries refuse — see [Handle] for the contract, which is that nothing calls an
// entry while this is running. What it buys is that the state a caller can
// observe never lags the state of the leases.
//
// The release order is fixed and each step is where it is on purpose:
//
//  1. The Runtime lease goes first, and only once the caller says the run is
//     over. A Runtime released while its Runner is still emitting would let
//     shutdown close the adapter mid-answer.
//  2. The run lease is released only after a clean finish — see releaseLease.
//  3. The run context is cancelled after that decision is taken, because the
//     decision is made by reading it.
//  4. The watcher is waited for, so a finished run leaves no goroutine.
//
// Nothing is returned. A release that failed leaves a lock that expires by TTL,
// which is the same state a crash would have left and the next Acquire recovers
// from; by the time this runs the caller has already delivered its answer, so
// there is nobody left to tell and nothing to correct.
func (h *Handle) Close() {
	if h == nil {
		return
	}
	if h.state.Swap(handleClosed) == handleClosed {
		return
	}
	h.runtime.Release()
	h.releaseLease()
	h.cancelRun()
	<-h.watching
}

// releaseLease gives the run lease back, but only after a clean finish.
//
// The asymmetry is deliberate. Releasing makes the session immediately
// available to the next Worker, which is right when this one has genuinely
// stopped — but an upstream Runner whose context was just cancelled keeps
// emitting terminal events for about a second afterwards, on a context this
// process cannot reach. Deleting the lock in that moment would hand the session
// to another Worker while the previous run is still writing to it. Leaving it
// to expire by TTL costs the next caller a few seconds and covers exactly that
// tail.
//
// So: a run that ended by itself releases; a run that ended because the caller
// went away, the lease was lost, or the process is shutting down does not.
func (h *Handle) releaseLease() {
	select {
	case <-h.lease.Done():
		// Lost or taken over. There is nothing of ours left to give back, and a
		// Release cannot disturb a new owner anyway — it is owner-matched — but
		// the TTL is what covers our own tail writes, so leave it alone.
		return
	default:
	}
	if h.runCtx.Err() != nil {
		return
	}
	// Independent of the run: the caller may already be gone, and this still
	// has to complete.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(h.runCtx), releaseTimeout)
	defer cancel()
	_ = h.lease.Release(ctx)
}
