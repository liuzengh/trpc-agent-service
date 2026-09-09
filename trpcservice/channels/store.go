package channels

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Sentinel errors a caller is expected to branch on.
//
// Everything else a Store returns is one of the tenant package's sentinels —
// ErrInvalidArgument for a malformed call, ErrTenantScope for a cross-tenant
// one, ErrNotFound for a missing row — so a caller has one vocabulary to map
// rather than one per package.
var (
	// ErrStaleClaim means a CAS did not match: the caller's claim or send token
	// is no longer the one on the row, or the row is no longer in the state the
	// token was issued for.
	//
	// It is the single most important error in this package, because it is what
	// a Worker sees when recovery has already taken its Run away. The correct
	// response is always to stop and write nothing, never to retry the write
	// with a fresh read: the other Worker owns the row now.
	ErrStaleClaim = errors.New("channels: stale claim")

	// ErrExecutionStarted means a yield was attempted after the Runner had
	// already been called. A Run past that line cannot be silently requeued —
	// there may be Events — so the Worker must finish it instead and let the
	// reconciliation rules decide what a retry does.
	ErrExecutionStarted = errors.New("channels: run execution already started")
)

// MaxListLimit caps every list and scan in this package.
//
// The limit is a required argument rather than an optional one because these
// queries run against the largest tables in the system on behalf of background
// jobs, and a scan that forgot to bound itself would not fail — it would
// succeed slowly, once, in production.
const MaxListLimit = 500

// MaxExhaustSweep bounds how many spent Runs one ClaimNextRun will terminate
// before giving up and letting the next call continue.
//
// The sweep has to exist: a Run whose attempts are gone still blocks its
// Session, so a claim that merely reported "nothing to do" would leave the
// conversation permanently dead. It has to be bounded because in the PostgreSQL
// Store it runs inside a transaction, and a Session with a long tail of
// poisoned messages would otherwise hold locks for as long as it takes to walk
// all of them. Both Stores use this number so they stop in the same place.
const MaxExhaustSweep = 64

// AcceptIDs are the identifiers the platform mints before it calls Accept.
//
// They are minted by the caller, not by the Store, because Accept is retried:
// an adapter whose request timed out re-sends the same event, and if the Store
// generated the ids the retry would produce a second set for a row that already
// exists. With caller-minted ids the retry is recognisably the same call, and
// the duplicate path can return the ids the first call stored.
type AcceptIDs struct {
	InboxID   string
	RunID     string
	RequestID string
}

// Validate rejects ids that could not address a row.
func (i AcceptIDs) Validate() error {
	if err := tenant.ValidateResourceID("inbox id", i.InboxID); err != nil {
		return err
	}
	if err := tenant.ValidateResourceID("run id", i.RunID); err != nil {
		return err
	}
	return tenant.ValidateResourceID("request id", i.RequestID)
}

// AcceptRequest is one external event offered to the platform.
type AcceptRequest struct {
	IDs      AcceptIDs
	Policy   RunPolicy
	Envelope InboundEnvelope
	// Now is the caller's clock. Every mutating method in this package takes
	// one instead of reading the wall clock inside the Store.
	//
	// That is what lets the in-memory Store and the PostgreSQL Store be held to
	// one conformance suite that asserts exact deadlines: a suite that could
	// only say "some time later" would not be able to check that a backoff is
	// bounded, that a recovery grace is honoured, or that an expired lease is
	// expired. It also keeps the two implementations from disagreeing about
	// whose clock counts — the application's or the database's — which is a
	// question every deadline in this package would otherwise inherit.
	//
	// The cost is that Workers on different hosts contribute different clocks,
	// which is exactly the skew RecoveryGrace is sized to absorb.
	Now time.Time
}

// Validate rejects a request the Store must not act on.
func (r AcceptRequest) Validate(scope tenant.TenantContext) error {
	if err := r.IDs.Validate(); err != nil {
		return err
	}
	if err := r.Policy.Validate(); err != nil {
		return err
	}
	if err := r.Envelope.Validate(scope); err != nil {
		return err
	}
	return requireTime("now", r.Now)
}

// AcceptResult reports what Accept stored, or what a previous Accept had
// already stored for the same external event.
//
// Every field describes the row that now exists, never the call that produced
// it. On the duplicate path that distinction is the whole point: the ids, the
// sequence, the attempt and the acceptance time are all read back from the
// original row, so a caller cannot tell a redelivery apart from the first
// delivery by looking at values that would be wrong for the row it addresses.
type AcceptResult struct {
	InboxID   string
	RunID     string
	RequestID string
	// AcceptSequence is this Run's position in its Session's order.
	AcceptSequence int64
	// Attempt is the Run's current attempt counter — its dispatch generation.
	// A caller that announces this Run has to fence its bookkeeping with this
	// value rather than assuming zero: a duplicate that arrives after the Run
	// has been claimed and requeued a few times is on a later generation, and
	// marking it as dispatched under attempt 0 would stamp a generation that no
	// longer exists. See RunDispatch.
	Attempt int32
	// AcceptedAt is when the row was accepted, which on the duplicate path is
	// when the *first* delivery was accepted, not now. It is returned rather
	// than assumed by the caller for the same reason as Attempt: the caller's
	// own clock reading describes this redelivery, and reporting it as the
	// acceptance time would make an event look newer every time it was
	// re-sent.
	AcceptedAt time.Time
	// Duplicate reports that the external event was already accepted. The ids
	// are then the *original* ids, not the ones this call proposed, and no
	// second Run exists.
	Duplicate bool
}

// RunToken addresses a Run together with the claim that authorises writing to
// it. Every mid-execution write takes one, so there is no way to express "write
// to this Run" without also saying which claim is doing the writing.
type RunToken struct {
	RunID      string
	ClaimToken string
}

// Validate rejects a token that could not authorise a write.
func (t RunToken) Validate() error {
	if err := tenant.ValidateResourceID("run id", t.RunID); err != nil {
		return err
	}
	return tenant.ValidateResourceID("claim token", t.ClaimToken)
}

// ClaimRunRequest asks for the next executable Run in one Session.
type ClaimRunRequest struct {
	// ClaimToken is generated by the caller before the call, so that a claim
	// whose response is lost still has a token the caller knows. Without that,
	// a lost response would leave a running Run whose owner cannot address it
	// and which can only be recovered by timeout.
	ClaimToken string
	// ClaimedBy names the Worker, for operators. It is not used for fencing —
	// the token is — because a Worker identity is reused across restarts.
	ClaimedBy string
	Now       time.Time
}

// Validate rejects a claim that could not be fenced.
func (r ClaimRunRequest) Validate() error {
	if err := tenant.ValidateResourceID("claim token", r.ClaimToken); err != nil {
		return err
	}
	if err := tenant.ValidateResourceID("claimed by", r.ClaimedBy); err != nil {
		return err
	}
	return requireTime("now", r.Now)
}

// RunClaim is everything a Worker needs to execute, returned by the claim
// itself so that a Worker never has to re-read the input it is about to run.
type RunClaim struct {
	Run            Run
	Message        InboundMessage
	DeliveryTarget DeliveryTarget
	// RemainingExecutionBudget is the execution deadline minus the claim time.
	// It is what a Worker derives its Run context deadline from; it is given as
	// a duration rather than a deadline so that the Worker's own clock, not the
	// clock that wrote the row, bounds its own work.
	//
	// It has to be anchored to a clock reading taken *before* the claim call,
	// never to the time the Worker gets around to using it:
	//
	//	claimedAt := time.Now()
	//	claim, ok, err := store.ClaimRun(ctx, ...)
	//	deadline := claimedAt.Add(claim.RemainingExecutionBudget)
	//
	// Anchoring it later re-bases the budget and quietly moves the deadline past
	// the Run's persisted ExecuteDeadlineAt, which does not move — so this
	// Worker would still be running when the recovery scanner is allowed to take
	// the Run over, and two executions would overlap on one Session. Anchoring
	// it before the call cannot: the row's deadline was stamped at some moment
	// after that read, so the derived one lands at or before it.
	RemainingExecutionBudget time.Duration
}

// Clone returns a copy that shares nothing mutable with c.
func (c RunClaim) Clone() RunClaim {
	copied := c
	copied.Run = c.Run.Clone()
	copied.Message = c.Message.Clone()
	copied.DeliveryTarget = c.DeliveryTarget.Clone()
	return copied
}

// YieldRunRequest returns a claimed Run to the queue before execution began.
type YieldRunRequest struct {
	// ErrorType records why, for operators. The Run is not failing, so this is
	// the reason for the *retry*, not an outcome.
	ErrorType ErrorType
	Now       time.Time
}

// Validate rejects a yield the Store must not act on.
func (r YieldRunRequest) Validate() error {
	if err := r.ErrorType.Validate(); err != nil {
		return err
	}
	return requireTime("now", r.Now)
}

// RequeueOutcome reports what happened to an item that was being returned to
// the queue. It exists because "put it back" and "it has no attempts left, so
// it was terminated instead" are both correct outcomes of the same call, and a
// caller that assumed the first would keep waiting for a Run that is already
// dead.
type RequeueOutcome string

const (
	// RequeueRetried means the item is queued for another attempt.
	RequeueRetried RequeueOutcome = "retried"
	// RequeueExhausted means the attempt budget was spent, so the item was
	// terminated as failed(attempts_exhausted) instead of being requeued.
	RequeueExhausted RequeueOutcome = "exhausted"
)

// FinishRunRequest is the single terminal write of an execution: the Run's
// outcome and the whole answer, in one transaction.
//
// They are one call because the alternative has no correct ordering. Writing
// the outcome first and the parts second loses the answer if the process dies
// between them; writing the parts first and the outcome second can send an
// answer for a Run that a recovery scanner has already given to someone else.
type FinishRunRequest struct {
	Token RunToken
	// Status must be succeeded or failed.
	Status RunStatus
	// RevisionID is the revision that actually answered.
	RevisionID string
	// ErrorType must be set when Status is failed and empty when it is not.
	ErrorType ErrorType
	Stats     RunStats
	// Outbox is the answer, in order. It may be empty: a Run can legitimately
	// succeed with nothing to say, and a failed Run has nothing to say unless
	// its Binding configured a safe generic reply.
	Outbox []OutboxDraft
	Now    time.Time
}

// Validate rejects a finish the Store must not act on.
func (r FinishRunRequest) Validate() error {
	if err := r.Token.Validate(); err != nil {
		return err
	}
	if err := r.Status.Validate(); err != nil {
		return err
	}
	if !r.Status.Terminal() {
		return errInvalidf("finish status must be succeeded or failed")
	}
	if err := r.ErrorType.Validate(); err != nil {
		return err
	}
	// A succeeded Run with an error type and a failed Run without one are both
	// rows nobody could interpret later, so neither is storable.
	if r.Status == RunSucceeded && r.ErrorType != ErrorNone {
		return errInvalidf("a succeeded run cannot carry an error type")
	}
	if r.Status == RunFailed && r.ErrorType == ErrorNone {
		return errInvalidf("a failed run must carry an error type")
	}
	if r.RevisionID != "" {
		if err := tenant.ValidateResourceID("revision id", r.RevisionID); err != nil {
			return err
		}
	}
	if err := r.Stats.Validate(); err != nil {
		return err
	}
	if len(r.Outbox) > MaxOutboxParts {
		return errInvalidf("a run may not produce more than %d outbox parts", MaxOutboxParts)
	}
	if r.Stats.OutputParts != int32(len(r.Outbox)) {
		return errInvalidf("run output part count does not match the outbox")
	}
	seenPart := make(map[int32]struct{}, len(r.Outbox))
	seenID := make(map[string]struct{}, len(r.Outbox))
	for _, draft := range r.Outbox {
		if err := draft.Validate(); err != nil {
			return err
		}
		if _, ok := seenPart[draft.PartNo]; ok {
			return errInvalidf("duplicate outbox part number")
		}
		seenPart[draft.PartNo] = struct{}{}
		if _, ok := seenID[draft.OutboxID]; ok {
			return errInvalidf("duplicate outbox id")
		}
		seenID[draft.OutboxID] = struct{}{}
	}
	return requireTime("now", r.Now)
}

// FinishRunResult is the committed state, returned so a caller can publish
// wakeups for exactly the parts that were created without a second read.
type FinishRunResult struct {
	Run    Run
	Outbox []OutboxPart
}

// RunRef is the minimal cross-tenant reference a platform scan may return.
//
// It is deliberately only a tenant and an id. Scans run across every tenant on
// behalf of internal jobs, and their results reach wakeup payloads and logs; a
// scan that returned session ids, external event ids or bodies would put
// tenant-identifying data on paths that ordinary tenant-scoped reads keep it
// off. Anything more is fetched afterwards, under an explicit tenant scope.
type RunRef struct {
	TenantID string
	RunID    string
}

// Validate rejects a reference that could not address a row.
func (r RunRef) Validate() error {
	if err := tenant.ValidateResourceID("tenant id", r.TenantID); err != nil {
		return err
	}
	return tenant.ValidateResourceID("run id", r.RunID)
}

// RunDispatch is a Run a scan found dispatchable, together with the generation
// it was found on.
//
// # Why the attempt travels with the reference
//
// Publishing a wakeup and recording that it was published are two steps with a
// gap between them, and the row is not frozen across that gap. Between the list
// and the mark a Worker can claim the Run, fail, and requeue it — which bumps
// the attempt, sets a fresh NextAttemptAt in the future, and clears
// LastDispatchedAt precisely so the next scan will announce the new generation
// when it comes due. A mark that matched only the reference and the status
// would land on that new generation and stamp it as already announced, and the
// row would then sit unannounced until StaleAfter expired rather than until its
// backoff did.
//
// Attempt is that generation. The mark is a compare-and-set on it, so a stamp
// computed for attempt N can only ever be written to attempt N; when the row has
// moved on, the write matches nothing and the caller is told so by a count of
// zero. There is no separate dispatch token because there is nothing a token
// would carry that the attempt does not already say, and a second column would
// be a second thing every requeue path had to remember to reset.
//
// # Why it does not travel any further
//
// This type is a Store-internal candidate, not a payload. Wakeups still carry a
// bare ref (see RunWakeup): a consumer reads the Run from PostgreSQL, so an
// attempt in the payload could only be stale by the time it was read, and a
// consumer that trusted it would be fencing against a number from the past.
type RunDispatch struct {
	RunRef
	// Attempt is the Run's attempt counter as the scan read it.
	Attempt int32
}

// Validate rejects a candidate that could not address a generation.
func (d RunDispatch) Validate() error {
	if err := d.RunRef.Validate(); err != nil {
		return err
	}
	if d.Attempt < 0 {
		return errInvalidf("attempt must not be negative")
	}
	return nil
}

// RunRecovery reports one Run the recovery scanner moved.
type RunRecovery struct {
	RunRef
	Outcome RequeueOutcome
}

// ScanScope optionally narrows a scan to one binding of one tenant.
//
// The zero value is the platform scan every existing caller makes. It crosses
// tenants deliberately: recovery and dispatch are duties of the process, not of
// a tenant, and the refs these scans return carry no conversation content.
//
// A scope with both fields set is what a single-binding adapter needs. Such an
// adapter may only act on its own rows, and filtering a returned page in Go
// cannot give it that: the LIMIT has already chosen the page by then, so one
// busy neighbour would starve it while it discarded rows it was never allowed
// to see. The narrowing therefore belongs in the query.
//
// Half a scope is refused rather than guessed at. A tenant on its own would
// quietly widen to every binding that tenant has, and a binding on its own
// would quietly cross the isolation boundary; neither is a scan any caller
// means to ask for, and both would fail as a silent over-read rather than as an
// error.
type ScanScope struct {
	TenantID  string
	BindingID string
}

// Scoped reports whether this scope narrows the scan.
func (s ScanScope) Scoped() bool {
	return s != ScanScope{}
}

// Validate rejects a scope that is neither absent nor complete.
func (s ScanScope) Validate() error {
	if !s.Scoped() {
		return nil
	}
	if s.TenantID == "" || s.BindingID == "" {
		return errInvalidf("scan scope needs both tenant id and channel binding id")
	}
	if err := tenant.ValidateResourceID("tenant id", s.TenantID); err != nil {
		return err
	}
	return tenant.ValidateResourceID("channel binding id", s.BindingID)
}

// RecoverRequest drives a scanner over expired in-flight rows.
type RecoverRequest struct {
	Now time.Time
	// Scope is optional; see ScanScope.
	Scope ScanScope
	Limit int
}

// Validate rejects a scan the Store must not run.
func (r RecoverRequest) Validate() error {
	if err := requireTime("now", r.Now); err != nil {
		return err
	}
	if err := r.Scope.Validate(); err != nil {
		return err
	}
	return requireLimit(r.Limit)
}

// DispatchScanRequest finds work whose wakeup may have been lost.
//
// Redis Streams are allowed to drop or duplicate wakeups, so PostgreSQL is the
// only place that knows what is actually due. This scan is the safety net that
// makes that acceptable: it re-publishes for anything due whose last wakeup is
// older than StaleAfter.
type DispatchScanRequest struct {
	Now time.Time
	// StaleAfter is how long since the last published wakeup before another one
	// is worth publishing. It stops every scan from re-publishing everything
	// that is merely still queued.
	StaleAfter time.Duration
	// Scope is optional; see ScanScope.
	Scope ScanScope
	Limit int
}

// Validate rejects a scan the Store must not run.
func (r DispatchScanRequest) Validate() error {
	if err := requireTime("now", r.Now); err != nil {
		return err
	}
	if r.StaleAfter <= 0 {
		return errInvalidf("stale after must be positive")
	}
	if err := r.Scope.Validate(); err != nil {
		return err
	}
	return requireLimit(r.Limit)
}

// OutboxToken addresses a part together with the send that authorises writing
// to it, mirroring RunToken.
type OutboxToken struct {
	OutboxID  string
	SendToken string
}

// Validate rejects a token that could not authorise a write.
func (t OutboxToken) Validate() error {
	if err := tenant.ValidateResourceID("outbox id", t.OutboxID); err != nil {
		return err
	}
	return tenant.ValidateResourceID("send token", t.SendToken)
}

// ClaimOutboxRequest claims one part for sending.
type ClaimOutboxRequest struct {
	// OutboxID, when set, claims exactly that part and no other. This is the
	// wakeup-driven path: the Sender was told which part to send. When it is
	// empty the earliest due part is claimed instead, which is the recovery
	// path for a wakeup that was never delivered. Both go through the same CAS,
	// so a part cannot be claimed twice by taking different routes to it.
	OutboxID string
	// ExpectedChannel, when set, refuses to claim a part whose persisted channel
	// is not this one.
	//
	// It is not a filter and it is not a router: the caller already decided which
	// part it wants, from a wakeup that named a channel. This re-checks that
	// claim against the row, which is the only authority, so a wakeup that was
	// forged, or that went stale because the row was rewritten, cannot make a
	// process deliver a part through the wrong protocol. A mismatch is *not* a
	// send attempt — the part is left exactly as it was, attempt included, for
	// the dispatch scan to re-announce with the right channel.
	//
	// There is deliberately no binding filter here. A Sender is a protocol
	// adapter, not a credential holder; which binding's credential an answer goes
	// out under is resolved from the persisted part when it is sent, so
	// restricting the claim by binding would only fragment one shared consumer
	// group into pools that starve each other.
	ExpectedChannel ChannelType
	SendToken       string
	SentBy          string
	// SendTimeout bounds this attempt. Past it the part is treated as an
	// unknown outcome rather than a failure, because a request that timed out
	// may still have been delivered.
	//
	// It is persisted as an absolute deadline at microsecond resolution, so a
	// value finer than that is refused rather than silently rounded: the whole
	// point of the stored deadline is that every process derives the same budget
	// from it, and two Stores that rounded differently would not.
	SendTimeout time.Duration
	Now         time.Time
}

// MinSendTimeout is the shortest send budget a claim may ask for. It is the
// resolution of the column the deadline is stored in, so anything smaller could
// not be told apart from no budget at all.
const MinSendTimeout = time.Microsecond

// Validate rejects a claim that could not be fenced.
func (r ClaimOutboxRequest) Validate() error {
	if r.OutboxID != "" {
		if err := tenant.ValidateResourceID("outbox id", r.OutboxID); err != nil {
			return err
		}
	}
	if r.ExpectedChannel != "" {
		if err := r.ExpectedChannel.Validate(); err != nil {
			return err
		}
	}
	if err := tenant.ValidateResourceID("send token", r.SendToken); err != nil {
		return err
	}
	if err := tenant.ValidateResourceID("sent by", r.SentBy); err != nil {
		return err
	}
	if err := validateSendTimeout(r.SendTimeout); err != nil {
		return err
	}
	return requireTime("now", r.Now)
}

// validateSendTimeout is shared by the claim request and DispatcherOptions so a
// budget that a Store would refuse cannot be configured in the first place.
func validateSendTimeout(timeout time.Duration) error {
	if timeout < MinSendTimeout {
		return errInvalidf("send timeout must be at least %s", MinSendTimeout)
	}
	// Stored as a timestamptz, whose resolution is one microsecond. A finer
	// value would be rounded on the way in, so the deadline the Sender is held
	// to would not be the deadline the caller asked for.
	if timeout%time.Microsecond != 0 {
		return errInvalidf("send timeout must be a whole number of microseconds")
	}
	return nil
}

// OutboxClaim is everything a Sender needs to deliver one part.
//
// There is deliberately no remaining-budget field. The budget is
// Part.SendDeadlineAt, an absolute time the Store wrote and every process reads
// the same way; a duration computed at claim time would already be wrong by the
// time the caller used it, and a caller that used it anyway could hand a Sender
// a context that outlived the deadline the recovery scanner is enforcing.
type OutboxClaim struct {
	Part OutboxPart
}

// Clone returns a copy that shares nothing mutable with c.
func (c OutboxClaim) Clone() OutboxClaim {
	copied := c
	copied.Part = c.Part.Clone()
	return copied
}

// SendOutcome is the four-way classification every send attempt collapses to.
//
// The fourth one is the one that matters. A send that times out, or whose
// connection drops after the request left, has an *unknown* outcome: treating
// it as a failure and retrying can double-post, and treating it as a success
// can silently drop the answer. It gets its own outcome so that the retry can
// be marked, and so nobody has to guess on behalf of the channel.
type SendOutcome string

const (
	// SendSucceeded means the channel confirmed delivery.
	SendSucceeded SendOutcome = "succeeded"
	// SendRetryable means the channel refused in a way that may pass, such as
	// rate limiting or an expired credential.
	SendRetryable SendOutcome = "retryable"
	// SendPermanent means the channel refused in a way that never passes.
	SendPermanent SendOutcome = "permanent"
	// SendUnknown means the attempt neither confirmed nor definitely failed.
	SendUnknown SendOutcome = "outcome_unknown"
)

// Validate rejects an outcome outside the closed vocabulary.
func (o SendOutcome) Validate() error {
	switch o {
	case SendSucceeded, SendRetryable, SendPermanent, SendUnknown:
		return nil
	default:
		return errInvalidf("unsupported send outcome")
	}
}

// SendResult is what a Sender learned from one attempt.
type SendResult struct {
	Outcome SendOutcome
	// ErrorType classifies a non-success. It must be empty on success.
	ErrorType ErrorType
	// ExternalMessageID is the channel's id for the delivered message, when it
	// returns one. It is optional because not every channel does.
	ExternalMessageID string
}

// Validate rejects a result the Store could not store coherently.
//
// The outcome and the error class are not two independent fields. Each outcome
// admits exactly the classes that can describe it, and the pairing is checked
// here rather than trusted, because an adapter is third-party code and the
// combination is what decides whether a part is retried, marked at risk of
// duplication, or closed forever. "Permanent" carrying a rate-limit class would
// throw an answer away over a transient refusal; "succeeded" carrying an error
// class would record a failure as a delivery. Anything outside the table is
// refused, and the dispatcher treats a refused result as an unknown outcome —
// the fail-closed direction.
func (r SendResult) Validate() error {
	if err := r.Outcome.Validate(); err != nil {
		return err
	}
	if err := r.ErrorType.Validate(); err != nil {
		return err
	}
	if !sendOutcomeAdmits(r.Outcome, r.ErrorType) {
		return errInvalidf(
			"send outcome %q cannot be classified as %q", r.Outcome, r.ErrorType)
	}
	if r.ExternalMessageID != "" && r.Outcome != SendSucceeded {
		return errInvalidf("only a successful send may record an external message id")
	}
	return boundedText(
		"external message id", r.ExternalMessageID, MaxExternalMessageIDBytes, false)
}

// sendOutcomeAdmits is the fixed outcome-to-class table.
//
// Deliberately not derived from a property of the class: predecessor_failed,
// run_timeout and the rest are real error classes that are simply never the
// answer to "what did this send attempt do?", and a rule clever enough to
// exclude them would also silently admit the next class somebody adds.
func sendOutcomeAdmits(outcome SendOutcome, errorType ErrorType) bool {
	switch outcome {
	case SendSucceeded:
		return errorType == ErrorNone
	case SendRetryable:
		// Internal is here because a dispatcher that could not even start the
		// attempt reports it, and that is a retry with no delivery risk.
		return errorType == ErrorRateLimited ||
			errorType == ErrorAuthExpired ||
			errorType == ErrorInternal
	case SendPermanent:
		return errorType == ErrorPermanent
	case SendUnknown:
		return errorType == ErrorOutcomeUnknown
	default:
		return false
	}
}

// CompleteOutboxRequest closes one send attempt.
type CompleteOutboxRequest struct {
	Token  OutboxToken
	Result SendResult
	Now    time.Time
}

// Validate rejects a completion the Store must not act on.
func (r CompleteOutboxRequest) Validate() error {
	if err := r.Token.Validate(); err != nil {
		return err
	}
	if err := r.Result.Validate(); err != nil {
		return err
	}
	return requireTime("now", r.Now)
}

// OutboxRef is the minimal cross-tenant reference a platform scan may return.
// See RunRef for why it carries nothing else.
type OutboxRef struct {
	TenantID string
	OutboxID string
}

// Validate rejects a reference that could not address a row.
func (r OutboxRef) Validate() error {
	if err := tenant.ValidateResourceID("tenant id", r.TenantID); err != nil {
		return err
	}
	return tenant.ValidateResourceID("outbox id", r.OutboxID)
}

// OutboxDispatch is a part a scan found dispatchable, together with the
// generation it was found on. See RunDispatch for why the attempt is part of
// the candidate and why it stops at the Store.
type OutboxDispatch struct {
	OutboxRef
	// Channel is the part's persisted channel, carried out of the scan so the
	// wakeup can name it; see OutboxWakeup.Channel. Reading it here costs
	// nothing — the scan is already on the row — and it is the only way a
	// published wakeup can be routed without the consumer first claiming the
	// part to find out whether it can send it.
	Channel ChannelType
	// Attempt is the part's attempt counter as the scan read it.
	Attempt int32
}

// Validate rejects a candidate that could not address a generation.
func (d OutboxDispatch) Validate() error {
	if err := d.OutboxRef.Validate(); err != nil {
		return err
	}
	if err := d.Channel.Validate(); err != nil {
		return err
	}
	if d.Attempt < 0 {
		return errInvalidf("attempt must not be negative")
	}
	return nil
}

// OutboxRecovery reports one part the recovery scanner moved.
type OutboxRecovery struct {
	OutboxRef
	Outcome RequeueOutcome
}

// ListRequest is a bounded, tenant-scoped listing.
type ListRequest struct {
	Limit int
}

// Validate rejects an unbounded listing.
func (r ListRequest) Validate() error { return requireLimit(r.Limit) }

// InboxStore accepts external events and reads them back.
type InboxStore interface {
	// Accept atomically records one external event and the Run that will answer
	// it. Re-accepting the same (tenant, binding, external event id) returns the
	// original identifiers, sequence, attempt and acceptance time with Duplicate
	// set, and creates no second Run.
	Accept(
		ctx context.Context,
		scope tenant.TenantContext,
		request AcceptRequest,
	) (AcceptResult, error)

	// GetInbox returns one accepted event, including its body. It is tenant
	// scoped because it is the only read in this package that returns content.
	GetInbox(
		ctx context.Context,
		scope tenant.TenantContext,
		inboxID string,
	) (InboxMessage, error)
}

// RunStore owns the execution side of the pipeline.
//
// Every method here takes an explicit tenant.TenantContext except the two
// scans, which are internal platform jobs and return only references. Passing
// the scope explicitly rather than reading it from the context is what makes a
// missing scope a compile error instead of a runtime one.
type RunStore interface {
	// GetRun returns one Run.
	GetRun(ctx context.Context, scope tenant.TenantContext, runID string) (Run, error)

	// ListSessionRuns returns a Session's Runs in accept order.
	ListSessionRuns(
		ctx context.Context,
		scope tenant.TenantContext,
		key sessiondir.Key,
		request ListRequest,
	) ([]Run, error)

	// ClaimNextRun claims the earliest executable Run of one Session.
	//
	// It looks only at the Session's earliest non-terminal Run. If that Run is
	// running, or is still in backoff, nothing is claimed and the second return
	// value is false — the Store never skips ahead to a later message, because
	// a conversation answered out of order is worse than one answered late.
	//
	// If that earliest Run has already spent its attempt budget it is
	// terminated as failed(attempts_exhausted) in the same transaction and the
	// search continues, so an undeliverable message cannot block its Session
	// forever.
	ClaimNextRun(
		ctx context.Context,
		scope tenant.TenantContext,
		key sessiondir.Key,
		request ClaimRunRequest,
	) (RunClaim, bool, error)

	// MarkRunStarted records that the Runner is about to be called. It is the
	// point after which a yield is no longer safe. Calling it twice under the
	// same claim is a no-op rather than an error, so a retried write does not
	// move the recorded start time.
	//
	// It writes two marks in the same fenced update: Run.ExecutionStartedAt,
	// which belongs to this attempt and is cleared when the claim is, and
	// Run.FirstExecutionStartedAt, which is set once for the Run and never
	// moved. A later attempt therefore sets the first and leaves the second
	// alone.
	MarkRunStarted(
		ctx context.Context,
		scope tenant.TenantContext,
		token RunToken,
		now time.Time,
	) error

	// RecordRunRevision stores the revision that is answering this Run, once
	// the Session pin has been resolved.
	RecordRunRevision(
		ctx context.Context,
		scope tenant.TenantContext,
		token RunToken,
		revisionID string,
		now time.Time,
	) error

	// YieldRun returns a claimed Run to the queue. It succeeds only while
	// execution has not started; afterwards it reports ErrExecutionStarted and
	// the caller must finish the Run instead.
	YieldRun(
		ctx context.Context,
		scope tenant.TenantContext,
		token RunToken,
		request YieldRunRequest,
	) (RequeueOutcome, error)

	// FinishRun terminates a Run and writes its answer in one transaction. A
	// caller whose claim is stale gets ErrStaleClaim and writes no parts.
	FinishRun(
		ctx context.Context,
		scope tenant.TenantContext,
		request FinishRunRequest,
	) (FinishRunResult, error)

	// ListRunOutbox returns a Run's answer parts in order.
	ListRunOutbox(
		ctx context.Context,
		scope tenant.TenantContext,
		runID string,
		request ListRequest,
	) ([]OutboxPart, error)

	// RecoverRuns requeues or terminates Runs whose recovery deadline passed.
	// It crosses tenants unless the request carries a ScanScope, and returns
	// only references.
	RecoverRuns(ctx context.Context, request RecoverRequest) ([]RunRecovery, error)

	// ListDispatchableRuns returns due Runs whose wakeup looks lost, each with
	// the generation it was found on. It crosses tenants unless the request
	// carries a ScanScope, and returns only references.
	ListDispatchableRuns(ctx context.Context, request DispatchScanRequest) ([]RunDispatch, error)

	// MarkRunsDispatched records that a wakeup was published, after it was.
	// Recording before publishing would suppress the next scan for a wakeup
	// that never went out.
	//
	// The stamp is written only where the row is still accepted, still on the
	// candidate's attempt, and already due at `at` — so a row that was claimed
	// and requeued while the wakeup was in flight, and a row whose backoff has
	// not expired, are both left alone. The returned count is how many rows
	// matched; anything less than len(dispatches) means the rest moved on, which
	// is an ordinary race and not an error.
	//
	// `at` is when the publishing finished, not when the scan started. A batch
	// that took longer than the scan's StaleAfter would otherwise stamp itself
	// as already stale and be re-announced immediately by the next scan.
	//
	// The stamp never moves backwards: a slow caller marking an older time
	// cannot undo a newer mark, so two overlapping scans cannot talk each other
	// into re-publishing.
	MarkRunsDispatched(ctx context.Context, dispatches []RunDispatch, at time.Time) (int, error)
}

// OutboxStore owns the delivery side of the pipeline.
type OutboxStore interface {
	// GetOutboxPart returns one part, including its body and target.
	GetOutboxPart(
		ctx context.Context,
		scope tenant.TenantContext,
		outboxID string,
	) (OutboxPart, error)

	// ClaimOutbox claims one due part for sending.
	//
	// # Parts of one answer are ordered
	//
	// A part is eligible only when every lower-numbered part of the same Run has
	// already been sent. One answer that did not fit in one message is still one
	// answer, and a user who receives its second half first has been sent
	// something the agent never said. The gate is in the Store rather than in
	// the dispatcher because it is the only place that can enforce it: the
	// wakeups are a lossy, unordered transport, and any number of dispatchers
	// may be claiming at once.
	//
	// The guarantee is per Run and no wider. Two Runs of the same Session are
	// ordered by the Run side — a Session's head blocks the next message — and
	// nothing here claims to order answers across Sessions.
	ClaimOutbox(
		ctx context.Context,
		scope tenant.TenantContext,
		request ClaimOutboxRequest,
	) (OutboxClaim, bool, error)

	// CompleteOutbox closes a send attempt and reports the resulting status.
	//
	// When the attempt ends the part terminally — refused permanently, or out of
	// attempts — the same transaction closes the Run's later parts that have not
	// been sent yet, with ErrorPredecessorFailed. Parts already in flight are
	// left alone: their send is the authority on their own outcome, and there is
	// no honest way to say what a request that is still on the wire did.
	CompleteOutbox(
		ctx context.Context,
		scope tenant.TenantContext,
		request CompleteOutboxRequest,
	) (OutboxStatus, error)

	// RecoverOutbox requeues or terminates parts whose send deadline passed.
	// A requeued part is marked duplicate_risk, because a send that ran out of
	// time may still have been delivered. A part it terminates closes the Run's
	// later unsent parts, exactly as CompleteOutbox does. It crosses tenants
	// unless the request carries a ScanScope.
	RecoverOutbox(ctx context.Context, request RecoverRequest) ([]OutboxRecovery, error)

	// ListDispatchableOutbox returns due parts whose wakeup looks lost, each with
	// the generation it was found on. It applies the same eligibility rule as
	// ClaimOutbox, so it lists at most the currently sendable head of each Run
	// and never announces a part nothing could claim. It crosses tenants unless
	// the request carries a ScanScope.
	ListDispatchableOutbox(ctx context.Context, request DispatchScanRequest) ([]OutboxDispatch, error)

	// MarkOutboxDispatched records that a wakeup was published, after it was.
	// It is fenced, due-guarded and monotonic exactly like MarkRunsDispatched,
	// and returns how many rows matched.
	MarkOutboxDispatched(ctx context.Context, dispatches []OutboxDispatch, at time.Time) (int, error)
}

// Store is the whole channel pipeline surface. The three interfaces above are
// separate so that a Worker can be handed the RunStore alone and a Sender the
// OutboxStore alone; this composite exists for wiring and for the conformance
// suite, which has to exercise all three against one backend.
type Store interface {
	InboxStore
	RunStore
	OutboxStore
}

// requireTime rejects a zero clock reading. A zero time silently means "the
// beginning of time" in every comparison in this package, which would make
// every deadline already expired.
func requireTime(field string, value time.Time) error {
	if value.IsZero() {
		return errInvalidf("%s is required", field)
	}
	return nil
}

// requireLimit rejects an unbounded or nonsensical list size.
func requireLimit(limit int) error {
	if limit <= 0 {
		return errInvalidf("limit must be positive")
	}
	if limit > MaxListLimit {
		return errInvalidf("limit must not exceed %d", MaxListLimit)
	}
	return nil
}

// checkContext mirrors the guard every store in this repository makes first.
func checkContext(ctx context.Context) error {
	if ctx == nil {
		return errInvalidf("context is required")
	}
	return ctx.Err()
}

// scopedID validates a tenant scope and one resource id together, which is the
// opening of nearly every tenant-scoped read.
func scopedID(scope tenant.TenantContext, field, id string) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	return tenant.ValidateResourceID(field, id)
}
