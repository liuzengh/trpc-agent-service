package channels

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/sessiondir"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// RunStatus is the lifecycle of one accepted event.
//
// The legal transitions are:
//
//	accepted -> running                (a Worker claims it)
//	running  -> succeeded | failed     (the Worker reports an outcome)
//	running  -> accepted               (a yield before execution, or the
//	                                    recovery scanner after the deadline)
//
// running -> accepted is the only backward edge and it is the dangerous one:
// it is what makes a Run retryable, and it is also what could run the same
// message twice. It is therefore reachable from exactly two places — YieldRun,
// which requires that execution had not started, and the recovery scanner,
// which requires that recover_after has passed — and from nowhere else.
type RunStatus string

const (
	RunAccepted  RunStatus = "accepted"
	RunRunning   RunStatus = "running"
	RunSucceeded RunStatus = "succeeded"
	RunFailed    RunStatus = "failed"
)

// Validate rejects a status this platform does not model.
func (s RunStatus) Validate() error {
	switch s {
	case RunAccepted, RunRunning, RunSucceeded, RunFailed:
		return nil
	default:
		return errInvalidf("unsupported run status")
	}
}

// Terminal reports whether the Run will never move again. A terminal Run no
// longer blocks the ordering of later Runs in the same Session.
func (s RunStatus) Terminal() bool {
	return s == RunSucceeded || s == RunFailed
}

// OutboxStatus is the lifecycle of one outbound part.
//
//	pending -> sending                       (a Sender claims it)
//	sending -> sent                          (the channel confirmed)
//	sending -> pending                       (retryable, or outcome unknown)
//	sending -> failed                        (permanent, or attempts exhausted)
type OutboxStatus string

const (
	OutboxPending OutboxStatus = "pending"
	OutboxSending OutboxStatus = "sending"
	OutboxSent    OutboxStatus = "sent"
	OutboxFailed  OutboxStatus = "failed"
)

// Validate rejects a status this platform does not model.
func (s OutboxStatus) Validate() error {
	switch s {
	case OutboxPending, OutboxSending, OutboxSent, OutboxFailed:
		return nil
	default:
		return errInvalidf("unsupported outbox status")
	}
}

// Terminal reports whether the part will never move again.
func (s OutboxStatus) Terminal() bool {
	return s == OutboxSent || s == OutboxFailed
}

// ErrorType is a stable, closed classification of why something did not
// succeed. It is stored, it is compared, and it may be exported as a metric
// label, so it is a fixed vocabulary rather than a message.
//
// The reason a failure reason is not free text is the same reason an error
// string may not carry a message body: a Run's failure is usually about the
// user's content, and content that reaches a metric label or a dashboard has
// left the auditable store. A Worker that needs detail logs it against the
// run_id, which is not sensitive; the row keeps only the class.
type ErrorType string

const (
	// ErrorNone is the zero value, used by successful runs and parts.
	ErrorNone ErrorType = ""

	// ErrorAttemptsExhausted means max_attempts was reached. It is the terminal
	// state of every retry loop in this package, and it is what guarantees a
	// poisoned message eventually stops blocking its Session.
	ErrorAttemptsExhausted ErrorType = "attempts_exhausted"

	// ErrorRunTimeout means execution exceeded max_run_duration.
	ErrorRunTimeout ErrorType = "run_timeout"

	// ErrorRunCancelled means execution was cut short by shutdown or by losing
	// the Session lease, before any answer was produced.
	ErrorRunCancelled ErrorType = "run_cancelled"

	// ErrorInterruptedBeforeOutput means a previous attempt wrote a user Event
	// but no final output, and this build could not prove it could resume
	// without appending a second user turn.
	ErrorInterruptedBeforeOutput ErrorType = "interrupted_before_output"

	// ErrorToolOutcomeUnknown means a previous attempt may have executed a tool
	// whose external effect cannot be proven replayable, so retrying could
	// duplicate that effect.
	ErrorToolOutcomeUnknown ErrorType = "tool_outcome_unknown"

	// ErrorAgentFailed means the agent itself returned an error.
	ErrorAgentFailed ErrorType = "agent_failed"

	// ErrorRateLimited is a retryable send rejection.
	ErrorRateLimited ErrorType = "rate_limited"

	// ErrorAuthExpired is a retryable send rejection that needs a refreshed
	// credential; it is distinct from rate limiting because the operator
	// response is different.
	ErrorAuthExpired ErrorType = "auth_expired"

	// ErrorPermanent is a send rejection that will never succeed: a deleted
	// conversation, a revoked bot, a body the channel refuses.
	ErrorPermanent ErrorType = "permanent"

	// ErrorOutcomeUnknown means a send neither succeeded nor definitely failed.
	// It is retried, and the retry is marked duplicate_risk.
	ErrorOutcomeUnknown ErrorType = "outcome_unknown"

	// ErrorPredecessorFailed means an earlier part of the same answer will never
	// be delivered, so this one is closed without ever being attempted.
	//
	// It exists because the parts of one answer are ordered: a user who is sent
	// part 2 of a reply whose part 1 was permanently rejected reads a truncated
	// message with no sign that anything is missing. The alternative — leaving
	// the later parts pending forever — is worse still, because ClaimOutbox
	// gates them behind the predecessor that will now never be sent, so nothing
	// would ever claim them and the dispatch scan would carry them until the
	// rows were deleted by hand.
	ErrorPredecessorFailed ErrorType = "predecessor_failed"

	// ErrorInternal is the catch-all for a platform fault that is not the
	// user's content and not the channel's answer.
	ErrorInternal ErrorType = "internal"
)

// Validate rejects an error type outside the closed vocabulary.
func (e ErrorType) Validate() error {
	switch e {
	case ErrorNone,
		ErrorAttemptsExhausted,
		ErrorRunTimeout,
		ErrorRunCancelled,
		ErrorInterruptedBeforeOutput,
		ErrorToolOutcomeUnknown,
		ErrorAgentFailed,
		ErrorRateLimited,
		ErrorAuthExpired,
		ErrorPermanent,
		ErrorOutcomeUnknown,
		ErrorPredecessorFailed,
		ErrorInternal:
		return nil
	default:
		return errInvalidf("unsupported error type")
	}
}

// Bounds on the retry policy frozen onto a Run at accept time.
//
// The policy is copied onto the row instead of read from configuration at claim
// time. A Run that has already burned three of three attempts must not become
// retryable again because an operator raised the limit, and a Run in flight
// must not have its deadline moved underneath the Worker that is honouring it.
const (
	// MinRunAttempts is one: every accepted Run is executed at least once.
	MinRunAttempts int32 = 1
	// MaxRunAttempts bounds the retry budget. It is small because a Run holds
	// up every later message in its Session while it retries.
	MaxRunAttempts int32 = 16

	// MinRunDuration and MaxRunDurationLimit bound one execution.
	MinRunDuration      = time.Second
	MaxRunDurationLimit = 30 * time.Minute

	// MinRecoveryGrace is the smallest safe gap between "this Worker's
	// execution deadline passed" and "another Worker may take the Run".
	//
	// It has to cover three things that all happen after the deadline: the
	// upstream Runner keeps writing terminal events for about a second after
	// its context is cancelled, the losing Worker still needs a round trip to
	// notice and stop, and the two Workers' clocks are not the same clock. Five
	// seconds is that sum with room to spare; anything smaller and recovery
	// starts a second execution while the first is still writing.
	MinRecoveryGrace = 5 * time.Second
	// MaxRecoveryGrace bounds how long a crashed Worker can strand a Session.
	MaxRecoveryGrace = 30 * time.Minute
)

// Retry backoff bounds. See Backoff.
const (
	// BaseRetryBackoff is the delay after the first failed attempt.
	BaseRetryBackoff = 2 * time.Second
	// MaxRetryBackoff caps the exponential growth.
	MaxRetryBackoff = 5 * time.Minute
	// backoffShiftLimit stops the doubling before it can overflow; by this
	// point the cap has been reached anyway.
	backoffShiftLimit = 20
	// backoffSpreadDivisor sets the width of the deterministic spread as a
	// fraction of the base delay.
	backoffSpreadDivisor = 4
)

// Backoff returns how long to wait before the next attempt of an item that has
// already used `attempt` attempts, given that item's stable id.
//
// Two properties matter and they pull in opposite directions.
//
// It must be *deterministic*, because the in-memory Store and the PostgreSQL
// Store are held to the same conformance suite and a random schedule cannot be
// asserted. It must also *spread*, because a channel outage fails every in
// flight part at once and a purely exponential schedule would retry all of them
// in the same millisecond, reproducing the outage against a recovering channel.
//
// Both are satisfied by taking the spread from a hash of the item's own id
// rather than from a random source: two different rows get different delays,
// and the same row gets the same delay in both Stores and in every test run.
//
// The schedule is computed here rather than in SQL for the same reason the
// caller passes in its own clock: one implementation, one set of numbers, and a
// value that can be asserted directly instead of being re-derived from whatever
// the database thought the time was.
func Backoff(attempt int32, id string) time.Duration {
	shift := attempt - 1
	if shift < 0 {
		shift = 0
	}
	if shift > backoffShiftLimit {
		shift = backoffShiftLimit
	}
	base := BaseRetryBackoff << uint(shift)
	if base > MaxRetryBackoff || base <= 0 {
		base = MaxRetryBackoff
	}
	digest := fnv.New64a()
	// Errors from a hash Write are impossible for the in-memory hashes in the
	// standard library, and fnv's Write is documented never to return one.
	_, _ = digest.Write([]byte(id))
	fraction := float64(digest.Sum64()%1024) / 1024
	spread := time.Duration(float64(base/backoffSpreadDivisor) * fraction)
	return base + spread
}

// NormalizeTime is how every timestamp in this package is stored.
//
// UTC because a row that remembers a location is a row two processes in
// different zones disagree about. Microseconds because that is PostgreSQL's
// timestamptz resolution: a Go time.Time carries nanoseconds, so without this
// truncation a value would come back from the database subtly different from
// the one written, and every "the Store returned what I stored" assertion would
// have to be approximate. Approximate assertions are exactly what the deadline
// and backoff rules here cannot be checked with.
//
// It is exported because both Store implementations and the conformance suite
// have to agree on it exactly.
func NormalizeTime(value time.Time) time.Time {
	return value.UTC().Truncate(time.Microsecond)
}

// NormalizeTimePtr applies NormalizeTime to an optional timestamp, returning a
// fresh pointer so the result shares nothing with its argument.
func NormalizeTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	normalized := NormalizeTime(*value)
	return &normalized
}

// RunPolicy is the retry and timeout budget frozen onto a Run when it is
// accepted. See the constant block above for why it is copied rather than read.
type RunPolicy struct {
	// MaxAttempts is how many times the Run may be claimed in total.
	MaxAttempts int32
	// MaxRunDuration bounds one execution, from claim to outcome.
	MaxRunDuration time.Duration
	// RecoveryGrace is added to the execution deadline to get the point at
	// which another Worker may take over.
	RecoveryGrace time.Duration
}

// Validate rejects a policy that could strand or duplicate work.
func (p RunPolicy) Validate() error {
	if p.MaxAttempts < MinRunAttempts || p.MaxAttempts > MaxRunAttempts {
		return errInvalidf(
			"max attempts must be between %d and %d", MinRunAttempts, MaxRunAttempts)
	}
	if p.MaxRunDuration < MinRunDuration || p.MaxRunDuration > MaxRunDurationLimit {
		return errInvalidf(
			"max run duration must be between %s and %s", MinRunDuration, MaxRunDurationLimit)
	}
	if p.RecoveryGrace < MinRecoveryGrace || p.RecoveryGrace > MaxRecoveryGrace {
		return errInvalidf(
			"recovery grace must be between %s and %s", MinRecoveryGrace, MaxRecoveryGrace)
	}
	// PostgreSQL stores both values as integer milliseconds. Refuse finer
	// precision here so accepting a policy never changes it merely because the
	// caller selected the durable Store instead of the in-memory one.
	if p.MaxRunDuration%time.Millisecond != 0 {
		return errInvalidf("max run duration must use millisecond precision")
	}
	if p.RecoveryGrace%time.Millisecond != 0 {
		return errInvalidf("recovery grace must use millisecond precision")
	}
	return nil
}

// DefaultRunPolicy is a usable starting point for a Binding that has not
// configured its own. It is not a fallback applied inside a Store: Accept
// requires an explicit policy, so a caller that forgot to set one fails loudly
// instead of silently inheriting numbers it never chose.
func DefaultRunPolicy() RunPolicy {
	return RunPolicy{
		MaxAttempts:    3,
		MaxRunDuration: 5 * time.Minute,
		RecoveryGrace:  30 * time.Second,
	}
}

// InboxMessage is one accepted external event as stored. It is the only record
// in this package that holds the full third-party payload, and it is returned
// only from tenant-scoped reads.
type InboxMessage struct {
	TenantID         string
	InboxID          string
	Channel          ChannelType
	ChannelBindingID string
	AgentAppID       string
	PrincipalID      string
	SessionID        string
	ExternalEventID  string
	ReceivedAt       time.Time
	AcceptedAt       time.Time
	Message          InboundMessage
	DeliveryTarget   DeliveryTarget
}

// SessionKey is the conversation this event belongs to.
func (m InboxMessage) SessionKey() sessiondir.Key {
	return sessiondir.Key{
		TenantID:    m.TenantID,
		AppID:       m.AgentAppID,
		PrincipalID: m.PrincipalID,
		SessionID:   m.SessionID,
	}
}

// Clone returns a copy that shares nothing mutable with m.
func (m InboxMessage) Clone() InboxMessage {
	copied := m
	copied.Message = m.Message.Clone()
	copied.DeliveryTarget = m.DeliveryTarget.Clone()
	return copied
}

// RunStats is the non-sensitive accounting a finished Run keeps. Everything in
// it is a count or a duration; none of it can carry content.
type RunStats struct {
	// EventCount is how many Events the execution appended.
	EventCount int32
	// OutputParts is how many Outbox parts the answer became.
	OutputParts int32
	// ExecutionMillis is wall-clock execution time.
	ExecutionMillis int64
}

// Validate rejects impossible accounting.
func (s RunStats) Validate() error {
	if s.EventCount < 0 || s.OutputParts < 0 || s.ExecutionMillis < 0 {
		return errInvalidf("run stats cannot be negative")
	}
	return nil
}

// Run is the unit of execution: exactly one per accepted inbox message, created
// in the same transaction as that message and sharing its request id.
type Run struct {
	TenantID  string
	RunID     string
	RequestID string
	InboxID   string

	Channel          ChannelType
	ChannelBindingID string
	AgentAppID       string
	PrincipalID      string
	SessionID        string

	// AcceptSequence orders Runs within one Session. It is assigned under a
	// transaction-level lock on the Session key at accept time, and it is what
	// "strictly in order per Session" is defined against — not received_at,
	// which comes from a third party, and not created_at, which is a clock.
	AcceptSequence int64

	Status RunStatus
	// Attempt counts claims, not executions: it is incremented atomically by
	// the claim itself, so a Worker that dies before doing anything still
	// consumes one. That is the conservative direction — the alternative
	// counts nothing when a Worker dies mid-execution and retries forever.
	Attempt int32
	Policy  RunPolicy

	// NextAttemptAt gates claiming. A Run in the past is due; a Run in the
	// future is in backoff and, because ordering is strict, is also holding
	// back every later Run in its Session.
	NextAttemptAt time.Time
	// LastDispatchedAt records when a wakeup was last published for this Run.
	// It exists so the dispatcher can avoid re-publishing a wakeup every scan.
	LastDispatchedAt *time.Time

	// The claim fields are set together by ClaimNextRun and cleared together
	// when the Run returns to accepted. ClaimToken is the fencing value every
	// later write must present.
	ClaimToken        string
	ClaimedBy         string
	ClaimedAt         *time.Time
	ExecuteDeadlineAt *time.Time
	RecoverAfter      *time.Time

	// ExecutionStartedAt is written just before the Runner is called and is the
	// dividing line between "nothing happened yet, a yield is free" and "there
	// may be Events, a retry has to reconcile". It belongs to the *current*
	// attempt and is cleared with the rest of the claim, so the next attempt
	// starts able to yield again.
	ExecutionStartedAt *time.Time

	// FirstExecutionStartedAt is when any attempt of this Run first called the
	// Runner. It is written by the same MarkRunStarted that sets
	// ExecutionStartedAt and is then never moved and never cleared — not by a
	// claim, a yield, a recovery, a finish or an exhaustion.
	//
	// The two are separate because they answer different questions. "May this
	// attempt still be yielded?" is about this attempt and has to be re-answered
	// as yes after a recovery, which is why ExecutionStartedAt is cleared.
	// "Did this request ever reach the model?" is about the request, is what an
	// operator asks when a user reports a duplicate reply, and cannot be
	// reconstructed afterwards: the attempt counter counts claims, so a Worker
	// that died before starting looks exactly like one that died after.
	FirstExecutionStartedAt *time.Time

	// RevisionID is the revision that actually executed, recorded once the Pin
	// is resolved. It is stored separately from the Session pin because a Run
	// is answered by the revision that was pinned when it ran.
	RevisionID string
	ErrorType  ErrorType
	Stats      RunStats
	FinishedAt *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}

// SessionKey is the conversation this Run belongs to.
func (r Run) SessionKey() sessiondir.Key {
	return sessiondir.Key{
		TenantID:    r.TenantID,
		AppID:       r.AgentAppID,
		PrincipalID: r.PrincipalID,
		SessionID:   r.SessionID,
	}
}

// AttemptsExhausted reports whether every attempt in the budget has been used.
// A Run in this state must never be left in or returned to accepted; see
// ClaimNextRun, YieldRun and RecoverRuns, each of which terminates it instead.
func (r Run) AttemptsExhausted() bool { return r.Attempt >= r.Policy.MaxAttempts }

// Clone returns a copy that shares no pointer with r.
func (r Run) Clone() Run {
	copied := r
	copied.ClaimedAt = cloneTime(r.ClaimedAt)
	copied.ExecuteDeadlineAt = cloneTime(r.ExecuteDeadlineAt)
	copied.RecoverAfter = cloneTime(r.RecoverAfter)
	copied.ExecutionStartedAt = cloneTime(r.ExecutionStartedAt)
	copied.FirstExecutionStartedAt = cloneTime(r.FirstExecutionStartedAt)
	copied.LastDispatchedAt = cloneTime(r.LastDispatchedAt)
	copied.FinishedAt = cloneTime(r.FinishedAt)
	return copied
}

// OutboxPart is one message on its way back to a channel. A Run's answer may be
// several parts because channels cap message length, and each part is
// independently claimed, sent, retried and deduplicated.
type OutboxPart struct {
	TenantID  string
	OutboxID  string
	RunID     string
	RequestID string

	Channel          ChannelType
	ChannelBindingID string
	SessionID        string

	// PartNo orders the parts of one answer, from zero.
	PartNo int32
	// IdempotencyKey is derived from RequestID and PartNo by the Store, and is
	// unique per (tenant, binding). It is what makes "the Worker retried and
	// produced the same answer again" insert nothing rather than send twice.
	IdempotencyKey string
	// ClientMessageID is the stable id handed to the channel for its own
	// deduplication. It is separate from IdempotencyKey because it crosses a
	// trust boundary: the channel sees it, and it must stay identical across
	// send retries of the same part.
	ClientMessageID string

	Status        OutboxStatus
	Attempt       int32
	MaxAttempts   int32
	NextAttemptAt time.Time
	// LastDispatchedAt mirrors Run.LastDispatchedAt.
	LastDispatchedAt *time.Time

	// SendToken fences a send exactly as ClaimToken fences an execution.
	SendToken      string
	SentBy         string
	SendDeadlineAt *time.Time

	// DuplicateRisk records that some earlier attempt may have been delivered
	// even though it was not confirmed. It is sticky: once a part has been sent
	// with an unknown outcome, every later attempt carries the same warning,
	// because the risk does not go away by being retried.
	DuplicateRisk bool
	ErrorType     ErrorType
	// ExternalMessageID is the channel's own id for the delivered message.
	ExternalMessageID string
	SentAt            *time.Time

	Message        OutboundMessage
	DeliveryTarget DeliveryTarget

	CreatedAt time.Time
	UpdatedAt time.Time
}

// AttemptsExhausted reports whether every send attempt has been used.
func (p OutboxPart) AttemptsExhausted() bool { return p.Attempt >= p.MaxAttempts }

// Clone returns a copy that shares nothing mutable with p.
func (p OutboxPart) Clone() OutboxPart {
	copied := p
	copied.SendDeadlineAt = cloneTime(p.SendDeadlineAt)
	copied.LastDispatchedAt = cloneTime(p.LastDispatchedAt)
	copied.SentAt = cloneTime(p.SentAt)
	copied.Message = p.Message.Clone()
	copied.DeliveryTarget = p.DeliveryTarget.Clone()
	return copied
}

// IdempotencyKeyFor derives an Outbox part's idempotency key.
//
// It is built from the request id and the part number rather than from the
// content, so a retry that produces a *different* answer for the same part
// still collides and still sends once. Content-derived keys have the opposite
// failure mode: a model that phrases its answer differently on retry would
// produce a new key and a second message.
//
// It is exported because both Stores and the conformance suite have to agree on
// it exactly; a Store that derived it privately could not be checked.
func IdempotencyKeyFor(requestID string, partNo int32) string {
	return requestID + ":" + strconv.FormatInt(int64(partNo), 10)
}

// MaxOutboxParts bounds how many parts one Run's answer may become. It is a
// guard against an agent that emits an unbounded stream being turned into an
// unbounded number of sends.
const MaxOutboxParts = 64

// MinOutboxAttempts and MaxOutboxAttempts bound a part's send budget.
const (
	MinOutboxAttempts int32 = 1
	MaxOutboxAttempts int32 = 16
)

// OutboxDraft is one part of an answer, as handed to FinishRun. It carries only
// what the Worker decides; the Store fills in identity, ordering, status and
// scheduling so that a Worker cannot construct a part that is already claimed.
type OutboxDraft struct {
	// OutboxID is pre-minted by the caller, like every other id in this
	// package, so that a retried Finish addresses the same row.
	OutboxID string
	// PartNo is the position of this part in the answer, from zero.
	PartNo int32
	// ClientMessageID is the stable id shown to the channel.
	ClientMessageID string
	// MaxAttempts is this part's send budget.
	MaxAttempts int32
	Message     OutboundMessage
	// DeliveryTarget is normally copied from the originating inbox message.
	DeliveryTarget DeliveryTarget
}

// Validate rejects a draft that could not become a sendable part.
func (d OutboxDraft) Validate() error {
	if err := tenant.ValidateResourceID("outbox id", d.OutboxID); err != nil {
		return err
	}
	if d.PartNo < 0 || d.PartNo >= MaxOutboxParts {
		return errInvalidf("part number must be between 0 and %d", MaxOutboxParts-1)
	}
	if err := tenant.ValidateResourceID("client message id", d.ClientMessageID); err != nil {
		return err
	}
	if d.MaxAttempts < MinOutboxAttempts || d.MaxAttempts > MaxOutboxAttempts {
		return errInvalidf(
			"outbox max attempts must be between %d and %d", MinOutboxAttempts, MaxOutboxAttempts)
	}
	if err := d.Message.Validate(); err != nil {
		return err
	}
	return d.DeliveryTarget.Validate()
}

// Clone returns a copy that shares nothing mutable with d.
func (d OutboxDraft) Clone() OutboxDraft {
	copied := d
	copied.Message = d.Message.Clone()
	copied.DeliveryTarget = d.DeliveryTarget.Clone()
	return copied
}

// validateSessionKey applies the ordinary resource-id rules to a Session key
// and checks it against the caller's tenant scope.
func validateSessionKey(scope tenant.TenantContext, key sessiondir.Key) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	if err := key.Validate(); err != nil {
		return err
	}
	if key.TenantID != scope.TenantID {
		return fmt.Errorf(
			"%w: session key tenant %q does not match %q",
			tenant.ErrTenantScope, key.TenantID, scope.TenantID)
	}
	return nil
}
