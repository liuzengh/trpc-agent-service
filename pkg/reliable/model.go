//
// Tencent is pleased to support the open source community by making
// trpc-agent-go available.
//
// Copyright (C) 2025 Tencent. All rights reserved.
//
// trpc-agent-go is licensed under the Apache License Version 2.0.

// Package reliable owns the platform's single durable Inbox/Outbox model.
package reliable

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	summarycoord "trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/summary"
)

// InboxStatus is the durable execution state of an inbound message.
type InboxStatus string

const (
	InboxReceived   InboxStatus = "RECEIVED"
	InboxProcessing InboxStatus = "PROCESSING"
	InboxRetryWait  InboxStatus = "RETRY_WAIT"
	// InboxWaitingApproval is an operator-gated pause. Claims from this state
	// must not consume the normal transient-attempt budget because the Worker
	// has not started the dangerous tool yet.
	InboxWaitingApproval InboxStatus = "WAITING_APPROVAL"
	// InboxWaitingReconciliation is terminal for automatic consumers. An
	// operator must reconcile the execution outcome and explicitly replay it.
	InboxWaitingReconciliation InboxStatus = "WAITING_RECONCILIATION"
	InboxCompleted             InboxStatus = "COMPLETED"
	InboxDeadLetter            InboxStatus = "DEAD_LETTERED"
)

// OutboxStatus is the durable delivery state of an Agent reply.
type OutboxStatus string

const (
	OutboxPending    OutboxStatus = "REPLY_PENDING"
	OutboxDelivering OutboxStatus = "DELIVERING"
	// OutboxDispatchStarted is the durable fence immediately before a provider
	// call may cross its side-effect boundary. Expired rows in this state require
	// reconciliation, never an automatic resend.
	OutboxDispatchStarted OutboxStatus = "DISPATCH_STARTED"
	OutboxRetryWait       OutboxStatus = "RETRY_WAIT"
	// OutboxWaitingReconciliation is terminal for automatic delivery. An
	// operator must reconcile the provider/account state before replaying it.
	OutboxWaitingReconciliation OutboxStatus = "WAITING_RECONCILIATION"
	OutboxDelivered             OutboxStatus = "REPLIED"
	OutboxDeadLetter            OutboxStatus = "DEAD_LETTERED"
)

// OutboxReplayMode controls whether an operator resumes after the last
// durably acknowledged chunk or deliberately redelivers the whole reply.
// Callers must make this choice explicit so a recovery action cannot silently
// duplicate chunks that users have already received.
type OutboxReplayMode string

const (
	// OutboxReplayResume keeps DeliveryCursor and continues at the first chunk
	// that has not been durably acknowledged.
	OutboxReplayResume OutboxReplayMode = "resume"
	// OutboxReplayRestart resets DeliveryCursor and redelivers from chunk zero.
	OutboxReplayRestart OutboxReplayMode = "restart"
)

var (
	// ErrNoWork means there is currently no claimable message.
	ErrNoWork = errors.New("no claimable message")
	// ErrStaleLease means a worker tried to mutate state after losing ownership.
	ErrStaleLease = errors.New("stale or expired lease")
	// ErrIdempotencyConflict means the same source identifier was reused with a
	// different immutable payload or routing identity. Silently treating it as
	// a duplicate would lose data or attach work to the wrong session.
	ErrIdempotencyConflict = errors.New("idempotency key reused with different payload")
	// ErrInvalidTransition means the requested state transition is not allowed.
	ErrInvalidTransition = errors.New("invalid state transition")
	// ErrOutboxConflict means an Inbox completion found an existing Outbox row.
	// This indicates durable state that requires reconciliation; treating it as
	// an ordinary transient error would leave the Inbox in PROCESSING and make
	// the same session's FIFO stream retry forever.
	ErrOutboxConflict = errors.New("outbox completion conflict")
	// ErrSummaryCompletionConflict means a Worker-provided summary receipt does
	// not match the authoritative Inbox/app/session identity. Replaying model
	// execution cannot repair it; an operator must reconcile the mixed state.
	ErrSummaryCompletionConflict = errors.New("summary completion conflict")
	// ErrTenantInactive means ingress lost its active-tenant authorization
	// before the durable Inbox transaction committed. Providers should be
	// acknowledged without scheduling Agent execution.
	ErrTenantInactive = errors.New("tenant is not active")
	// ErrStoreUnavailable means a durable store has not been initialized with a
	// usable backend. It is returned instead of allowing a malformed or typed-
	// nil store to panic inside an application boundary.
	ErrStoreUnavailable = errors.New("reliable store unavailable")
	// ErrInvalidInboxMessage identifies caller data that cannot be persisted in
	// the durable Inbox. It is permanent and must not be retried by a provider.
	ErrInvalidInboxMessage = errors.New("inbox message is invalid")
	// ErrApprovalDeadlineInvalid means a Worker supplied an approval deadline
	// that is not valid according to the durable store's clock. Consumers must
	// not turn this protocol violation into an unbounded retry loop.
	ErrApprovalDeadlineInvalid = errors.New("approval deadline is invalid")
	// ErrInvalidExpiredWorkReapBatchSize rejects a maintenance request that
	// would either make no progress or make one transaction unbounded.
	ErrInvalidExpiredWorkReapBatchSize = errors.New("expired work reaper batch size is invalid")
	// ErrInvalidTenantQueuePolicy identifies an operator queue policy that
	// cannot be applied atomically by a fair claimer.
	ErrInvalidTenantQueuePolicy = errors.New("tenant queue policy is invalid")
	// ErrFairQueueNotReady means the durable schema required by the optional
	// fair claimer has not been installed or is not discoverable.
	ErrFairQueueNotReady = errors.New("fair queue schema is not ready")
	// ErrFairQueueUnavailable means an operator enabled fair scheduling on a
	// store that does not implement the optional durable capability.
	ErrFairQueueUnavailable = errors.New("fair inbox queue is unavailable")
	// ErrTenantQueueFull means an operator-owned tenant backlog limit rejected
	// admission before the message consumed a session sequence number.
	ErrTenantQueueFull = errors.New("tenant queue is full")
)

// MaxApprovalWait bounds a durable operator-gated pause. The bound is enforced
// by the Store using its authoritative clock, rather than by a Consumer node's
// wall clock, so ordinary cross-node clock skew cannot reject a valid approval.
const MaxApprovalWait = 24 * time.Hour

// MaxExpiredWorkReapBatchSize limits one Inbox and one Outbox maintenance
// transaction independently. A production scheduler should run small, regular
// batches instead of turning a Claim poll into a global cleanup scan.
const MaxExpiredWorkReapBatchSize = 1000

// ErrLeaseExpiredAfterFinalAttempt is recorded when a claimed message reaches
// its retry limit and its lease expires before the worker can persist a
// terminal result. Keeping this reason stable across stores makes DLQ evidence
// and replay tooling consistent in production and local tests.
var ErrLeaseExpiredAfterFinalAttempt = errors.New("lease expired after final attempt")

// CurrentInboxRoutingVersion is the durable routing schema understood by the
// current Gateway and Consumer. Version zero is reserved for rows written by
// older binaries before the authoritative group/session columns existed.
const CurrentInboxRoutingVersion = 1

// Lease is the ownership proof returned by a claim. Fence is monotonically
// incremented by PostgreSQL on every claim and must accompany every commit.
type Lease struct {
	Owner string
	Fence int64
	Until time.Time
}

// inboxIdentityMatches reports whether a duplicate provider delivery describes
// the same immutable work item. The uniqueness key already covers tenant,
// channel, account and provider message ID, but checking them here keeps the
// in-memory and SQL implementations defensive if their keying rules evolve.
// TraceParent and retry policy are deliberately excluded: they are per-attempt
// metadata and may legitimately differ on a redelivery.
func inboxIdentityMatches(existing, candidate *InboxMessage) bool {
	if existing == nil || candidate == nil {
		return false
	}
	return existing.TenantID == candidate.TenantID &&
		existing.ChannelType == candidate.ChannelType &&
		existing.ChannelAccountID == candidate.ChannelAccountID &&
		existing.ExternalMessageID == candidate.ExternalMessageID &&
		existing.AgentApp == candidate.AgentApp &&
		existing.ConversationID == candidate.ConversationID &&
		existing.ReplyToID == candidate.ReplyToID &&
		existing.UserID == candidate.UserID &&
		existing.SessionID == candidate.SessionID &&
		strings.EqualFold(existing.PayloadHash, candidate.PayloadHash) &&
		// During a rolling migration one side of a duplicate comparison can be a
		// legacy row without routing columns. The immutable payload hash still
		// protects that comparison; compare the new fields whenever both records
		// are authoritative so a changed group/direct interpretation is rejected.
		(existing.RoutingVersion == 0 || candidate.RoutingVersion == 0 ||
			(existing.RoutingVersion == candidate.RoutingVersion &&
				existing.IsGroupChat == candidate.IsGroupChat &&
				existing.SessionOwnerID == candidate.SessionOwnerID))
}

// InboxMessage is the canonical representation of accepted IM work.
type InboxMessage struct {
	ID                int64
	TenantID          string
	ChannelType       string
	ChannelAccountID  string
	AgentApp          string
	ExternalMessageID string
	ConversationID    string
	// ReplyToID is the provider-native message identifier used for reply
	// threading. It is captured at trusted ingress and copied to Outbox by the
	// Store; workers cannot override it during completion.
	ReplyToID string
	UserID    string
	SessionID string
	// IsGroupChat and SessionOwnerID are persisted routing decisions. For
	// RoutingVersion >= 1 they are authoritative and must be copied unchanged
	// into every Consumer request. Version-zero rows are legacy and require the
	// original payload to prove their routing identity before replay.
	IsGroupChat    bool
	SessionOwnerID string
	RoutingVersion int
	// SessionSequence is allocated monotonically within the durable
	// (tenant, agent app, session) stream. A later message is not claimable
	// until every earlier sequence is completed.
	SessionSequence int64
	PayloadHash     string
	Payload         []byte
	TraceParent     string
	Status          InboxStatus
	AttemptCount    int
	MaxAttempts     int
	NextAttemptAt   *time.Time
	// ApprovalDeadline bounds an operator-gated WAITING_APPROVAL pause. It is
	// populated only while that state is active and is cleared on normal retry,
	// completion, replay, or terminal failure.
	ApprovalDeadline *time.Time
	Lease            Lease
	LastError        string
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// OutboxReply is the only worker-controlled input accepted while completing
// an Inbox message. Delivery routing is deliberately absent: the Store derives
// tenant, channel, account, conversation and reply target from the leased
// Inbox row so a compromised worker cannot forge a cross-tenant reply route.
type OutboxReply struct {
	ContentType string
	Content     string
	TraceParent string
}

// ReplayQueue identifies the durable queue changed by an operator replay.
type ReplayQueue string

const (
	ReplayQueueInbox  ReplayQueue = "inbox"
	ReplayQueueOutbox ReplayQueue = "outbox"
)

// ReplayAuditRecord is the immutable evidence for an operator-triggered
// replay. PostgreSQL persists the same fields in message_replay_audit.
type ReplayAuditRecord struct {
	Queue       ReplayQueue
	MessageID   int64
	TenantID    string
	RequestedBy string
	Reason      string
	Mode        OutboxReplayMode
	CreatedAt   time.Time
}

// OutboxMessage is the canonical representation of a reply awaiting delivery.
type OutboxMessage struct {
	ID               int64
	InboxID          int64
	TenantID         string
	AgentApp         string
	SessionID        string
	SessionSequence  int64
	ChannelType      string
	ChannelAccountID string
	ConversationID   string
	ReplyToID        string
	ContentType      string
	Content          string
	TraceParent      string
	DeliveryCursor   int
	Status           OutboxStatus
	AttemptCount     int
	MaxAttempts      int
	NextAttemptAt    *time.Time
	Lease            Lease
	LastError        string
	DeliveredAt      *time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

// Store is the single persistence boundary shared by Gateway, Consumer and
// Delivery. Implementations must make claims and fenced mutations atomic.
type Store interface {
	EnqueueInbox(ctx context.Context, msg *InboxMessage) (inserted bool, err error)
	ClaimInbox(ctx context.Context, owner string, leaseDuration time.Duration) (*InboxMessage, error)
	RenewInbox(ctx context.Context, id int64, lease Lease, leaseDuration time.Duration) (Lease, error)
	CompleteInbox(ctx context.Context, id int64, lease Lease, reply OutboxReply) (*OutboxMessage, error)
	RetryInbox(ctx context.Context, id int64, lease Lease, cause error, nextAttempt time.Time) error
	BlockInbox(ctx context.Context, id int64, lease Lease, cause error) error
	DeadLetterInbox(ctx context.Context, id int64, lease Lease, cause error) error
	ReplayInbox(ctx context.Context, id int64, actor, reason string) error

	ClaimOutbox(ctx context.Context, owner string, leaseDuration time.Duration) (*OutboxMessage, error)
	RenewOutbox(ctx context.Context, id int64, lease Lease, leaseDuration time.Duration) (Lease, error)
	// MarkDelivered is valid only after MarkDispatchStarted has fenced the
	// provider side-effect boundary for this lease.
	MarkDelivered(ctx context.Context, id int64, lease Lease) error
	// AdvanceOutbox is valid only after MarkDispatchStarted has fenced the
	// provider side-effect boundary for this lease.
	AdvanceOutbox(ctx context.Context, id int64, lease Lease, nextCursor int) error
	RetryOutbox(ctx context.Context, id int64, lease Lease, cause error, nextAttempt time.Time, cursor int) error
	DeadLetterOutbox(ctx context.Context, id int64, lease Lease, cause error) error
	// ReplayOutbox resumes after the last durably acknowledged delivery chunk.
	// It is the safe default for a dead-letter recovery.
	ReplayOutbox(ctx context.Context, id int64, actor, reason string) error

	PingContext(ctx context.Context) error
	Close() error
}

// TenantScopedOutboxReplayer performs replay with an atomic tenant predicate.
// Administrative callers must use this capability to prevent cross-tenant ID
// substitution between authorization and the durable state transition.
type TenantScopedOutboxReplayer interface {
	ReplayOutboxForTenant(ctx context.Context, tenantID string, id int64, actor, reason string) error
}

// SummaryInboxCompleter atomically commits a successful Inbox transition, its
// Outbox reply and the derived Summary job. A Consumer must fail closed when a
// Worker returns a summary receipt but the configured store lacks this seam.
type SummaryInboxCompleter interface {
	CompleteInboxWithSummary(
		context.Context,
		int64,
		Lease,
		OutboxReply,
		summarycoord.EnqueueRequest,
	) (*OutboxMessage, error)
}

// ExpiredWorkReapResult describes terminal transitions made by one bounded
// maintenance pass. Each ReapExpired invocation changes at most its requested
// batch size of Inbox rows in total and at most that many Outbox rows.
type ExpiredWorkReapResult struct {
	InboxFinalAttemptExpired  int
	InboxApprovalExpired      int
	OutboxFinalAttemptExpired int
	OutboxDispatchUnknown     int
}

// ExpiredWorkReaper is an optional maintenance seam. It intentionally stays
// outside Store so existing Store adapters remain source-compatible. Durable
// implementations must make the selection and terminal transition atomic and
// safe for multiple reaper replicas.
type ExpiredWorkReaper interface {
	ReapExpired(ctx context.Context, batchSize int) (ExpiredWorkReapResult, error)
}

// QueueStats is a read-only snapshot of automatic queue work. Depth includes
// received/processing/retry/approval Inbox rows and pending/delivering/
// dispatch-started/retry Outbox rows. Terminal and reconciliation rows are
// intentionally excluded: they require operator action rather than worker
// capacity. A zero Oldest value means that the corresponding queue is empty.
type QueueStats struct {
	InboxDepth   int64
	InboxOldest  time.Time
	OutboxDepth  int64
	OutboxOldest time.Time
	// ObservedAt is the clock reading used to measure the Oldest timestamps.
	// Durable stores should populate it from their authoritative clock so a
	// pipeline process with skewed wall time does not misstate queue age.
	ObservedAt time.Time
}

// QueueInspector is an optional read-only observation seam. It stays outside
// Store so existing adapters remain source-compatible while operators can use
// one contract for backlog dashboards and oldest-message alerts.
type QueueInspector interface {
	InspectQueue(ctx context.Context) (QueueStats, error)
}

// TenantQueuePolicy is operator-owned scheduling state. It is deliberately
// separate from TenantConfig: accepting these values from a tenant request
// would let a tenant buy or starve shared worker capacity. A zero limit means
// unlimited for that dimension; Weight must be positive.
type TenantQueuePolicy struct {
	TenantID    string
	Weight      int64
	MaxQueued   int64
	MaxInflight int64
}

// Validate checks the policy before it reaches a database or scheduler.
func (p TenantQueuePolicy) Validate() error {
	if p.TenantID == "" || len(p.TenantID) > 64 || strings.ContainsAny(p.TenantID, "\x00\r\n") {
		return fmt.Errorf("%w: tenant id is invalid", ErrInvalidTenantQueuePolicy)
	}
	if p.Weight <= 0 || p.Weight > 1_000_000 {
		return fmt.Errorf("%w: weight must be between 1 and 1000000", ErrInvalidTenantQueuePolicy)
	}
	if p.MaxQueued < 0 || p.MaxInflight < 0 {
		return fmt.Errorf("%w: limits cannot be negative", ErrInvalidTenantQueuePolicy)
	}
	return nil
}

// FairInboxClaimer is an optional durable claim capability. Implementations
// must select at most one session head in one transaction/critical section,
// enforce MaxInflight before issuing the lease, and update the tenant's fair
// scheduling state together with the claim. Store remains source-compatible
// for external adapters that do not opt into this contract.
type FairInboxClaimer interface {
	ClaimInboxFair(ctx context.Context, owner string, leaseDuration time.Duration) (*InboxMessage, error)
}

// FairInboxReadiness is an optional startup probe for durable fair-queue
// deployments. Built-in PostgreSQL uses it to fail closed before claiming
// work when migration 035 is missing; local stores may report ready directly.
type FairInboxReadiness interface {
	CheckFairInboxReady(ctx context.Context) error
}

// TenantQueuePolicyStore is the operator control-plane seam for fair queue
// policy. It is intentionally not part of Store so existing adapters can be
// upgraded independently of normal message processing.
type TenantQueuePolicyStore interface {
	UpsertTenantQueuePolicy(ctx context.Context, policy TenantQueuePolicy) error
	// DeleteTenantQueuePolicy restores the tenant's unlimited weight-one
	// default. Implementations must retain a schedule row so existing backlog
	// remains eligible for fair claiming.
	DeleteTenantQueuePolicy(ctx context.Context, tenantID string) error
}

// QueueAdmissionStore is an optional atomic ingress capability. The check and
// Inbox insert must share the store's tenant serialization boundary; callers
// must not implement this as a read-then-EnqueueInbox sequence.
type QueueAdmissionStore interface {
	EnqueueInboxWithAdmission(ctx context.Context, msg *InboxMessage) (inserted bool, err error)
}

// selectInboxByProviderKeyQuery returns the canonical durable Inbox row for
// an idempotent provider event. Keep this query shared by the normal conflict
// path and the queue-full duplicate fast path so both paths validate exactly
// the same routing identity.
const selectInboxByProviderKeyQuery = `
	SELECT id, tenant_id, channel_type, channel_account_id,
	       external_message_id, agent_app_name, conversation_id, reply_to_id,
	       user_id, session_id, is_group_chat, session_owner_id,
	       routing_version, session_sequence, payload_hash, payload,
	       trace_parent, status, attempt_count, max_attempts, next_attempt_at,
	       approval_deadline,
	       lease_owner, lease_version, lease_until, last_error, created_at, updated_at
	FROM inbox_messages
	WHERE tenant_id=$1 AND channel_type=$2 AND channel_account_id=$3
	  AND external_message_id=$4`

// ValidateExpiredWorkReapBatchSize keeps each maintenance transaction bounded.
// Exporting the validation lets deployment entry points reject bad environment
// configuration before starting background work.
func ValidateExpiredWorkReapBatchSize(batchSize int) error {
	if batchSize < 1 || batchSize > MaxExpiredWorkReapBatchSize {
		return fmt.Errorf("%w: must be between 1 and %d", ErrInvalidExpiredWorkReapBatchSize, MaxExpiredWorkReapBatchSize)
	}
	return nil
}

// RetryAfterStore is an optional clock-safe retry seam. Distributed stores
// should calculate the retry deadline from their authoritative clock inside
// the fenced update (PostgreSQL uses clock_timestamp()). Keeping this optional preserves
// source compatibility for external Store implementations; the in-tree
// PostgreSQL and memory stores both implement it.
type RetryAfterStore interface {
	RetryInboxAfter(ctx context.Context, id int64, lease Lease, cause error, delay time.Duration) error
	RetryOutboxAfter(ctx context.Context, id int64, lease Lease, cause error, delay time.Duration, cursor int) error
}

// ApprovalWaitStore is an optional durable seam for the 428 approval path.
// Implementations move a leased Inbox row to WAITING_APPROVAL without
// incrementing its attempt budget. Stores that do not implement this seam are
// handled conservatively by the pipeline as an audited reconciliation block.
type ApprovalWaitStore interface {
	WaitInboxApproval(ctx context.Context, id int64, lease Lease, cause error, delay time.Duration, deadline time.Time) error
}

// OutboxRestartStore is an optional operator seam for deliberately
// redelivering an Outbox message from chunk zero. Keeping this destructive
// recovery operation out of Store preserves compatibility for ordinary Store
// adapters and forces operator tooling to opt in explicitly.
type OutboxRestartStore interface {
	RestartOutbox(ctx context.Context, id int64, actor, reason string) error
}

// OutboxBlocker is an optional capability for stores that persist the
// reconciliation state. Keeping it optional preserves source compatibility for
// external Store implementations; the delivery pipeline falls back to a
// dead-letter transition when the capability is unavailable.
type OutboxBlocker interface {
	BlockOutbox(ctx context.Context, id int64, lease Lease, cause error) error
}

// OutboxDispatchFence is the durable pre-dispatch fence. Delivery must call
// it after claiming and before invoking a provider.
type OutboxDispatchFence interface {
	MarkDispatchStarted(ctx context.Context, id int64, lease Lease) error
}

// ValidateLeaseOwner validates the exact value persisted in lease_owner.
// The database column is bounded to 256 bytes and the value is also emitted
// in operational logs, so malformed UTF-8 and control/format characters are
// rejected before a claim opens a transaction.
func ValidateLeaseOwner(owner string) error {
	if owner == "" {
		return fmt.Errorf("lease owner is required")
	}
	if !utf8.ValidString(owner) {
		return fmt.Errorf("lease owner must be valid UTF-8")
	}
	if len(owner) > 256 {
		return fmt.Errorf("lease owner exceeds 256 bytes")
	}
	for _, r := range owner {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return fmt.Errorf("lease owner contains control or format character")
		}
	}
	return nil
}

func validateOutboxReplayMode(mode OutboxReplayMode) error {
	switch mode {
	case OutboxReplayResume, OutboxReplayRestart:
		return nil
	default:
		return fmt.Errorf("replay outbox: unsupported mode %q", mode)
	}
}

// ValidateInboxTransition rejects accidental state-machine bypasses.
func ValidateInboxTransition(from, to InboxStatus) error {
	allowed := map[InboxStatus]map[InboxStatus]bool{
		InboxReceived:              {InboxProcessing: true},
		InboxProcessing:            {InboxCompleted: true, InboxRetryWait: true, InboxWaitingApproval: true, InboxWaitingReconciliation: true, InboxDeadLetter: true},
		InboxRetryWait:             {InboxProcessing: true},
		InboxWaitingApproval:       {InboxProcessing: true},
		InboxWaitingReconciliation: {InboxReceived: true},
		InboxCompleted:             {},
		InboxDeadLetter:            {InboxReceived: true},
	}
	toSet, ok := allowed[from]
	if !ok || !toSet[to] {
		return fmt.Errorf("%w: inbox %s -> %s", ErrInvalidTransition, from, to)
	}
	return nil
}

// ValidateOutboxTransition rejects accidental delivery-state bypasses.
func ValidateOutboxTransition(from, to OutboxStatus) error {
	allowed := map[OutboxStatus]map[OutboxStatus]bool{
		OutboxPending:               {OutboxDelivering: true},
		OutboxDelivering:            {OutboxDispatchStarted: true, OutboxRetryWait: true, OutboxWaitingReconciliation: true, OutboxDeadLetter: true},
		OutboxDispatchStarted:       {OutboxDelivered: true, OutboxRetryWait: true, OutboxWaitingReconciliation: true, OutboxDeadLetter: true},
		OutboxRetryWait:             {OutboxDelivering: true},
		OutboxWaitingReconciliation: {OutboxPending: true},
		OutboxDelivered:             {},
		OutboxDeadLetter:            {OutboxPending: true},
	}
	toSet, ok := allowed[from]
	if !ok || !toSet[to] {
		return fmt.Errorf("%w: outbox %s -> %s", ErrInvalidTransition, from, to)
	}
	return nil
}

// Backoff returns bounded exponential retry delay with deterministic caller-
// supplied jitter. Deterministic jitter makes failure tests reproducible.
func Backoff(attempt int, base, maximum, jitter time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if base <= 0 {
		base = time.Second
	}
	if maximum < base {
		maximum = base
	}
	delay := base
	for i := 1; i < attempt && delay < maximum; i++ {
		if delay > maximum/2 {
			delay = maximum
			break
		}
		delay *= 2
	}
	if delay > maximum {
		delay = maximum
	}
	if jitter > 0 && delay+jitter <= maximum {
		delay += jitter
	}
	return delay
}

// DeterministicJitter spreads retries without requiring shared random state.
// The same message remains reproducible in tests while different IDs avoid a
// synchronized retry storm.
func DeterministicJitter(messageID int64, maximum time.Duration) time.Duration {
	if maximum <= 0 {
		return 0
	}
	value := uint64(messageID)*1103515245 + 12345
	return time.Duration(value % uint64(maximum))
}
