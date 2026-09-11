// Package store defines the durable Inbox/Outbox contract that lets the
// service ACK IM callbacks only after the message is persisted, recover
// in-flight work after a crash, and retry replies independently of Agent
// execution.
package store

import (
	"context"
	"errors"
	"time"
	"unicode"

	"github.com/cyl6/trpc-agent-service/trpcservice/delivery"
	"github.com/jackc/pgx/v5"
)

// ErrDuplicate is returned by InsertInbox when the exact message (by dedup
// key) has already been persisted. Callers treat it as success: the platform
// redelivery is acknowledged without reprocessing.
var ErrDuplicate = errors.New("store: duplicate inbox message")

// ErrLeaseLost is returned when a completion or retry no longer matches the
// lease owner, e.g. the lease expired and another worker reclaimed the row.
// The caller must stop touching the record; the new owner owns it.
var ErrLeaseLost = errors.New("store: lease ownership lost")

// ErrNotFound is returned when a leased record disappeared (e.g. truncated
// by an operator). The caller should log and move on.
var ErrNotFound = errors.New("store: record not found")

// ErrOperationConflict means a deterministic operation identity was reused
// with different immutable routing, part metadata, or payload bytes.
var ErrOperationConflict = errors.New("store: delivery operation conflict")

// ErrInboxIdentityMismatch indicates that a transaction participant presented
// routing or attempt metadata that differs from the authoritative leased row.
// This is a fail-closed corruption/configuration signal, not a retryable lease
// race.
var ErrInboxIdentityMismatch = errors.New("store: inbox identity fence mismatch")

// ErrInvalidTransition is returned when an operation is no longer in the
// state/version an attempt or operator expected.
var ErrInvalidTransition = errors.New("store: invalid delivery transition")

// Inbox statuses. A message moves received -> processing -> processed, with
// retry re-arming next_attempt_at and dead_letter as the terminal failure.
const (
	InboxReceived   = "received"
	InboxProcessing = "processing"
	InboxProcessed  = "processed"
	InboxRetry      = "retry"
	InboxDead       = "dead_letter"
)

// Outbox statuses. pending -> sending -> sent, with retry/dead_letter sharing
// the inbox semantics.
const (
	OutboxPending = "pending"
	OutboxSending = "sending"
	OutboxSent    = "sent"
	OutboxRetry   = "retry"
	OutboxDead    = "dead_letter"
)

// Detailed delivery states coexist with the legacy scheduling status. An
// unknown provider result is encoded as status=sending with no lease, which
// makes old FIFO readers conservatively block the lane without reclaiming it.
const (
	DeliveryPending           = "pending"
	DeliveryInFlight          = "in_flight"
	DeliveryConfirmed         = "confirmed"
	DeliveryRetryableNotSent  = "retryable_not_sent"
	DeliveryPermanentRejected = "permanent_rejected"
	DeliveryUnknown           = "unknown"
	DeliveryRetryExhausted    = "retry_exhausted"
	DeliveryCanceled          = "canceled"
)

const (
	AttemptLeased     = "leased"
	AttemptDispatched = "dispatched"
	AttemptFinished   = "finished"
)

const (
	ResolveAssumeDelivered = "assume_delivered"
	ResolveRetry           = "retry"
	ResolveCancel          = "cancel"
)

// InboxRecord is one normalized inbound IM message awaiting Agent execution.
// Payload carries the full serialized worker task (including the tenant
// config snapshot) so replay after restart uses the same config revision.
type InboxRecord struct {
	InboxID           string `json:"inbox_id"`
	TenantID          string `json:"tenant_id"`
	ChannelType       string `json:"channel_type"`
	BindingID         string `json:"binding_id"`
	ExternalMessageID string `json:"external_message_id"`
	DedupKey          string `json:"dedup_key"`
	// PipelineSchemaVersion and atomic metadata make rolling upgrades
	// explicit. Zero/empty values identify backlog written before combined
	// Session+Inbox+Outbox commits were eligible.
	PipelineSchemaVersion int    `json:"pipeline_schema_version"`
	AtomicCommitMode      string `json:"atomic_commit_mode"`
	DatabaseIdentity      string `json:"database_identity,omitempty"`
	// PartitionKey is the full tenant/app/binding/channel/session identity.
	// Only the oldest non-terminal record in a partition may be leased, which
	// preserves transcript order while unrelated sessions run concurrently.
	PartitionKey  string            `json:"partition_key"`
	Payload       []byte            `json:"payload"`
	TraceCarrier  map[string]string `json:"trace_carrier,omitempty"`
	Status        string            `json:"status"`
	AttemptCount  int               `json:"attempt_count"`
	LastErrorType string            `json:"last_error_type,omitempty"`
}

// OutboxRecord is one outbound reply awaiting IM delivery. It is committed in
// the same transaction that marks the inbox message processed, so an Agent
// result is never lost between execution and delivery.
type OutboxRecord struct {
	OutboxID          string            `json:"outbox_id"`
	OperationKey      string            `json:"operation_key"`
	OperationVersion  int               `json:"operation_version"`
	PartIndex         int               `json:"part_index"`
	PartCount         int               `json:"part_count"`
	PayloadHash       string            `json:"payload_hash"`
	TenantID          string            `json:"tenant_id"`
	ChannelType       string            `json:"channel_type"`
	BindingID         string            `json:"binding_id"`
	DedupKey          string            `json:"dedup_key"`
	PartitionKey      string            `json:"partition_key"`
	Payload           []byte            `json:"payload"`
	TraceID           string            `json:"trace_id,omitempty"`
	TraceCarrier      map[string]string `json:"trace_carrier,omitempty"`
	Status            string            `json:"status"`
	DeliveryState     string            `json:"delivery_state"`
	StateVersion      int64             `json:"state_version"`
	AttemptCount      int               `json:"attempt_count"`
	AttemptPhase      string            `json:"attempt_phase,omitempty"`
	LastErrorType     string            `json:"last_error_type,omitempty"`
	ProviderCode      string            `json:"provider_code,omitempty"`
	ProviderMessageID string            `json:"provider_message_id,omitempty"`
	ProviderRequestID string            `json:"provider_request_id,omitempty"`
	ResponseHash      string            `json:"response_hash,omitempty"`
	LeaseExpiresAt    time.Time         `json:"-"`
}

// OutboxAttempt is the immutable history identity and current phase of one
// leased provider attempt. AttemptNo is fresh while OperationKey is stable.
type OutboxAttempt struct {
	OutboxID          string           `json:"outbox_id"`
	OperationKey      string           `json:"operation_key"`
	AttemptNo         int              `json:"attempt_no"`
	LeaseOwner        string           `json:"-"`
	Phase             string           `json:"phase"`
	Outcome           delivery.Outcome `json:"outcome,omitempty"`
	ErrorType         string           `json:"error_type,omitempty"`
	ProviderCode      string           `json:"provider_code,omitempty"`
	HTTPStatus        int              `json:"http_status,omitempty"`
	ProviderMessageID string           `json:"provider_message_id,omitempty"`
	ProviderRequestID string           `json:"provider_request_id,omitempty"`
	ResponseHash      string           `json:"response_hash,omitempty"`
	StartedAt         time.Time        `json:"started_at"`
	DispatchedAt      time.Time        `json:"dispatched_at,omitempty"`
	FinishedAt        time.Time        `json:"finished_at,omitempty"`
}

// ResolveRequest is a compare-and-swap decision for a parked unknown result.
// The caller explicitly accepts the semantics of assume/retry/cancel.
type ResolveRequest struct {
	ResolutionID    string
	OutboxID        string
	ExpectedVersion int64
	ExpectedAttempt int
	Action          string
	Actor           string
	Reason          string
	Now             time.Time
}

func validLedgerIdentifier(value string, maxBytes int) bool {
	if len(value) > maxBytes {
		return false
	}
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			continue
		}
		switch r {
		case '-', '_', '.', ':', '/':
			continue
		default:
			return false
		}
	}
	return true
}

// Store is the persistence contract for the durable pipeline. All lease-based
// methods fence on lease_owner: a worker whose lease expired gets
// ErrLeaseLost instead of silently overwriting the new owner's state. Durable
// implementations must evaluate due times and lease deadlines against their
// own authoritative clock; the now arguments keep the in-memory
// implementation deterministic. For retries, retryAt-now is the requested
// delay and may be rebased onto the durable store's clock.
type Store interface {
	// EnsureSchema creates the runtime tables if missing. It is idempotent.
	EnsureSchema(ctx context.Context) error
	Close() error

	// InsertInbox persists a message before ACK. Returns ErrDuplicate when the
	// dedup key already exists (platform redelivery) — not an error for the
	// caller.
	InsertInbox(ctx context.Context, rec *InboxRecord, now time.Time) error

	// LeaseInbox atomically claims up to limit runnable messages by moving
	// them to processing with the given owner and lease deadline. Each lease
	// increments attempt_count, so attempts are counted from the first run.
	// owner is an attempt token and must be globally fresh for every claim;
	// reusing it after completion or expiry would defeat lease fencing.
	LeaseInbox(ctx context.Context, owner string, now time.Time, leaseTTL time.Duration, limit int) ([]InboxRecord, error)

	// RenewInboxLease extends a still-valid processing lease. It must return
	// ErrLeaseLost when the owner changed or the previous deadline elapsed;
	// an expired attempt must never be resurrected.
	RenewInboxLease(ctx context.Context, inboxID, owner string, now time.Time, leaseTTL time.Duration) error

	// CompleteInbox marks a message processed and, when outbox is non-nil,
	// enqueues the reply in the same transaction. The pair is atomic: either
	// the reply is durably queued or the message stays retryable.
	CompleteInbox(ctx context.Context, inboxID, owner string, outbox *OutboxRecord, now time.Time) error

	// CompleteInboxBatch atomically inserts every deterministic delivery part
	// in slice order and completes the Inbox. Existing identical operations are
	// idempotent; mismatched reuse returns ErrOperationConflict.
	CompleteInboxBatch(ctx context.Context, inboxID, owner string, outboxes []OutboxRecord, now time.Time) error

	// RetryInbox re-arms a failed message; retryAt is now plus the backoff.
	RetryInbox(ctx context.Context, inboxID, owner, errType string, now, retryAt time.Time) error

	// DeadLetterInbox terminally parks a message that exhausted its attempts.
	DeadLetterInbox(ctx context.Context, inboxID, owner, errType string, now time.Time) error

	// LeaseOutbox claims pending replies for delivery. owner has the same
	// fresh-per-attempt requirement as LeaseInbox.
	LeaseOutbox(ctx context.Context, owner string, now time.Time, leaseTTL time.Duration, limit int) ([]OutboxRecord, error)

	// RenewOutboxLease extends a still-valid delivery lease using the same
	// fencing rule as RenewInboxLease.
	RenewOutboxLease(ctx context.Context, outboxID, owner string, now time.Time, leaseTTL time.Duration) error

	// MarkOutboxDispatched durably records the point immediately before the
	// first provider request may be emitted. A crash after this point becomes
	// unknown rather than an automatic retry.
	MarkOutboxDispatched(ctx context.Context, outboxID, owner string, attemptNo int, now time.Time) error

	// FinishOutboxAttempt atomically records the structured adapter result and
	// moves the scheduling row. exhausted only affects retryable_not_sent: the
	// attempt remains accurately recorded while the operation is parked dead.
	FinishOutboxAttempt(ctx context.Context, outboxID, owner string, attemptNo int, result delivery.Result, now, retryAt time.Time, exhausted bool) error

	// CompleteOutbox marks a reply sent.
	CompleteOutbox(ctx context.Context, outboxID, owner string, now time.Time) error

	// RetryOutbox re-arms a failed delivery; retryAt is now plus the backoff.
	RetryOutbox(ctx context.Context, outboxID, owner, errType string, now, retryAt time.Time) error

	// DeadLetterOutbox terminally parks an undeliverable reply.
	DeadLetterOutbox(ctx context.Context, outboxID, owner, errType string, now time.Time) error

	// ListUncertainOutbox returns parked operations without resolving them.
	ListUncertainOutbox(ctx context.Context, limit int) ([]OutboxRecord, error)

	// ResolveOutbox applies an audited CAS decision to an unknown operation.
	ResolveOutbox(ctx context.Context, req ResolveRequest) error

	// ReclaimExpired resets records whose lease expired before completion, so
	// they become runnable again. It returns the number of reclaimed rows.
	ReclaimExpired(ctx context.Context, now time.Time) (inbox, outbox int, err error)

	// Depths returns runnable and dead counts for both queues; used by
	// metrics and the readiness endpoint.
	Depths(ctx context.Context) (runnableInbox, deadInbox, pendingOutbox, deadOutbox, uncertainOutbox int, err error)
}

// InboxLeaseFence is the complete immutable identity captured from one Inbox
// lease. AttemptCount prevents an old attempt from becoming valid again even
// if an owner token were accidentally reused; the routing fields prevent a
// task payload from redirecting the transaction to another logical message.
type InboxLeaseFence struct {
	InboxID               string
	Owner                 string
	AttemptCount          int
	TenantID              string
	ChannelType           string
	BindingID             string
	DedupKey              string
	PartitionKey          string
	PipelineSchemaVersion int
	AtomicCommitMode      string
	DatabaseIdentity      string
}

// InboxBatchTxCompleter is the optional PostgreSQL-local extension used by a
// strict Session turn to include Inbox completion and deterministic Outbox
// insertion in its own transaction. Implementations must not commit or roll
// back tx. Memory stores deliberately do not implement this interface.
type InboxBatchTxCompleter interface {
	CompleteInboxBatchTx(context.Context, pgx.Tx, InboxLeaseFence, []OutboxRecord) error
}
