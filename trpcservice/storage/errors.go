package storage

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

var (
	ErrInvalidArgument           = errors.New("invalid storage argument")
	ErrNotFound                  = errors.New("resource not found")
	ErrTenantMismatch            = tenant.ErrTenantMismatch
	ErrConflict                  = errors.New("write conflict")
	ErrLeaseLost                 = errors.New("session lease lost")
	ErrFenceRejected             = errors.New("stale fence token")
	ErrAlreadyClaimed            = errors.New("message already claimed")
	ErrAlreadyCompleted          = errors.New("message already completed")
	ErrBackendUnavailable        = errors.New("coordination backend unavailable")
	ErrOperationAmbiguous        = errors.New("coordination operation result is ambiguous")
	ErrCompletionOutcomeUnknown  = errors.New("completion transaction outcome is unknown")
	ErrCompletionPartial         = errors.New("completion facts are partially committed")
	ErrEpochRejected             = errors.New("coordination epoch rejected")
	ErrInvalidOwner              = errors.New("invalid coordination owner")
	ErrRateLimited               = errors.New("rate limited")
	ErrDedupConflict             = errors.New("outbox deduplication conflict")
	ErrOutboxLockLost            = errors.New("outbox processing lock lost")
	ErrOutboxLockExpired         = errors.New("outbox processing lock expired")
	ErrUnsafeFailureCode         = errors.New("unsafe durable failure code")
	ErrAlreadyDead               = errors.New("message already dead")
	ErrTransactionOutcomeUnknown = errors.New("transaction outcome is unknown")
	ErrInvalidDelivery           = errors.New("invalid queue delivery")
	ErrDeliveryExpired           = errors.New("queue delivery expired")
	ErrDeliveryFinished          = errors.New("queue delivery already finished")
)

// ExecutionCommitRecord is the storage-neutral payload for the durable
// execution result commit. The repository must re-check the current lease in
// the same transaction that writes the result.
// pi-lens-ignore: DuplicateDecl
type ExecutionCommitRecord struct {
	JobID       string
	ExecutionID string
	TenantID    string
	SessionID   string
	OwnerID     string
	Epoch       Epoch
	FenceToken  uint64
	ResultJSON  []byte
	// ConfigVersion is the additive P1-08 durability field: the immutable
	// config version of the executed job. Zero means unknown/legacy.
	ConfigVersion int64
}

// pi-lens-ignore: DuplicateDecl
type ExecutionResultRecord struct {
	JobID         string
	ExecutionID   string
	TenantID      string
	SessionID     string
	OwnerID       string
	Epoch         Epoch
	FenceToken    uint64
	Status        string
	ResultVersion int64
	ResultJSON    []byte
	CommittedAt   time.Time
	// ConfigVersion is the immutable configuration version used by the job.
	// Zero preserves compatibility with legacy result rows.
	ConfigVersion int64
}

// ExecutionResultRepository is the P0-09A durable commit boundary used by the
// execution adapter. It deliberately does not expose Queue Ack semantics.
// pi-lens-ignore: DuplicateDecl
type ExecutionResultRepository interface {
	CommitExecution(context.Context, ExecutionCommitRecord) error
	GetExecutionResult(context.Context, tenant.TenantContext, string, string) (ExecutionResultRecord, error)
}

// DeliveryAckRecord is the storage-neutral identity of a claimed queue
// delivery. DeliveryID is the queue's current delivery token.
type DeliveryAckRecord struct {
	TenantID    string
	JobID       string
	ExecutionID string
	SessionID   string
	DeliveryID  string
}

// AtomicCompletionRequest joins the fenced result, queue delivery, and (when
// non-nil) durable reply identity without making storage depend on execution,
// queue, or worker runtime types. A nil Outbox is the explicitly retained
// P0-09C two-fact contract; durable Worker paths must always provide it.
type AtomicCompletionRequest struct {
	Commit   ExecutionCommitRecord
	Delivery DeliveryAckRecord
	Outbox   *OutboxMessage
}

// AtomicCompletionCoordinator commits an execution result, acknowledges its
// delivery, and optionally enqueues the durable reply in one transaction.
// Implementations must not fall back to independent repository, queue, or
// outbox operations.
type AtomicCompletionCoordinator interface {
	CommitResultAndAck(context.Context, AtomicCompletionRequest) error
}
