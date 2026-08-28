package execution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

const (
	ReplyOutboxSchemaVersion = channels.ReplyOutboxSchemaVersion
	ReplyOutboxKind          = channels.ReplyOutboxKind
)

// ReplyOutboxPayload remains exported from execution for compatibility. The
// channel package owns the versioned sender-facing contract.
type ReplyOutboxPayload = channels.ReplyOutboxPayload

// BuildReplyOutboxMessage deterministically maps one successful execution to
// a durable reply fact. The execution result remains a separate complete fact;
// this payload is the intentionally smaller sender-facing projection.
func BuildReplyOutboxMessage(commit ExecutionCommit) (storage.OutboxMessage, error) {
	if err := validateRepositoryCommit(commit); err != nil {
		return storage.OutboxMessage{}, err
	}
	routing, err := channels.RoutingFromTenantContext(commit.TenantContext)
	if err != nil {
		return storage.OutboxMessage{}, fmt.Errorf("execution: reply routing: %w", err)
	}
	payload := ReplyOutboxPayload{
		SchemaVersion:        ReplyOutboxSchemaVersion,
		Kind:                 ReplyOutboxKind,
		TenantID:             commit.TenantID,
		SessionID:            commit.SessionID,
		JobID:                commit.JobID,
		ExecutionID:          commit.ExecutionID,
		RequestID:            commit.TenantContext.RequestID,
		MessageID:            commit.TenantContext.MessageID,
		TraceID:              commit.TenantContext.TraceID,
		Channel:              routing.Channel,
		DestinationType:      routing.DestinationType,
		DestinationID:        routing.DestinationID,
		ReplyText:            commit.Result.Text,
		FinishType:           commit.Result.FinishType,
		SenderRoutingVersion: channels.SenderRoutingVersion,
	}
	encoded, err := channels.EncodeReplyOutboxPayload(payload)
	if err != nil {
		return storage.OutboxMessage{}, fmt.Errorf("execution: marshal reply outbox: %w", err)
	}
	message := storage.OutboxMessage{
		TenantID:    commit.TenantID,
		ID:          "reply-" + commit.ExecutionID,
		Kind:        ReplyOutboxKind,
		AggregateID: commit.ExecutionID,
		DedupKey:    replyDedupKey(commit.TenantID, commit.ExecutionID),
		Payload:     encoded,
	}
	if err := storage.ValidateOutboxMessage(message); err != nil {
		return storage.OutboxMessage{}, err
	}
	return message, nil
}

func replyDedupKey(tenantID, executionID string) string {
	canonical := tenantID + "|" + executionID + "|" + ReplyOutboxKind
	if len(canonical) <= 256 {
		return canonical
	}
	digest := sha256.Sum256([]byte(canonical))
	return "reply-sha256-" + hex.EncodeToString(digest[:])
}

// RepositorySink adapts the R3 FencedExecutionSink contract to the storage
// repository without importing execution types into the storage package.
type RepositorySink struct {
	Repository storage.ExecutionResultRepository
}

func NewRepositorySink(repository storage.ExecutionResultRepository) (*RepositorySink, error) {
	if repository == nil {
		return nil, ErrMissingDependency
	}
	return &RepositorySink{Repository: repository}, nil
}

func (s *RepositorySink) Commit(ctx context.Context, commit ExecutionCommit) error {
	if s == nil || s.Repository == nil {
		return ErrMissingDependency
	}
	if err := validateRepositoryCommit(commit); err != nil {
		return err
	}
	resultJSON, err := json.Marshal(commit.Result)
	if err != nil {
		return fmt.Errorf("execution: marshal result: %w", err)
	}
	return s.Repository.CommitExecution(ctx, storage.ExecutionCommitRecord{
		JobID:       commit.JobID,
		ExecutionID: commit.ExecutionID,
		TenantID:    commit.TenantID,
		SessionID:   commit.SessionID,
		OwnerID:     commit.OwnerID,
		Epoch:       commit.Epoch,
		FenceToken:  commit.FenceToken,
		ResultJSON:  resultJSON,
	})
}

// AtomicCompletionRequestFor converts an execution commit and its claimed
// delivery to the storage-neutral atomic completion contract. It performs the
// same identity checks as RepositorySink before a durable coordinator is called.
// pi-lens-ignore: UndeclaredImportedName
func AtomicCompletionRequestFor(commit ExecutionCommit, delivery queue.Delivery) (storage.AtomicCompletionRequest, error) {
	if err := validateRepositoryCommit(commit); err != nil {
		// pi-lens-ignore: UndeclaredImportedName
		return storage.AtomicCompletionRequest{}, err
	}
	if delivery.DeliveryID == "" || delivery.Job.JobID == "" || delivery.Job.ExecutionID == "" {
		return storage.AtomicCompletionRequest{}, queue.ErrInvalidDelivery
	}
	if delivery.Job.JobID != commit.JobID || delivery.Job.ExecutionID != commit.ExecutionID {
		// pi-lens-ignore: UndeclaredImportedName
		return storage.AtomicCompletionRequest{}, queue.ErrInvalidDelivery
	}
	if delivery.Job.Tenant.TenantID != commit.TenantID || delivery.Job.Tenant.SessionID != commit.SessionID {
		// pi-lens-ignore: UndeclaredImportedName
		return storage.AtomicCompletionRequest{}, storage.ErrTenantMismatch
	}
	resultJSON, err := json.Marshal(commit.Result)
	if err != nil {
		// pi-lens-ignore: UndeclaredImportedName
		return storage.AtomicCompletionRequest{}, fmt.Errorf("execution: marshal result: %w", err)
	}
	outbox, err := BuildReplyOutboxMessage(commit)
	if err != nil {
		return storage.AtomicCompletionRequest{}, err
	}
	// pi-lens-ignore: UndeclaredImportedName
	return storage.AtomicCompletionRequest{
		Commit: storage.ExecutionCommitRecord{
			JobID:       commit.JobID,
			ExecutionID: commit.ExecutionID,
			TenantID:    commit.TenantID,
			SessionID:   commit.SessionID,
			OwnerID:     commit.OwnerID,
			Epoch:       commit.Epoch,
			FenceToken:  commit.FenceToken,
			ResultJSON:  resultJSON,
		},
		// pi-lens-ignore: UndeclaredImportedName
		Delivery: storage.DeliveryAckRecord{
			TenantID:    delivery.Job.Tenant.TenantID,
			JobID:       delivery.Job.JobID,
			ExecutionID: delivery.Job.ExecutionID,
			SessionID:   delivery.Job.Tenant.SessionID,
			DeliveryID:  delivery.DeliveryID,
		},
		// Durable Worker completion must carry the reply fact explicitly.
		Outbox: &outbox,
	}, nil
}

func validateRepositoryCommit(commit ExecutionCommit) error {
	if commit.JobID == "" || commit.ExecutionID == "" || commit.TenantID == "" || commit.SessionID == "" || commit.OwnerID == "" || commit.Epoch == 0 || commit.FenceToken == 0 {
		return ErrInvalidRequest
	}
	if commit.JobID != commit.Job.JobID || commit.ExecutionID != commit.Job.ExecutionID {
		return ErrInvalidRequest
	}
	if commit.TenantID != commit.TenantContext.TenantID || commit.SessionID != commit.TenantContext.SessionID {
		return storage.ErrTenantMismatch
	}
	if commit.OwnerID != commit.Lease.OwnerID || commit.Epoch != commit.Lease.Epoch || commit.FenceToken != commit.Lease.FenceToken ||
		commit.Lease.TenantID != commit.TenantID || commit.Lease.SessionID != commit.SessionID || commit.Lease.ResourceID != commit.SessionID ||
		commit.Lease.ExpiresAt.IsZero() {
		return storage.ErrFenceRejected
	}
	if commit.Input.TenantContext.TenantID != commit.TenantID || commit.Input.TenantContext.SessionID != commit.SessionID {
		return storage.ErrTenantMismatch
	}
	return nil
}

var _ FencedExecutionSink = (*RepositorySink)(nil)
