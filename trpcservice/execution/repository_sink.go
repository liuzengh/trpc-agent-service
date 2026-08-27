package execution

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

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
