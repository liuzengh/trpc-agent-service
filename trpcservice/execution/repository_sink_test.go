package execution

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestRepositorySinkRequiresRepository(t *testing.T) {
	if _, err := NewRepositorySink(nil); !errors.Is(err, ErrMissingDependency) {
		t.Fatalf("nil repository error=%v", err)
	}
	if err := (&RepositorySink{}).Commit(context.Background(), ExecutionCommit{}); !errors.Is(err, ErrMissingDependency) {
		t.Fatalf("nil sink repository error=%v", err)
	}
}

func TestValidateRepositoryCommitRejectsMismatchedExecutionIdentity(t *testing.T) {
	commit := validRepositoryCommitForTest()
	commit.JobID = "different-job"
	if err := validateRepositoryCommit(commit); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("mismatched job error=%v", err)
	}
	commit = validRepositoryCommitForTest()
	commit.ExecutionID = "different-execution"
	if err := validateRepositoryCommit(commit); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("mismatched execution error=%v", err)
	}
}

func TestValidateRepositoryCommitRejectsTenantAndFenceMismatch(t *testing.T) {
	commit := validRepositoryCommitForTest()
	commit.TenantID = "different-tenant"
	if err := validateRepositoryCommit(commit); !errors.Is(err, storage.ErrTenantMismatch) {
		t.Fatalf("tenant mismatch error=%v", err)
	}
	commit = validRepositoryCommitForTest()
	commit.FenceToken++
	if err := validateRepositoryCommit(commit); !errors.Is(err, storage.ErrFenceRejected) {
		t.Fatalf("fence mismatch error=%v", err)
	}
}

func validRepositoryCommitForTest() ExecutionCommit {
	now := time.Now().UTC()
	tc := tenant.TenantContext{TenantID: "tenant-sink", SessionID: "session-sink"}
	job := queue.AgentJob{JobID: "job-sink", ExecutionID: "execution-sink"}
	lease := storage.Lease{TenantID: tc.TenantID, SessionID: tc.SessionID, ResourceID: tc.SessionID, OwnerID: "owner-sink", Epoch: 2, FenceToken: 7, ExpiresAt: now.Add(time.Minute)}
	return ExecutionCommit{
		JobID: job.JobID, ExecutionID: job.ExecutionID, TenantID: tc.TenantID, SessionID: tc.SessionID,
		OwnerID: lease.OwnerID, Epoch: lease.Epoch, FenceToken: lease.FenceToken,
		Job: job, TenantContext: tc, Input: agent.AgentInput{TenantContext: tc}, Lease: lease,
	}
}
