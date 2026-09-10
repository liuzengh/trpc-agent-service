package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/assembly"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

// errorModel simulates a provider outage: the model call itself fails.
type errorModel struct{}

func (errorModel) GenerateContent(ctx context.Context, _ *model.Request) (<-chan *model.Response, error) {
	return nil, errors.New("provider unavailable")
}

func (errorModel) Info() model.Info { return model.Info{Name: "error-model"} }

// TestRuntimeModelFailureWritesNothingAndStaysRetryable verifies that a model
// outage produces no durable reply/audit side effect and releases its claim.
func TestRuntimeModelFailureWritesNothingAndStaysRetryable(t *testing.T) {
	repository := tenantNewRepo(t)
	dedup := storage.NewMemoryExecutionDedupStore()
	stateStore := storage.NewMemoryStateStore()
	message := inbound("model-fail-1")

	runtime, err := NewRuntime(
		repository,
		assembly.NewFactory(errorModel{}),
		storage.NewMemoryIdempotencyStore(),
		stateStore,
		time.Minute,
		time.Hour,
		WithInvocationFactory(DefaultInvocationFactory),
		WithExecutionDedup(dedup),
	)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	if _, err := runtime.Handle(context.Background(), "telegram-bot-a", message); err == nil {
		t.Fatal("Handle() with failing model error = nil, want failure")
	}
	if got := pendingOutboxCount(t, stateStore, "tenant-a"); got != 0 {
		t.Fatalf("pending outbox events = %d, want 0 after model failure", got)
	}
	audits, err := stateStore.ListAudit(context.Background(), "tenant-a", message.MessageID)
	if err != nil || len(audits) != 0 {
		t.Fatalf("audit after model failure = %d, error = %v, want 0", len(audits), err)
	}
	state, err := dedup.Begin(context.Background(), "tenant-a", "telegram", "telegram-bot-a", message.MessageID, "trace-model-retry", time.Minute)
	if err != nil || state != storage.ExecutionFresh {
		t.Fatalf("Begin() after model failure state = %q, error = %v, want fresh", state, err)
	}
}

func tenantNewRepo(t *testing.T) tenant.Repository {
	t.Helper()
	repository := tenant.NewMemoryRepository()
	publishRuntimeConfig(t, repository, "tenant-a", "telegram-bot-a")
	return repository
}
