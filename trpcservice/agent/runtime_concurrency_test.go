package agent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/testutil"
	"github.com/liuzengh/trpc-agent-service/trpcservice/assembly"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func inbound(messageID string) channels.InboundMessage {
	return channels.InboundMessage{
		MessageID: messageID, Channel: channels.Telegram,
		ConversationID: "chat-1", SenderID: "user-1", Text: "hello",
	}
}

// TestRuntimeExactlyOnceUnderConcurrentDuplicateSubmission drives the same
// message through one Runtime from many goroutines. Exactly one execution may
// deliver a reply; every other attempt must be rejected as duplicate or
// in-progress. This is the exactly-once contract under real contention.
func TestRuntimeExactlyOnceUnderConcurrentDuplicateSubmission(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	publishRuntimeConfig(t, repository, "tenant-a", "telegram-bot-a")

	dedup := storage.NewMemoryExecutionDedupStore()
	stateStore := storage.NewMemoryStateStore()
	runtime := buildRuntime(t, repository, dedup, stateStore)

	const goroutines = 32
	results := make(chan error, goroutines)
	var wait sync.WaitGroup
	for index := 0; index < goroutines; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := runtime.Handle(context.Background(), "telegram-bot-a", inbound("contested-1"))
			results <- err
		}()
	}
	wait.Wait()
	close(results)

	succeeded, duplicates, inProgress := 0, 0, 0
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrDuplicateMessage):
			duplicates++
		case errors.Is(err, ErrMessageInProgress):
			inProgress++
		default:
			t.Fatalf("unexpected Handle() error = %v", err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("successful executions = %d, want exactly 1 (duplicates=%d, in-progress=%d)", succeeded, duplicates, inProgress)
	}
	if succeeded+duplicates+inProgress != goroutines {
		t.Fatalf("classified attempts = %d, want %d", succeeded+duplicates+inProgress, goroutines)
	}
	if got := pendingOutboxCount(t, stateStore, "tenant-a"); got != 1 {
		t.Fatalf("pending outbox events = %d, want exactly 1", got)
	}
	audits, err := stateStore.ListAudit(context.Background(), "tenant-a", "contested-1")
	if err != nil || len(audits) != 1 {
		t.Fatalf("audit records = %d, error = %v, want exactly 1", len(audits), err)
	}
}

// TestRuntimeExactlyOnceAcrossNodes simulates two worker nodes whose Redis
// lease state diverged: both hold independent idempotency stores, so only the
// durable execution claim can prevent a duplicate reply.
func TestRuntimeExactlyOnceAcrossNodes(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	publishRuntimeConfig(t, repository, "tenant-a", "telegram-bot-a")

	dedup := storage.NewMemoryExecutionDedupStore()
	stateStore := storage.NewMemoryStateStore()

	runtimeA := buildRuntime(t, repository, dedup, stateStore)
	runtimeB := buildRuntime(t, repository, dedup, stateStore)

	start := make(chan struct{})
	results := make(chan error, 2)
	for _, runtime := range []*Runtime{runtimeA, runtimeB} {
		go func(handle *Runtime) {
			<-start
			_, err := handle.Handle(context.Background(), "telegram-bot-a", inbound("cross-node-1"))
			results <- err
		}(runtime)
	}
	close(start)

	first := <-results
	second := <-results
	if first == nil && second == nil {
		t.Fatal("both nodes delivered, want exactly one success")
	}
	if first != nil && second != nil {
		t.Fatalf("both nodes failed: %v and %v, want one success", first, second)
	}
	failure := first
	if failure == nil {
		failure = second
	}
	if !errors.Is(failure, ErrDuplicateMessage) && !errors.Is(failure, ErrMessageInProgress) {
		t.Fatalf("losing node error = %v, want duplicate or in-progress", failure)
	}
	if got := pendingOutboxCount(t, stateStore, "tenant-a"); got != 1 {
		t.Fatalf("pending outbox events = %d, want exactly 1", got)
	}
}

func buildRuntime(t *testing.T, repository tenant.Repository, dedup storage.ExecutionDedupStore, stateStore storage.StateStore) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(
		repository,
		assembly.NewFactory(testutil.NewFakeModel("runtime reply")),
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
	return runtime
}
