package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/testutil"
	"github.com/liuzengh/trpc-agent-service/trpcservice/assembly"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// TestRuntimeExecutionDedupBackstop verifies the durable idempotency backstop:
// a completed message is rejected as duplicate, a live claim owned by another
// node reports in-progress, and a delivered execution persists session and
// audit state.
func TestRuntimeExecutionDedupBackstop(t *testing.T) {
	t.Parallel()

	repository := tenant.NewMemoryRepository()
	publishRuntimeConfig(t, repository, "tenant-a", "telegram-bot-a")

	dedup := storage.NewMemoryExecutionDedupStore()
	stateStore := storage.NewMemoryStateStore()
	runtime := newRuntimeWithBackstops(t, repository, dedup, stateStore)

	inbound := func(messageID string) channels.InboundMessage {
		return channels.InboundMessage{
			MessageID:      messageID,
			Channel:        channels.Telegram,
			ConversationID: "chat-1",
			SenderID:       "user-1",
			Text:           "hello",
		}
	}

	first, err := runtime.Handle(context.Background(), "telegram-bot-a", inbound("update-42"))
	if err != nil {
		t.Fatalf("first Handle() error = %v", err)
	}
	if got := pendingOutboxCount(t, stateStore, "tenant-a"); got != 1 {
		t.Fatalf("pending outbox events = %d, want 1", got)
	}

	session, err := stateStore.GetSession(context.Background(), "tenant-a", first.SessionKey)
	if err != nil {
		t.Fatalf("GetSession() error = %v, want recorded session", err)
	}
	if session.Revision != 1 || session.LastMessageID != "update-42" {
		t.Fatalf("session = %+v, want revision 1 and message update-42", session)
	}
	audits, err := stateStore.ListAudit(context.Background(), "tenant-a", "update-42")
	if err != nil || len(audits) != 1 {
		t.Fatalf("ListAudit() = %d events, error = %v, want 1", len(audits), err)
	}

	// Production PostgresStateStore flips the claim to completed inside the
	// same transaction; mirror that here.
	dedup.MarkCompleted("tenant-a", "telegram", "telegram-bot-a", "update-42")
	_, err = runtime.Handle(context.Background(), "telegram-bot-a", inbound("update-42"))
	if !errors.Is(err, ErrDuplicateMessage) {
		t.Fatalf("duplicate error = %v, want ErrDuplicateMessage", err)
	}
	if got := pendingOutboxCount(t, stateStore, "tenant-a"); got != 1 {
		t.Fatalf("pending outbox events after duplicate = %d, want 1", got)
	}

	// A live claim owned by another node must report in-progress.
	otherRuntime := newRuntimeWithBackstops(t, repository, dedup, stateStore)
	dedupClaim, err := dedup.Begin(context.Background(), "tenant-a", "support", "telegram", "telegram-bot-a", "update-43", "other-node", time.Minute)
	if err != nil || dedupClaim != storage.ExecutionFresh {
		t.Fatalf("Begin() claim = %v, error = %v, want fresh", dedupClaim, err)
	}
	_, err = otherRuntime.Handle(context.Background(), "telegram-bot-a", inbound("update-43"))
	if !errors.Is(err, ErrMessageInProgress) {
		t.Fatalf("in-progress error = %v, want ErrMessageInProgress", err)
	}
}

func newRuntimeWithBackstops(t *testing.T, repository tenant.Repository, dedup storage.ExecutionDedupStore, stateStore storage.StateStore) *Runtime {
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
