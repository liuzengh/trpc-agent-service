package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type failingSessionLeaseStore struct {
	*storage.MemoryStateStore
}

var errSessionLeaseUnavailable = errors.New("session lease unavailable")

type recordingIdempotencyStore struct {
	storage.IdempotencyStore
	renewed chan struct{}
}

func (s *recordingIdempotencyStore) Renew(ctx context.Context, lease storage.Lease, ttl time.Duration) error {
	err := s.IdempotencyStore.Renew(ctx, lease, ttl)
	if err == nil {
		select {
		case s.renewed <- struct{}{}:
		default:
		}
	}
	return err
}

func (s *failingSessionLeaseStore) AcquireSessionExecutionLease(context.Context, string, string, string, time.Duration) (storage.SessionExecutionLease, error) {
	return storage.SessionExecutionLease{}, errSessionLeaseUnavailable
}

func TestAcquireExecutionLeasesKeepsFailedClaimRetryable(t *testing.T) {
	idempotency := storage.NewMemoryIdempotencyStore()
	dedup := storage.NewMemoryExecutionDedupStore()
	runtime := &Runtime{
		idempotency: idempotency, executionDedup: dedup,
		stateStore:    &failingSessionLeaseStore{MemoryStateStore: storage.NewMemoryStateStore()},
		processingTTL: time.Minute, completedTTL: time.Hour,
	}
	snapshot := tenant.Snapshot{Config: config.TenantConfig{TenantID: "tenant-a", AppCode: "support"}}
	inbound := channels.InboundMessage{
		MessageID: "message-1", Channel: channels.Telegram,
		ConversationID: "chat-1", SenderID: "user-1", Text: "hello",
	}

	_, _, err := runtime.acquireExecutionLeases(
		context.Background(), snapshot, "telegram-main", "tenant-a/support/session/1", inbound, "trace-1",
	)
	if !errors.Is(err, errSessionLeaseUnavailable) {
		t.Fatalf("acquireExecutionLeases() error = %v, want session lease failure", err)
	}

	key, err := storage.BuildIdempotencyKey("tenant-a", channels.Telegram, "telegram-main", "message-1")
	if err != nil {
		t.Fatal(err)
	}
	acquired, err := idempotency.Acquire(context.Background(), key, time.Minute)
	if err != nil || acquired.State != storage.LeaseAcquired {
		t.Fatalf("idempotency after rollback = %q, %v; want acquired", acquired.State, err)
	}
	claims, err := dedup.ListClaims(context.Background(), "tenant-a", "", 10)
	if err != nil || len(claims) != 1 || claims[0].Status != "failed" {
		t.Fatalf("failed claim after lease error = %+v, %v", claims, err)
	}
	claim, err := dedup.Begin(context.Background(), "tenant-a", "support", "telegram", "telegram-main", "message-1", "trace-2", time.Minute)
	if err != nil || claim != storage.ExecutionFresh {
		t.Fatalf("dedup after rollback = %q, %v; want fresh", claim, err)
	}
}

func TestCompletedExecutionLeaseCloseKeepsMessageCompleted(t *testing.T) {
	idempotency := storage.NewMemoryIdempotencyStore()
	state := storage.NewMemoryStateStore()
	runtime := &Runtime{
		idempotency: idempotency, stateStore: state,
		processingTTL: time.Minute, completedTTL: time.Hour,
	}
	snapshot := tenant.Snapshot{Config: config.TenantConfig{TenantID: "tenant-a", AppCode: "support"}}
	inbound := channels.InboundMessage{
		MessageID: "message-1", Channel: channels.Telegram,
		ConversationID: "chat-1", SenderID: "user-1", Text: "hello",
	}

	_, leases, err := runtime.acquireExecutionLeases(
		context.Background(), snapshot, "telegram-main", "tenant-a/support/session/1", inbound, "trace-1",
	)
	if err != nil {
		t.Fatalf("acquireExecutionLeases() error = %v", err)
	}
	if err := leases.Complete(); err != nil {
		t.Fatalf("Complete() error = %v", err)
	}
	leases.Close()

	key, err := storage.BuildIdempotencyKey("tenant-a", channels.Telegram, "telegram-main", "message-1")
	if err != nil {
		t.Fatal(err)
	}
	acquired, err := idempotency.Acquire(context.Background(), key, time.Minute)
	if err != nil || acquired.State != storage.LeaseAlreadyCompleted {
		t.Fatalf("idempotency after completed close = %q, %v; want already completed", acquired.State, err)
	}
}

func TestExecutionLeasesRenewMessageIdempotencyWhileRunning(t *testing.T) {
	base := storage.NewMemoryIdempotencyStore()
	idempotency := &recordingIdempotencyStore{IdempotencyStore: base, renewed: make(chan struct{}, 1)}
	runtime := &Runtime{
		idempotency: idempotency, stateStore: storage.NewMemoryStateStore(),
		processingTTL: 600 * time.Millisecond, completedTTL: time.Hour,
	}
	snapshot := tenant.Snapshot{Config: config.TenantConfig{TenantID: "tenant-a", AppCode: "support"}}
	inbound := channels.InboundMessage{
		MessageID: "message-long", Channel: channels.Feishu,
		ConversationID: "chat-1", SenderID: "user-1", Text: "hello",
	}

	_, leases, err := runtime.acquireExecutionLeases(
		context.Background(), snapshot, "feishu-main", "tenant-a/support/session/long", inbound, "trace-long",
	)
	if err != nil {
		t.Fatalf("acquireExecutionLeases() error = %v", err)
	}
	defer leases.Close()
	select {
	case <-idempotency.renewed:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("message idempotency lease was not renewed during long execution")
	}
	if err := leases.Complete(); err != nil {
		t.Fatalf("Complete() after renewal error = %v", err)
	}
}
