package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/testutil"
	"github.com/liuzengh/trpc-agent-service/trpcservice/assembly"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type fencingRecordingStateStore struct {
	*storage.MemoryStateStore
	mu    sync.Mutex
	token uint64
}

func (s *fencingRecordingStateStore) RecordExecution(ctx context.Context, record storage.ExecutionRecord) (storage.OutboxEvent, error) {
	s.mu.Lock()
	s.token = record.FencingToken
	s.mu.Unlock()
	return s.MemoryStateStore.RecordExecution(ctx, record)
}

func (s *fencingRecordingStateStore) recordedToken() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.token
}

func TestRuntimePersistsNonZeroSessionExecutionFence(t *testing.T) {
	t.Parallel()

	repository := tenant.NewMemoryRepository()
	publishRuntimeConfig(t, repository, "tenant-a", "telegram-bot-a")
	stateStore := &fencingRecordingStateStore{MemoryStateStore: storage.NewMemoryStateStore()}
	runtime, err := NewRuntime(
		repository,
		assembly.NewFactory(testutil.NewFakeModel("runtime reply")),
		storage.NewMemoryIdempotencyStore(),
		stateStore,
		time.Minute,
		time.Hour,
	)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	if _, err := runtime.Handle(context.Background(), "telegram-bot-a", channels.InboundMessage{
		MessageID: "fenced-message", Channel: channels.Telegram,
		ConversationID: "chat-fenced", SenderID: "user-1", Text: "hello",
	}); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if got := stateStore.recordedToken(); got == 0 {
		t.Fatal("RecordExecution() fencing token = 0; runtime silently bypassed session fencing")
	}
}

func TestRuntimeRoutesBindingsToIsolatedTenantRunnersAndRejectsDuplicate(t *testing.T) {
	t.Parallel()

	repository := tenant.NewMemoryRepository()
	publishRuntimeConfig(t, repository, "tenant-a", "telegram-bot-a")
	publishRuntimeConfig(t, repository, "tenant-b", "telegram-bot-b")

	stateStore := storage.NewMemoryStateStore()
	runtime, err := NewRuntime(
		repository,
		assembly.NewFactory(testutil.NewFakeModel("runtime reply")),
		storage.NewMemoryIdempotencyStore(),
		stateStore,
		time.Minute,
		time.Hour,
	)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}

	first, err := runtime.Handle(context.Background(), "telegram-bot-a", channels.InboundMessage{
		MessageID:      "update-42",
		Channel:        channels.Telegram,
		ConversationID: "chat-1",
		SenderID:       "user-1",
		Text:           "hello",
	})
	if err != nil {
		t.Fatalf("Handle() tenant A error = %v", err)
	}
	if got, want := first.TenantID, "tenant-a"; got != want {
		t.Fatalf("tenant A result tenant = %q, want %q", got, want)
	}
	if !strings.HasPrefix(first.SessionKey, "tenant-a/support/session/") {
		t.Fatalf("tenant A session key = %q, want channel-neutral tenant session", first.SessionKey)
	}
	if strings.Contains(first.SessionKey, "/telegram/") {
		t.Fatalf("tenant A session key leaked channel: %q", first.SessionKey)
	}

	second, err := runtime.Handle(context.Background(), "telegram-bot-b", channels.InboundMessage{
		MessageID:      "update-42",
		Channel:        channels.Telegram,
		ConversationID: "chat-1",
		SenderID:       "user-1",
		Text:           "hello",
	})
	if err != nil {
		t.Fatalf("Handle() tenant B error = %v", err)
	}
	if got, want := second.TenantID, "tenant-b"; got != want {
		t.Fatalf("tenant B result tenant = %q, want %q", got, want)
	}
	if got := pendingOutboxCount(t, stateStore, "tenant-a") + pendingOutboxCount(t, stateStore, "tenant-b"); got != 2 {
		t.Fatalf("pending outbox events = %d, want 2 for same message ID in different tenants", got)
	}

	_, err = runtime.Handle(context.Background(), "telegram-bot-a", channels.InboundMessage{
		MessageID:      "update-42",
		Channel:        channels.Telegram,
		ConversationID: "chat-1",
		SenderID:       "user-1",
		Text:           "hello",
	})
	if !errors.Is(err, ErrDuplicateMessage) {
		t.Fatalf("duplicate error = %v, want ErrDuplicateMessage", err)
	}
	if got := pendingOutboxCount(t, stateStore, "tenant-a") + pendingOutboxCount(t, stateStore, "tenant-b"); got != 2 {
		t.Fatalf("pending outbox events after duplicate = %d, want 2", got)
	}
}

func TestRuntimeKeepsWebConversationsIndependent(t *testing.T) {
	t.Parallel()

	repository := tenant.NewMemoryRepository()
	stateStore := storage.NewMemoryStateStore()
	runtime, err := NewRuntime(
		repository,
		assembly.NewFactory(testutil.NewFakeModel("runtime reply")),
		storage.NewMemoryIdempotencyStore(),
		stateStore,
		time.Minute,
		time.Hour,
	)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	snapshot := tenant.Snapshot{Config: config.TenantConfig{
		TenantID:      "tenant-a",
		AppCode:       "support",
		Status:        config.AgentActive,
		ConfigVersion: 1,
	}}

	first, err := runtime.Handle(WithConfigurationSnapshot(context.Background(), snapshot), "web-console", channels.InboundMessage{
		MessageID:      "web-message-1",
		Channel:        channels.Web,
		ConversationID: "conversation-1",
		SenderID:       "console-user",
		WebOwnerID:     "console-user",
		Text:           "hello one",
	})
	if err != nil {
		t.Fatalf("Handle() first web conversation error = %v", err)
	}
	second, err := runtime.Handle(WithConfigurationSnapshot(context.Background(), snapshot), "web-console", channels.InboundMessage{
		MessageID:      "web-message-2",
		Channel:        channels.Web,
		ConversationID: "conversation-2",
		SenderID:       "console-user",
		WebOwnerID:     "console-user",
		Text:           "hello two",
	})
	if err != nil {
		t.Fatalf("Handle() second web conversation error = %v", err)
	}
	if first.SessionKey == second.SessionKey {
		t.Fatalf("independent web conversations unexpectedly shared session %q", first.SessionKey)
	}
	continued, err := runtime.Handle(WithConfigurationSnapshot(context.Background(), snapshot), "web-console", channels.InboundMessage{
		MessageID:      "web-message-3",
		Channel:        channels.Web,
		ConversationID: "conversation-1",
		SenderID:       "console-user",
		WebOwnerID:     "console-user",
		Text:           "hello again",
	})
	if err != nil {
		t.Fatalf("Handle() continued web conversation error = %v", err)
	}
	if first.SessionKey != continued.SessionKey {
		t.Fatalf("same web conversation changed session from %q to %q", first.SessionKey, continued.SessionKey)
	}
}

func publishRuntimeConfig(t *testing.T, repository tenant.Repository, tenantID, bindingID string) {
	t.Helper()

	_, err := repository.Publish(context.Background(), config.TenantConfig{
		TenantID:      tenantID,
		AppCode:       "support",
		Status:        config.AgentActive,
		ConfigVersion: 1,
		Channels: []config.ChannelBinding{{
			Type:      config.ChannelTelegram,
			BindingID: bindingID,
		}},
	})
	if err != nil {
		t.Fatalf("Publish() %q error = %v", tenantID, err)
	}
}

func pendingOutboxCount(t *testing.T, store *storage.MemoryStateStore, tenantID string) int {
	t.Helper()
	events, err := store.ListPendingOutbox(context.Background(), tenantID, 100)
	if err != nil {
		t.Fatalf("ListPendingOutbox(%q) error = %v", tenantID, err)
	}
	return len(events)
}
