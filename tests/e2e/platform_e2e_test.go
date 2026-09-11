package e2e_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/testutil"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/assembly"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestTwoTenantsAndChannelsRemainIsolatedAcrossReplay(t *testing.T) {
	repository := tenant.NewMemoryRepository()
	publish(t, repository, "tenant-a", config.ChannelTelegram, "telegram-a")
	publish(t, repository, "tenant-b", config.ChannelWeCom, "wecom-b")
	stateStore := storage.NewMemoryStateStore()
	runtime, err := agent.NewRuntime(repository, assembly.NewFactory(testutil.NewFakeModel("e2e reply")), storage.NewMemoryIdempotencyStore(), stateStore, time.Minute, time.Hour)
	if err != nil {
		t.Fatalf("NewRuntime() error = %v", err)
	}
	if _, err := runtime.Handle(context.Background(), "telegram-a", inbound(channels.Telegram)); err != nil {
		t.Fatalf("tenant A Handle() error = %v", err)
	}
	if _, err := runtime.Handle(context.Background(), "wecom-b", inbound(channels.WeCom)); err != nil {
		t.Fatalf("tenant B Handle() error = %v", err)
	}
	if _, err := runtime.Handle(context.Background(), "telegram-a", inbound(channels.Telegram)); !errors.Is(err, agent.ErrDuplicateMessage) {
		t.Fatalf("replay error = %v, want ErrDuplicateMessage", err)
	}
	if got, want := pendingOutboxCount(t, stateStore, "tenant-a")+pendingOutboxCount(t, stateStore, "tenant-b"), 2; got != want {
		t.Fatalf("pending replies = %d, want %d", got, want)
	}
}

func publish(t *testing.T, repository tenant.Repository, tenantID, channel, binding string) {
	t.Helper()
	_, err := repository.Publish(context.Background(), config.TenantConfig{TenantID: tenantID, AppCode: "support", Status: config.AgentActive, ConfigVersion: 1, Channels: []config.ChannelBinding{{Type: channel, BindingID: binding}}})
	if err != nil {
		t.Fatalf("Publish(%s) error = %v", tenantID, err)
	}
}

func inbound(channel channels.Channel) channels.InboundMessage {
	return channels.InboundMessage{MessageID: "same-message-id", Channel: channel, ConversationID: "conversation-1", SenderID: "user-1", Text: "hello"}
}

func pendingOutboxCount(t *testing.T, store *storage.MemoryStateStore, tenantID string) int {
	t.Helper()
	events, err := store.ListPendingOutbox(context.Background(), tenantID, 100)
	if err != nil {
		t.Fatalf("ListPendingOutbox(%q) error = %v", tenantID, err)
	}
	return len(events)
}
