package messaging

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

type recordingChannelResolver struct {
	tenantID, appCode string
	version           uint64
	key               channels.BindingKey
	sender            channels.Sender
}

func (r *recordingChannelResolver) ResolveSender(_ context.Context, tenantID, appCode string, version uint64, key channels.BindingKey) (channels.Sender, error) {
	r.tenantID, r.appCode, r.version, r.key = tenantID, appCode, version, key
	return r.sender, nil
}

type acceptingDynamicSender struct{ calls int }

func (s *acceptingDynamicSender) Send(context.Context, channels.ReplyTarget, channels.OutboundMessage) (channels.SendReceipt, error) {
	s.calls++
	return channels.SendReceipt{ExternalMessageID: "sent"}, nil
}

func TestDynamicOutboxResolverUsesImmutableConfigVersionAndBinding(t *testing.T) {
	store := storage.NewMemoryStateStore()
	_, err := store.RecordExecution(context.Background(), storage.ExecutionRecord{
		TenantID: "tenant-a", AppCode: "support", SessionKey: "tenant-a/support/telegram/chat-1",
		MessageID: "message-1", Channel: "telegram", BindingID: "bot-v3", TraceID: "trace-1", Action: "agent_reply",
		Result: "queued", OutboxType: ChannelReplyEventType(channels.Telegram),
		OutboxPayload: []byte(`{"channel":"telegram","binding_id":"bot-v3","app_code":"support","config_version":3,"conversation_id":"chat-1","text":"reply"}`),
	})
	if err != nil {
		t.Fatalf("RecordExecution(): %v", err)
	}
	sender := &acceptingDynamicSender{}
	resolver := &recordingChannelResolver{sender: sender}
	dispatcher, err := NewChannelOutboxDispatcherWithResolver(store, resolver)
	if err != nil {
		t.Fatalf("NewChannelOutboxDispatcherWithResolver(): %v", err)
	}
	if _, err := dispatcher.DispatchTenant(context.Background(), "tenant-a", 10); err != nil {
		t.Fatalf("DispatchTenant(): %v", err)
	}
	if resolver.tenantID != "tenant-a" || resolver.appCode != "support" || resolver.version != 3 {
		t.Fatalf("resolver snapshot = %s/%s@%d", resolver.tenantID, resolver.appCode, resolver.version)
	}
	if resolver.key.BindingID != "bot-v3" || resolver.key.Channel != channels.Telegram {
		t.Fatalf("resolver binding = %+v", resolver.key)
	}
}
