package messaging

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

type recordingSenderObserver struct {
	attributes []metrics.SenderAttributes
	errors     []error
}

func (o *recordingSenderObserver) StartSender(ctx context.Context, attributes metrics.SenderAttributes) (context.Context, func(error)) {
	o.attributes = append(o.attributes, attributes)
	return ctx, func(err error) { o.errors = append(o.errors, err) }
}

func TestChannelOutboxDispatcherObservesExternalSendAtBindingBoundary(t *testing.T) {
	store := storage.NewMemoryStateStore()
	if _, err := store.RecordExecution(context.Background(), storage.ExecutionRecord{
		TenantID: "tenant-a", AppCode: "support", SessionKey: "tenant-a/support/telegram/chat-1", MessageID: "message-1", Channel: "telegram", BindingID: "telegram-bot-b", TraceID: "trace-1", Action: "agent.reply", Result: "queued", OutboxType: ChannelReplyEventType(channels.Telegram), OutboxPayload: []byte(`{"channel":"telegram","binding_id":"telegram-bot-b","conversation_id":"chat-1","text":"reply"}`),
	}); err != nil {
		t.Fatalf("RecordExecution() error = %v", err)
	}
	observer := &recordingSenderObserver{}
	dispatcher, err := NewChannelOutboxDispatcher(store, map[channels.BindingKey]channels.Sender{{Channel: channels.Telegram, BindingID: "telegram-bot-b"}: &outboxSender{}}, WithSenderObserver(observer))
	if err != nil {
		t.Fatalf("NewChannelOutboxDispatcher() error = %v", err)
	}
	if _, err := dispatcher.DispatchTenant(context.Background(), "tenant-a", 1); err != nil {
		t.Fatalf("DispatchTenant() error = %v", err)
	}
	if len(observer.attributes) != 1 || observer.attributes[0] != (metrics.SenderAttributes{TenantID: "tenant-a", Channel: "telegram", BindingID: "telegram-bot-b"}) {
		t.Fatalf("sender telemetry attributes = %#v", observer.attributes)
	}
	if observer.errors[0] != nil {
		t.Fatalf("sender telemetry error = %v", observer.errors[0])
	}
}
