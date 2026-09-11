package messaging

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/safego"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

type outboxSender struct{ calls int }

func (s *outboxSender) Send(_ context.Context, target channels.ReplyTarget, message channels.OutboundMessage) (channels.SendReceipt, error) {
	s.calls++
	if target.Channel != channels.Telegram || target.BindingID != "telegram-bot-b" || target.ConversationID != "chat-1" || message.Text != "reply" {
		return channels.SendReceipt{}, ErrInvalidEnvelope
	}
	return channels.SendReceipt{ExternalMessageID: "reply-1"}, nil
}

func TestChannelOutboxDispatcherSendsOnlyCommittedReplyAndMarksItDelivered(t *testing.T) {
	t.Parallel()

	store := storage.NewMemoryStateStore()
	_, err := store.RecordExecution(context.Background(), storage.ExecutionRecord{
		TenantID: "tenant-a", AppCode: "support", SessionKey: "tenant-a/support/telegram/chat-1",
		MessageID: "message-1", Channel: "telegram", BindingID: "telegram-bot-b", TraceID: "trace-1", Action: "agent.reply",
		Result: "queued", OutboxType: ChannelReplyEventType(channels.Telegram),
		OutboxPayload: []byte(`{"channel":"telegram","binding_id":"telegram-bot-b","conversation_id":"chat-1","text":"reply"}`),
	})
	if err != nil {
		t.Fatalf("RecordExecution() error = %v", err)
	}
	sender := &outboxSender{}
	unused := &outboxSender{}
	dispatcher, err := NewChannelOutboxDispatcher(store, map[channels.BindingKey]channels.Sender{
		{Channel: channels.Telegram, BindingID: "telegram-bot-a"}: unused,
		{Channel: channels.Telegram, BindingID: "telegram-bot-b"}: sender,
	})
	if err != nil {
		t.Fatalf("NewChannelOutboxDispatcher() error = %v", err)
	}
	if _, err := dispatcher.DispatchTenant(context.Background(), "tenant-a", 10); err != nil {
		t.Fatalf("Dispatch() error = %v", err)
	}
	if sender.calls != 1 {
		t.Fatalf("sender calls = %d, want 1", sender.calls)
	}
	if unused.calls != 0 {
		t.Fatalf("other binding sender calls = %d, want 0", unused.calls)
	}
	pending, err := store.ListPendingOutbox(context.Background(), "tenant-a", 10)
	if err != nil {
		t.Fatalf("ListPendingOutbox() error = %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending replies = %d, want 0 after acknowledged send", len(pending))
	}
}

type blockingOutboxSender struct {
	started chan struct{}
	release chan struct{}
}

func (s *blockingOutboxSender) Send(ctx context.Context, _ channels.ReplyTarget, _ channels.OutboundMessage) (channels.SendReceipt, error) {
	close(s.started)
	select {
	case <-ctx.Done():
		return channels.SendReceipt{}, ctx.Err()
	case <-s.release:
		return channels.SendReceipt{ExternalMessageID: "reply-slow"}, nil
	}
}

type panickingRenewOutboxStore struct{ *storage.MemoryStateStore }

func (s *panickingRenewOutboxStore) RenewOutboxDelivery(context.Context, string, string, string, time.Duration) error {
	panic("renew outbox lease panic")
}

func TestChannelOutboxDispatcherConvertsLeaseRenewPanicToError(t *testing.T) {
	base := storage.NewMemoryStateStore()
	_, err := base.RecordExecution(context.Background(), storage.ExecutionRecord{
		TenantID: "tenant-a", AppCode: "support", SessionKey: "tenant-a/support/session/chat-1",
		MessageID: "message-panic", Channel: "telegram", BindingID: "telegram-bot-b", TraceID: "trace-panic", Action: "agent.reply",
		Result: "queued", OutboxType: ChannelReplyEventType(channels.Telegram),
		OutboxPayload: []byte(`{"channel":"telegram","binding_id":"telegram-bot-b","conversation_id":"chat-1","text":"reply"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &panickingRenewOutboxStore{MemoryStateStore: base}
	sender := &blockingOutboxSender{started: make(chan struct{}), release: make(chan struct{})}
	dispatcher, err := NewChannelOutboxDispatcher(store, map[channels.BindingKey]channels.Sender{
		{Channel: channels.Telegram, BindingID: "telegram-bot-b"}: sender,
	})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.lease = 30 * time.Millisecond
	done := make(chan error, 1)
	go func() {
		_, dispatchErr := dispatcher.DispatchTenant(context.Background(), "tenant-a", 1)
		done <- dispatchErr
	}()
	<-sender.started
	select {
	case dispatchErr := <-done:
		var panicErr *safego.PanicError
		if !errors.As(dispatchErr, &panicErr) || panicErr.Component != "channel outbox lease renewer" {
			t.Fatalf("DispatchTenant() error = %#v", dispatchErr)
		}
	case <-time.After(time.Second):
		t.Fatal("DispatchTenant() did not stop after lease renew panic")
	}
}

func TestChannelOutboxDispatcherRenewsLeaseDuringSlowSend(t *testing.T) {
	store := storage.NewMemoryStateStore()
	_, err := store.RecordExecution(context.Background(), storage.ExecutionRecord{
		TenantID: "tenant-a", AppCode: "support", SessionKey: "tenant-a/support/telegram/chat-1",
		MessageID: "message-slow", Channel: "telegram", BindingID: "telegram-bot-b", TraceID: "trace-slow", Action: "agent.reply",
		Result: "queued", OutboxType: ChannelReplyEventType(channels.Telegram),
		OutboxPayload: []byte("{\"channel\":\"telegram\",\"binding_id\":\"telegram-bot-b\",\"conversation_id\":\"chat-1\",\"text\":\"reply\"}"),
	})
	if err != nil {
		t.Fatalf("RecordExecution() error = %v", err)
	}
	sender := &blockingOutboxSender{started: make(chan struct{}), release: make(chan struct{})}
	dispatcher, err := NewChannelOutboxDispatcher(store, map[channels.BindingKey]channels.Sender{
		{Channel: channels.Telegram, BindingID: "telegram-bot-b"}: sender,
	})
	if err != nil {
		t.Fatalf("NewChannelOutboxDispatcher() error = %v", err)
	}
	dispatcher.lease = 30 * time.Millisecond
	done := make(chan error, 1)
	go func() {
		_, dispatchErr := dispatcher.DispatchTenant(context.Background(), "tenant-a", 1)
		done <- dispatchErr
	}()
	<-sender.started

	time.Sleep(75 * time.Millisecond)
	claimed, err := store.ClaimPendingOutbox(context.Background(), "tenant-a", "competing-dispatcher", dispatcher.lease, 1)
	if err != nil {
		t.Fatalf("competitor ClaimPendingOutbox() error = %v", err)
	}
	if len(claimed) != 0 {
		t.Fatalf("competitor claimed a slow in-flight delivery: %#v", claimed)
	}
	close(sender.release)
	if err := <-done; err != nil {
		t.Fatalf("DispatchTenant() error = %v", err)
	}
}

type channelRecordingSender struct {
	channel channels.Channel
	calls   int
}

func (s *channelRecordingSender) Send(_ context.Context, target channels.ReplyTarget, _ channels.OutboundMessage) (channels.SendReceipt, error) {
	if target.Channel != s.channel {
		return channels.SendReceipt{}, ErrInvalidEnvelope
	}
	s.calls++
	return channels.SendReceipt{ExternalMessageID: string(s.channel) + "-sent"}, nil
}

func TestChannelOutboxDispatcherClaimsOnlyConfiguredChannels(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStateStore()
	for _, item := range []struct {
		channel channels.Channel
		id      string
	}{
		{channel: channels.Telegram, id: "telegram-message"},
		{channel: channels.WeCom, id: "wecom-message"},
	} {
		_, err := store.RecordExecution(context.Background(), storage.ExecutionRecord{
			TenantID: "tenant-a", AppCode: "support", SessionKey: "tenant-a/support/" + string(item.channel) + "/chat-1",
			MessageID: item.id, Channel: string(item.channel), BindingID: "bot", TraceID: item.id, Action: "agent.reply", Result: "queued",
			OutboxType:    ChannelReplyEventType(item.channel),
			OutboxPayload: []byte(`{"channel":"` + string(item.channel) + `","binding_id":"bot","app_code":"support","config_version":1,"conversation_id":"chat-1","text":"reply"}`),
		})
		if err != nil {
			t.Fatalf("RecordExecution(%s) error = %v", item.channel, err)
		}
	}

	telegramSender := &channelRecordingSender{channel: channels.Telegram}
	wecomSender := &channelRecordingSender{channel: channels.WeCom}
	dispatcher, err := NewChannelOutboxDispatcher(store, map[channels.BindingKey]channels.Sender{
		{Channel: channels.Telegram, BindingID: "bot"}: telegramSender,
		{Channel: channels.WeCom, BindingID: "bot"}:    wecomSender,
	}, WithChannels(channels.Telegram))
	if err != nil {
		t.Fatalf("NewChannelOutboxDispatcher() error = %v", err)
	}

	if got, err := dispatcher.DispatchTenant(context.Background(), "tenant-a", 10); err != nil || got != 1 {
		t.Fatalf("DispatchTenant() = %d, %v; want 1, nil", got, err)
	}
	if telegramSender.calls != 1 || wecomSender.calls != 0 {
		t.Fatalf("sender calls telegram=%d wecom=%d, want 1/0", telegramSender.calls, wecomSender.calls)
	}
	pending, err := store.ListPendingOutbox(context.Background(), "tenant-a", 10)
	if err != nil {
		t.Fatalf("ListPendingOutbox() error = %v", err)
	}
	if len(pending) != 1 || pending[0].Type != ChannelReplyEventType(channels.WeCom) {
		t.Fatalf("pending = %#v, want only WeCom reply", pending)
	}
}
