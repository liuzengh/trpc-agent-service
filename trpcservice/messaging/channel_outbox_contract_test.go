package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

type contractSender struct {
	messages []channels.OutboundMessage
	err      error
}

func (s *contractSender) Send(_ context.Context, _ channels.ReplyTarget, message channels.OutboundMessage) (channels.SendReceipt, error) {
	s.messages = append(s.messages, message)
	if s.err != nil {
		return channels.SendReceipt{}, s.err
	}
	return channels.SendReceipt{ExternalMessageID: "sent-1"}, nil
}

type contractResolver struct {
	sender       channels.Sender
	err          error
	materialized []channels.OutboundFile
	materialErr  error
	cleaned      bool
}

func (r *contractResolver) ResolveSender(context.Context, string, string, uint64, channels.BindingKey) (channels.Sender, error) {
	return r.sender, r.err
}

func (r *contractResolver) MaterializeOutboundArtifacts(context.Context, string, string, uint64, []OutboundArtifactRef) ([]channels.OutboundFile, func(), error) {
	if r.materialErr != nil {
		return nil, nil, r.materialErr
	}
	return r.materialized, func() { r.cleaned = true }, nil
}

type contractDeliveryPolicy struct {
	segments []string
	segErr   error
	waitErr  error
	waits    int
}

func (p *contractDeliveryPolicy) Segments(channels.Channel, string) ([]string, error) {
	if p.segErr != nil {
		return nil, p.segErr
	}
	return append([]string(nil), p.segments...), nil
}

func (p *contractDeliveryPolicy) Wait(context.Context, channels.ReplyTarget) error {
	p.waits++
	return p.waitErr
}

func claimChannelReply(t *testing.T, store *storage.MemoryStateStore, dispatcher *ChannelOutboxDispatcher, payload channelReplyPayload) storage.OutboxEvent {
	t.Helper()
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	record, err := store.RecordExecution(context.Background(), storage.ExecutionRecord{
		TenantID: "support", AppCode: "assistant", SessionKey: "support/assistant/session-1",
		MessageID: "message-" + string(payload.Channel), Channel: string(payload.Channel), BindingID: payload.BindingID,
		TraceID: "trace-1", Action: "agent.reply", Result: "queued",
		OutboxType: ChannelReplyEventType(payload.Channel), OutboxPayload: encoded,
	})
	if err != nil {
		t.Fatalf("RecordExecution() error = %v", err)
	}
	events, err := store.ClaimPendingOutboxByType(context.Background(), "support", dispatcher.owner, dispatcher.lease, 1, ChannelReplyEventType(payload.Channel))
	if err != nil || len(events) != 1 {
		t.Fatalf("ClaimPendingOutboxByType() = %+v, %v", events, err)
	}
	if events[0].ID != record.ID {
		t.Fatalf("claimed event = %q, want %q", events[0].ID, record.ID)
	}
	return events[0]
}

func TestChannelOutboxOptionsValidateAndDeduplicate(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStateStore()
	sender := &contractSender{}
	key := channels.BindingKey{Channel: channels.Telegram, BindingID: "support-main"}

	if _, err := NewChannelOutboxDispatcher(store, nil); err == nil {
		t.Fatal("empty senders error = nil")
	}
	if _, err := NewChannelOutboxDispatcher(store, map[channels.BindingKey]channels.Sender{{Channel: channels.Channel("unknown"), BindingID: "x"}: sender}); err == nil {
		t.Fatal("invalid sender key error = nil")
	}
	if _, err := NewChannelOutboxDispatcher(store, map[channels.BindingKey]channels.Sender{key: nil}); err == nil {
		t.Fatal("nil sender error = nil")
	}
	if _, err := NewChannelOutboxDispatcher(store, map[channels.BindingKey]channels.Sender{key: sender}, nil); err == nil {
		t.Fatal("nil option error = nil")
	}
	if _, err := NewChannelOutboxDispatcher(store, map[channels.BindingKey]channels.Sender{key: sender}, WithChannels()); err == nil {
		t.Fatal("empty WithChannels error = nil")
	}
	if _, err := NewChannelOutboxDispatcher(store, map[channels.BindingKey]channels.Sender{key: sender}, WithChannels(channels.Channel("unknown"))); err == nil {
		t.Fatal("unsupported WithChannels error = nil")
	}
	if _, err := NewChannelOutboxDispatcher(store, map[channels.BindingKey]channels.Sender{key: sender}, WithDeliveryPolicy(nil)); err == nil {
		t.Fatal("nil delivery policy error = nil")
	}
	if _, err := NewChannelOutboxDispatcher(store, map[channels.BindingKey]channels.Sender{key: sender}, WithSenderObserver(nil)); err == nil {
		t.Fatal("nil sender observer error = nil")
	}

	dispatcher, err := NewChannelOutboxDispatcher(store, map[channels.BindingKey]channels.Sender{key: sender}, WithChannels(channels.Telegram, channels.Telegram))
	if err != nil {
		t.Fatal(err)
	}
	if len(dispatcher.channels) != 1 || dispatcher.channels[0] != channels.Telegram {
		t.Fatalf("deduplicated channels = %+v", dispatcher.channels)
	}
	if resolved, err := dispatcher.resolver.ResolveSender(context.Background(), "support", "assistant", 1, key); err != nil || resolved != sender {
		t.Fatalf("ResolveSender() = %T, %v", resolved, err)
	}
	if _, err := dispatcher.resolver.ResolveSender(context.Background(), "support", "assistant", 1, channels.BindingKey{Channel: channels.Telegram, BindingID: "missing"}); err == nil {
		t.Fatal("ResolveSender(missing) error = nil")
	}
}

func TestNewChannelOutboxDispatcherWithResolverValidatesDependencies(t *testing.T) {
	t.Parallel()
	type stateOnly struct{ storage.StateStore }
	if _, err := NewChannelOutboxDispatcherWithResolver(stateOnly{StateStore: storage.NewMemoryStateStore()}, &contractResolver{}); err == nil {
		t.Fatal("state store without delivery leases error = nil")
	}
	if _, err := NewChannelOutboxDispatcherWithResolver(storage.NewMemoryStateStore(), nil); err == nil {
		t.Fatal("nil resolver error = nil")
	}
}

func TestChannelOutboxDispatchValidatesClaimAndPayload(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStateStore()
	resolver := &contractResolver{sender: &contractSender{}}
	dispatcher, err := NewChannelOutboxDispatcherWithResolver(store, resolver)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := dispatcher.Dispatch(context.Background(), storage.OutboxEvent{DeliveryOwner: "other"}); err == nil {
		t.Fatal("foreign claim error = nil")
	}
	if _, err := dispatcher.Dispatch(context.Background(), storage.OutboxEvent{DeliveryOwner: dispatcher.owner, Payload: []byte("not-json")}); err == nil {
		t.Fatal("invalid JSON error = nil")
	}
	validPayload := channelReplyPayload{Channel: channels.Telegram, BindingID: "support-main", ConversationID: "customer-chat", Text: "客服回复"}
	encoded, _ := json.Marshal(validPayload)
	if _, err := dispatcher.Dispatch(context.Background(), storage.OutboxEvent{DeliveryOwner: dispatcher.owner, Type: ChannelReplyEventType(channels.Feishu), Payload: encoded}); err == nil {
		t.Fatal("mismatched event type error = nil")
	}
	for _, payload := range []channelReplyPayload{
		{Channel: channels.Channel("unknown"), BindingID: "support-main", ConversationID: "customer-chat", Text: "reply"},
		{Channel: channels.Telegram, BindingID: "support-main", ConversationID: "", Text: "reply"},
		{Channel: channels.Telegram, BindingID: "support-main", ConversationID: "customer-chat", Text: " "},
	} {
		encoded, _ := json.Marshal(payload)
		if _, err := dispatcher.Dispatch(context.Background(), storage.OutboxEvent{DeliveryOwner: dispatcher.owner, Type: ChannelReplyEventType(payload.Channel), Payload: encoded}); err == nil {
			t.Fatalf("invalid payload %+v error = nil", payload)
		}
	}
	webBadBinding := channelReplyPayload{Channel: channels.Telegram, BindingID: "", ConversationID: "customer-chat", Text: "reply"}
	encoded, _ = json.Marshal(webBadBinding)
	if _, err := dispatcher.Dispatch(context.Background(), storage.OutboxEvent{DeliveryOwner: dispatcher.owner, Type: ChannelReplyEventType(channels.Telegram), Payload: encoded}); err == nil {
		t.Fatal("invalid binding error = nil")
	}

	wantErr := errors.New("resolver unavailable")
	resolver.err = wantErr
	event := claimChannelReply(t, store, dispatcher, validPayload)
	if _, err := dispatcher.Dispatch(context.Background(), event); !errors.Is(err, wantErr) {
		t.Fatalf("resolver error = %v", err)
	}
}

func TestChannelOutboxDispatchFallsBackToEventIDForIdempotency(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStateStore()
	sender := &contractSender{}
	dispatcher, _ := NewChannelOutboxDispatcherWithResolver(store, &contractResolver{sender: sender})
	payload := channelReplyPayload{Channel: channels.Telegram, BindingID: "support-main", AppCode: "assistant", ConfigVersion: 1, ConversationID: "customer-chat", Text: "客服回复"}
	event := claimChannelReply(t, store, dispatcher, payload)
	event.RequestID = ""
	if _, err := dispatcher.Dispatch(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if len(sender.messages) != 1 || sender.messages[0].IdempotencyKey != event.ID {
		t.Fatalf("idempotency key = %+v", sender.messages)
	}
}

func TestChannelOutboxDispatchPreservesInteractiveCard(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStateStore()
	sender := &contractSender{}
	dispatcher, _ := NewChannelOutboxDispatcherWithResolver(store, &contractResolver{sender: sender})
	payload := channelReplyPayload{
		Channel: channels.Web, BindingID: "web-console", AppCode: "assistant", ConfigVersion: 1, ConversationID: "conversation-1", WebOwnerID: "user-1",
		Card: &channels.InteractiveCard{Title: "订单信息", Body: "已找到订单", Actions: []channels.CardAction{{Label: "查看", URL: "https://support.example.test/orders/42"}}},
	}
	event := claimChannelReply(t, store, dispatcher, payload)
	if _, err := dispatcher.Dispatch(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if len(sender.messages) != 1 || sender.messages[0].Card == nil || sender.messages[0].Card.Title != "订单信息" || sender.messages[0].Text != "" {
		t.Fatalf("delivered messages = %#v", sender.messages)
	}
}

func TestChannelOutboxDispatchRecordsSendFailureForRetry(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStateStore()
	wantErr := errors.New("channel temporarily unavailable")
	sender := &contractSender{err: wantErr}
	dispatcher, _ := NewChannelOutboxDispatcherWithResolver(store, &contractResolver{sender: sender})
	payload := channelReplyPayload{Channel: channels.Telegram, BindingID: "support-main", ConversationID: "customer-chat", Text: "客服回复"}
	event := claimChannelReply(t, store, dispatcher, payload)
	if _, err := dispatcher.Dispatch(context.Background(), event); !errors.Is(err, wantErr) {
		t.Fatalf("Dispatch(send error) = %v", err)
	}
	stored, err := store.FindOutboxByRequestID(context.Background(), "support", event.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.DeliveryOwner != "" || !strings.Contains(stored.LastDeliveryError, "temporarily unavailable") {
		t.Fatalf("failed outbox event = %+v", stored)
	}
}

func TestChannelOutboxDispatchReportsReceiptPersistenceFailure(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStateStore()
	sender := &contractSender{}
	dispatcher, _ := NewChannelOutboxDispatcherWithResolver(store, &contractResolver{sender: sender})
	payload := channelReplyPayload{Channel: channels.Telegram, BindingID: "support-main", ConversationID: "customer-chat", Text: "客服回复"}
	encoded, _ := json.Marshal(payload)
	event := storage.OutboxEvent{
		ID: "missing-event", TenantID: "support", Type: ChannelReplyEventType(channels.Telegram),
		Payload: encoded, DeliveryOwner: dispatcher.owner,
	}
	if _, err := dispatcher.Dispatch(context.Background(), event); err == nil || !strings.Contains(err.Error(), "persist channel send receipt") {
		t.Fatalf("Dispatch(receipt persistence) error = %v", err)
	}
}

func TestChannelOutboxDispatchMaterializesArtifactsForIM(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStateStore()
	sender := &contractSender{}
	resolver := &contractResolver{sender: sender, materialized: []channels.OutboundFile{{Path: "/tmp/support.txt", Name: "support.txt"}}}
	dispatcher, _ := NewChannelOutboxDispatcherWithResolver(store, resolver)
	payload := channelReplyPayload{
		Channel: channels.Telegram, BindingID: "support-main", AppCode: "assistant", ConfigVersion: 2,
		ConversationID: "customer-chat", Text: "客服回复",
		Artifacts: []OutboundArtifactRef{{Filename: "user:support.txt", Version: 1, UserID: "customer-1", SessionID: "session-1"}},
	}
	event := claimChannelReply(t, store, dispatcher, payload)
	if _, err := dispatcher.Dispatch(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if !resolver.cleaned || len(sender.messages) != 2 || len(sender.messages[1].Files) != 1 {
		t.Fatalf("materialized delivery cleaned=%v messages=%+v", resolver.cleaned, sender.messages)
	}

	resolver.materialErr = errors.New("artifact unavailable")
	store2 := storage.NewMemoryStateStore()
	dispatcher2, _ := NewChannelOutboxDispatcherWithResolver(store2, resolver)
	event = claimChannelReply(t, store2, dispatcher2, payload)
	if _, err := dispatcher2.Dispatch(context.Background(), event); err == nil || !strings.Contains(err.Error(), "materialize outbound artifacts") {
		t.Fatalf("materialization error = %v", err)
	}

	plainResolver := &recordingChannelResolver{sender: &contractSender{}}
	store3 := storage.NewMemoryStateStore()
	dispatcher3, _ := NewChannelOutboxDispatcherWithResolver(store3, plainResolver)
	event = claimChannelReply(t, store3, dispatcher3, payload)
	if _, err := dispatcher3.Dispatch(context.Background(), event); err == nil || !strings.Contains(err.Error(), "cannot materialize") {
		t.Fatalf("missing materializer error = %v", err)
	}
}

func TestChannelOutboxDispatchPreservesArtifactMetadataForWeb(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStateStore()
	sender := &contractSender{}
	resolver := &contractResolver{sender: sender}
	dispatcher, _ := NewChannelOutboxDispatcherWithResolver(store, resolver)
	payload := channelReplyPayload{
		Channel: channels.Web, BindingID: "web-console", AppCode: "assistant", ConfigVersion: 2,
		ConversationID: "conversation-1", WebOwnerID: "user-1", Text: "文档已生成",
		Artifacts: []OutboundArtifactRef{{Filename: "report.docx", Version: 3, Name: "维修报告", MimeType: "application/vnd.openxmlformats-officedocument.wordprocessingml.document"}},
	}
	event := claimChannelReply(t, store, dispatcher, payload)
	if _, err := dispatcher.Dispatch(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if resolver.cleaned || len(sender.messages) != 1 || len(sender.messages[0].Files) != 0 || len(sender.messages[0].Artifacts) != 1 {
		t.Fatalf("web artifact delivery cleaned=%v messages=%+v", resolver.cleaned, sender.messages)
	}
	artifact := sender.messages[0].Artifacts[0]
	if artifact.Filename != "report.docx" || artifact.Version != 3 || artifact.Name != "维修报告" {
		t.Fatalf("web artifact = %+v", artifact)
	}
}

func TestChannelOutboxSendWithLeaseSegmentsAndFiles(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStateStore()
	sender := &contractSender{}
	policy := &contractDeliveryPolicy{segments: []string{"第一段", "第二段"}}
	dispatcher, _ := NewChannelOutboxDispatcherWithResolver(store, &contractResolver{sender: sender}, WithDeliveryPolicy(policy))
	payload := channelReplyPayload{Channel: channels.Telegram, BindingID: "support-main", ConversationID: "customer-chat", Text: "完整回复"}
	event := claimChannelReply(t, store, dispatcher, payload)

	receipt, err := dispatcher.sendWithLease(context.Background(), event, sender,
		channels.ReplyTarget{Channel: channels.Telegram, BindingID: "support-main", ConversationID: "customer-chat"},
		channels.OutboundMessage{Text: "完整回复", IdempotencyKey: "request-1", UpdateMessageID: "progress-1", Files: []channels.OutboundFile{{Name: "answer.txt"}}},
	)
	if err != nil || receipt.ExternalMessageID != "sent-1" {
		t.Fatalf("sendWithLease() = %+v, %v", receipt, err)
	}
	if policy.waits != 2 || len(sender.messages) != 3 {
		t.Fatalf("waits=%d messages=%+v", policy.waits, sender.messages)
	}
	if sender.messages[0].IdempotencyKey != "request-1:part:0" || sender.messages[0].UpdateMessageID != "progress-1" {
		t.Fatalf("first segment = %+v", sender.messages[0])
	}
	if sender.messages[1].IdempotencyKey != "request-1:part:1" || sender.messages[1].UpdateMessageID != "" {
		t.Fatalf("second segment = %+v", sender.messages[1])
	}
	if sender.messages[2].IdempotencyKey != "request-1:files" || len(sender.messages[2].Files) != 1 || sender.messages[2].Text != "" {
		t.Fatalf("file message = %+v", sender.messages[2])
	}
}

func TestChannelOutboxSendWithLeaseReportsPolicyErrors(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStateStore()
	sender := &contractSender{}
	dispatcher, _ := NewChannelOutboxDispatcherWithResolver(store, &contractResolver{sender: sender})
	event := storage.OutboxEvent{TenantID: "support", ID: "event-1"}
	dispatcher.lease = 0
	if _, err := dispatcher.sendWithLease(context.Background(), event, sender, channels.ReplyTarget{}, channels.OutboundMessage{Text: "reply"}); err == nil {
		t.Fatal("non-positive lease error = nil")
	}

	dispatcher.lease = time.Hour
	wantErr := errors.New("segment failure")
	dispatcher.policy = &contractDeliveryPolicy{segErr: wantErr}
	if _, err := dispatcher.sendWithLease(context.Background(), event, sender, channels.ReplyTarget{}, channels.OutboundMessage{Text: "reply"}); !errors.Is(err, wantErr) {
		t.Fatalf("segment error = %v", err)
	}
	wantErr = errors.New("pace failure")
	dispatcher.policy = &contractDeliveryPolicy{segments: []string{"reply"}, waitErr: wantErr}
	if _, err := dispatcher.sendWithLease(context.Background(), event, sender, channels.ReplyTarget{}, channels.OutboundMessage{Text: "reply"}); !errors.Is(err, wantErr) {
		t.Fatalf("pace error = %v", err)
	}
}

func TestChannelOutboxDispatchTenantValidationAndEmptyQueue(t *testing.T) {
	t.Parallel()
	store := storage.NewMemoryStateStore()
	dispatcher, _ := NewChannelOutboxDispatcherWithResolver(store, &contractResolver{sender: &contractSender{}})
	if _, err := dispatcher.DispatchTenant(context.Background(), "support", 0); err == nil {
		t.Fatal("non-positive limit error = nil")
	}
	if got, err := dispatcher.DispatchTenant(context.Background(), "support", 3); err != nil || got != 0 {
		t.Fatalf("DispatchTenant(empty) = %d, %v", got, err)
	}
}
