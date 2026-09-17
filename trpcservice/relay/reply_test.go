package relay

import (
	"context"
	"errors"
	"testing"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging"
	memory "github.com/liuzengh/trpc-agent-service/trpcservice/storage/messaging/inmemory"
)

type replyPublisherStub struct {
	destination channel.ReplyDestination
	events      []channel.ReplyEvent
	err         error
}

func (s *replyPublisherStub) PublishReply(_ context.Context, destination channel.ReplyDestination, event channel.ReplyEvent) error {
	s.destination = destination
	s.events = append(s.events, event)
	return s.err
}

func TestReplyRelayPublishesStableReplyEventThenMarks(t *testing.T) {
	store := memory.New()
	result := messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://request", ContentDigest: "digest", ContentType: messaging.ContentTypeCard, Content: []byte(`{"header":{"title":"done"}}`), KeyVersion: 1}
	if err := store.PutResult(context.Background(), result); err != nil {
		t.Fatal(err)
	}
	if err := store.PutReplyRoute(messaging.ReplyRoute{TenantID: "tenant", RequestID: "request", Channel: "fake", ChannelBindingID: "binding", ExternalAccountID: "account", ExternalMessageID: "message", ConfigVersion: 2}); err != nil {
		t.Fatal(err)
	}
	outbox := &outboxStub{records: []messaging.OutboxRecord{{TenantID: "tenant", OutboxID: "outbox", Kind: "reply", AggregateID: "request", IdempotencyKey: "r1_key", PayloadRef: result.ResultRef, EventSeq: 7, Version: 2}}}
	publisher := &replyPublisherStub{}
	r := ReplyRelay{Outbox: outbox, Results: store, Routes: store, Replies: publisher, Owner: "reply-relay"}
	count, err := r.RunOnce(context.Background())
	if err != nil || count != 1 || !outbox.published || len(publisher.events) != 1 {
		t.Fatalf("count=%d err=%v published=%t events=%#v", count, err, outbox.published, publisher.events)
	}
	event := publisher.events[0]
	if event.DeliveryKey != "r1_key" || event.ContentRef != result.ResultRef || event.ContentType != messaging.ContentTypeCard || event.ChannelBindingID != "binding" || publisher.destination.ExternalAccountID != "account" {
		t.Fatalf("destination=%#v event=%#v", publisher.destination, event)
	}
}

func TestReplyRelayDoesNotPublishMismatchedResultReference(t *testing.T) {
	store := memory.New()
	_ = store.PutResult(context.Background(), messaging.ResultRecord{TenantID: "tenant", RequestID: "request", ResultRef: "result://right", ContentDigest: "digest", Content: []byte("done"), KeyVersion: 1})
	_ = store.PutReplyRoute(messaging.ReplyRoute{TenantID: "tenant", RequestID: "request", Channel: "fake", ChannelBindingID: "binding", ExternalAccountID: "account", ConfigVersion: 1})
	outbox := &outboxStub{records: []messaging.OutboxRecord{{TenantID: "tenant", OutboxID: "outbox", Kind: "reply", AggregateID: "request", IdempotencyKey: "r1_key", PayloadRef: "result://wrong", Version: 2}}}
	publisher := &replyPublisherStub{}
	r := ReplyRelay{Outbox: outbox, Results: store, Routes: store, Replies: publisher, Owner: "reply-relay"}
	count, err := r.RunOnce(context.Background())
	if count != 0 || !errors.Is(err, runtime.ErrTenantScope) || !outbox.retried || len(publisher.events) != 0 {
		t.Fatalf("count=%d err=%v retried=%t events=%d", count, err, outbox.retried, len(publisher.events))
	}
}

func TestReplyRelayPublishesEncryptedInteractionBeforeTerminalResult(t *testing.T) {
	store := memory.New()
	interaction := messaging.InteractionRecord{TenantID: "tenant", RequestID: "request", ContentRef: "confirmation://tenant/conf_1",
		ContentDigest: "interaction-digest", Content: []byte(`{"kind":"tool_confirmation"}`), KeyVersion: 1}
	if err := store.PutInteraction(context.Background(), interaction); err != nil {
		t.Fatal(err)
	}
	_ = store.PutReplyRoute(messaging.ReplyRoute{TenantID: "tenant", RequestID: "request", Channel: "fake", ChannelBindingID: "binding", ExternalAccountID: "account", ConfigVersion: 1})
	outbox := &outboxStub{records: []messaging.OutboxRecord{{TenantID: "tenant", OutboxID: "confirmation-outbox", Kind: "reply", AggregateID: "request",
		IdempotencyKey: "confirmation-reply", PayloadRef: interaction.ContentRef, EventSeq: 1, Version: 1}}}
	publisher := &replyPublisherStub{}
	count, err := (ReplyRelay{Outbox: outbox, Results: store, Routes: store, Replies: publisher, Owner: "reply-relay"}).RunOnce(context.Background())
	if err != nil || count != 1 || len(publisher.events) != 1 || publisher.events[0].ContentRef != interaction.ContentRef {
		t.Fatalf("count=%d events=%#v err=%v", count, publisher.events, err)
	}
}
