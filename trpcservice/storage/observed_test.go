package storage

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
)

type recordingStoreObserver struct {
	attributes []metrics.StoreAttributes
	errors     []error
}

func (o *recordingStoreObserver) StartStore(ctx context.Context, attributes metrics.StoreAttributes) (context.Context, func(error)) {
	o.attributes = append(o.attributes, attributes)
	return ctx, func(err error) { o.errors = append(o.errors, err) }
}

func TestObservedStateStoreRecordsOnlyStableOperationAttributes(t *testing.T) {
	observer := &recordingStoreObserver{}
	store, err := NewObservedStateStore(NewMemoryStateStore(), observer, "postgres")
	if err != nil {
		t.Fatalf("NewObservedStateStore() error = %v", err)
	}
	_, err = store.RecordExecution(context.Background(), ExecutionRecord{
		TenantID: "tenant-a", AppCode: "support", SessionKey: "tenant-a/support/telegram/chat-1", MessageID: "message-1", Channel: "telegram", BindingID: "telegram-bot", TraceID: "trace-1", Action: "agent.reply", Result: "queued", AuditDetail: "private content", OutboxType: "channel_reply", OutboxPayload: []byte(`{"text":"private content"}`),
	})
	if err != nil {
		t.Fatalf("RecordExecution() error = %v", err)
	}
	if len(observer.attributes) != 1 || observer.attributes[0] != (metrics.StoreAttributes{TenantID: "tenant-a", Backend: "postgres", Operation: "record_execution"}) {
		t.Fatalf("store telemetry attributes = %#v", observer.attributes)
	}
	if observer.errors[0] != nil {
		t.Fatalf("store telemetry error = %v", observer.errors[0])
	}
}

func TestObservedStateStorePreservesPageLocalMessageRouteCapability(t *testing.T) {
	delegate := NewMemoryStateStore()
	if _, err := delegate.RecordExecution(context.Background(), ExecutionRecord{
		TenantID: "tenant-a", AppCode: "support", SessionKey: "tenant-a/support/session/1",
		MessageID: "message-1", Channel: "web", BindingID: "web-console", ConversationID: "conversation-1", ConversationScope: "direct",
		ActorExternalUserID: "user-1", TriggerType: "direct", TraceID: "trace-1", Action: "agent.reply", Result: "queued", OutboxType: "channel_reply",
	}); err != nil {
		t.Fatalf("RecordExecution() error = %v", err)
	}
	observer := &recordingStoreObserver{}
	store, err := NewObservedStateStore(delegate, observer, "postgres")
	if err != nil {
		t.Fatalf("NewObservedStateStore() error = %v", err)
	}
	routes, err := store.ListInboundMessageRoutesByMessageIDs(context.Background(), "tenant-a", "tenant-a/support/session/1", []string{"message-1"})
	if err != nil {
		t.Fatalf("ListInboundMessageRoutesByMessageIDs() error = %v", err)
	}
	if len(routes) != 1 || routes[0].MessageID != "message-1" || routes[0].ActorExternalUserID != "user-1" {
		t.Fatalf("routes = %#v", routes)
	}
	if len(observer.attributes) != 1 || observer.attributes[0].Operation != "list_inbound_message_routes_by_ids" {
		t.Fatalf("store telemetry attributes = %#v", observer.attributes)
	}
}

func TestObservedStateStorePreservesSessionOwnershipCapability(t *testing.T) {
	delegate := NewMemoryStateStore()
	sessionKey, err := delegate.ResolveSession(context.Background(), SessionRoute{
		TenantID: "tenant-a", AppCode: "support", Channel: "telegram", BindingID: "tg-main",
		ConversationID: "42", ExternalUserID: "42", SubjectID: "external:telegram:tg-main:42", Scope: "direct",
	}, "tenant-a/support/session/anonymous")
	if err != nil {
		t.Fatal(err)
	}
	observer := &recordingStoreObserver{}
	store, err := NewObservedStateStore(delegate, observer, "postgres")
	if err != nil {
		t.Fatal(err)
	}
	var ownership SessionOwnershipStore = store
	claimable, err := ownership.ListClaimableSessions(context.Background(), "tenant-a", "telegram", "tg-main", "42", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimable) != 1 || claimable[0].SessionKey != sessionKey {
		t.Fatalf("claimable sessions = %#v", claimable)
	}
	if err := ownership.ClaimSession(context.Background(), "tenant-a", sessionKey, "platform-user-1"); err != nil {
		t.Fatal(err)
	}
	if err := ownership.EndChannelIdentityRoutes(context.Background(), "tenant-a", "telegram", "tg-main", "42"); err != nil {
		t.Fatal(err)
	}
	if len(observer.attributes) != 3 {
		t.Fatalf("ownership operation telemetry = %#v", observer.attributes)
	}
}
