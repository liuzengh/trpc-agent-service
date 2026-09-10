package gateway

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
)

func TestIdempotencyStorePendingCompletedAndFailedLifecycle(t *testing.T) {
	now := time.Date(2026, 8, 23, 13, 0, 0, 0, time.UTC)
	clock := now
	store, err := NewIdempotencyStore(IdempotencyConfig{TTL: time.Minute, MaxEntries: 4, Now: func() time.Time { return clock }})
	if err != nil {
		t.Fatal(err)
	}
	fixture := newGatewayFixture(t)
	principal := mustAPIPrincipal(t, fixture.tenant.TenantID, fixture.app.AppID)
	message := InboundMessage{Content: "hello", ExternalMessageID: "message-1", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}
	runIdempotencyCompletionLifecycle(t, store, principal, message)
	runIdempotencyFailureAndExpiryLifecycle(t, store, principal, message, &clock)
}

func runIdempotencyCompletionLifecycle(t *testing.T, store *IdempotencyStore, principal Principal, message InboundMessage) {
	t.Helper()
	claim, replay, err := store.Begin(context.Background(), principal, message)
	if err != nil || claim == nil || replay != nil {
		t.Fatalf("first idempotency claim = %v %v %v", claim, replay, err)
	}
	if _, _, err := store.Begin(context.Background(), principal, message); !errors.Is(err, ErrDuplicateMessage) {
		t.Fatalf("pending duplicate error = %v", err)
	}
	events := []DispatchEvent{{Type: DispatchEventMessage, RequestID: "request-1", Text: "hello"}, {Type: DispatchEventDone, RequestID: "request-1", Status: "complete", Done: true}}
	if err := claim.Complete(events); err != nil {
		t.Fatal(err)
	}
	if err := claim.Fail(); err != nil {
		t.Fatal(err)
	}
	_, replay, err = store.Begin(context.Background(), principal, message)
	if err != nil || len(replay) != 2 || replay[0].Text != "hello" {
		t.Fatalf("completed replay = %+v, err=%v", replay, err)
	}
	replay[0].Text = "mutated"
	_, replay, err = store.Begin(context.Background(), principal, message)
	if err != nil || replay[0].Text != "hello" {
		t.Fatalf("stored replay leaked mutable events = %+v, err=%v", replay, err)
	}
}

func runIdempotencyFailureAndExpiryLifecycle(t *testing.T, store *IdempotencyStore, principal Principal, message InboundMessage, clock *time.Time) {
	t.Helper()
	failedMessage := message
	failedMessage.ExternalMessageID = "message-failed"
	failedClaim, _, err := store.Begin(context.Background(), principal, failedMessage)
	if err != nil {
		t.Fatal(err)
	}
	if err := failedClaim.Fail(); err != nil {
		t.Fatal(err)
	}
	if retry, replay, err := store.Begin(context.Background(), principal, failedMessage); err != nil || retry == nil || replay != nil {
		t.Fatalf("failed key was not retryable: claim=%v replay=%v err=%v", retry, replay, err)
	}

	*clock = (*clock).Add(time.Minute)
	message.ExternalMessageID = "message-1"
	if fresh, replay, err := store.Begin(context.Background(), principal, message); err != nil || fresh == nil || replay != nil {
		t.Fatalf("expired completed key was not pruned: claim=%v replay=%v err=%v", fresh, replay, err)
	}
	if !store.Ready() {
		t.Fatal("open idempotency store is not ready")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if store.Ready() {
		t.Fatal("closed idempotency store is ready")
	}
	if _, _, err := store.Begin(context.Background(), principal, message); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed store error = %v", err)
	}
}

func TestIdempotencyStoreScopesCapacityAndConfigurationEdges(t *testing.T) {
	fixture := newGatewayFixture(t)
	principal := mustAPIPrincipal(t, fixture.tenant.TenantID, fixture.app.AppID)
	message := InboundMessage{Content: "hello", ExternalUserID: "user", ConversationKind: channels.ConversationDirect, ExternalPeerID: "peer"}
	runIdempotencyConfigurationEdges(t, principal, message)
	runIdempotencyCapacityAndIsolation(t, principal, message)
	runIdempotencyChannelKeyEdges(t, principal, message)
}

func runIdempotencyConfigurationEdges(t *testing.T, principal Principal, message InboundMessage) {
	t.Helper()
	if _, err := NewIdempotencyStore(IdempotencyConfig{TTL: -time.Second}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative TTL error = %v", err)
	}
	if _, err := NewIdempotencyStore(IdempotencyConfig{MaxEntries: -1}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("negative capacity error = %v", err)
	}
	var nilStore *IdempotencyStore
	if nilStore.Ready() {
		t.Fatal("nil idempotency store is ready")
	}
	var nilContext context.Context
	if _, _, err := nilStore.Begin(nilContext, principal, message); !errors.Is(err, ErrNotReady) {
		t.Fatalf("nil store error = %v", err)
	}
	emptyStore, err := NewIdempotencyStore(IdempotencyConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := emptyStore.Begin(nilContext, principal, message); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil context store error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	store, err := NewIdempotencyStore(IdempotencyConfig{MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.Begin(canceled, principal, message); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled store error = %v", err)
	}
	if _, _, err := store.Begin(context.Background(), principal, message); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing message ID error = %v", err)
	}
	if _, _, err := store.Begin(context.Background(), Principal{}, func() InboundMessage {
		value := message
		value.ExternalMessageID = "message-invalid-principal"
		return value
	}()); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("invalid principal error = %v", err)
	}

}

func runIdempotencyCapacityAndIsolation(t *testing.T, principal Principal, message InboundMessage) {
	t.Helper()
	store, err := NewIdempotencyStore(IdempotencyConfig{MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	firstMessage := message
	firstMessage.ExternalMessageID = "first"
	claim, _, err := store.Begin(context.Background(), principal, firstMessage)
	if err != nil {
		t.Fatal(err)
	}
	secondMessage := message
	secondMessage.ExternalMessageID = "second"
	if _, _, err := store.Begin(context.Background(), principal, secondMessage); !errors.Is(err, ErrIdempotencyCapacity) {
		t.Fatalf("capacity error = %v", err)
	}
	if err := claim.Fail(); err != nil {
		t.Fatal(err)
	}

	secondFixture := newGatewayFixture(t)
	secondPrincipal := mustAPIPrincipal(t, secondFixture.tenant.TenantID, secondFixture.app.AppID)
	isolationStore, err := NewIdempotencyStore(IdempotencyConfig{MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	if other, _, err := isolationStore.Begin(context.Background(), secondPrincipal, secondMessage); err != nil || other == nil {
		t.Fatalf("second tenant was not isolated: claim=%v err=%v", other, err)
	}

}

func runIdempotencyChannelKeyEdges(t *testing.T, principal Principal, message InboundMessage) {
	t.Helper()
	channelFixture := newGatewayFixture(t)
	target := newTrustedRoutingTarget(t, channelFixture)
	channelPrincipal, err := NewChannelPrincipal(target)
	if err != nil {
		t.Fatal(err)
	}
	channelMessage := InboundMessage{Content: "hello", ExternalMessageID: "channel-message", ExternalUserID: "user", ConversationKind: channels.ConversationGroup, ExternalChatID: "chat"}
	if key, err := makeIdempotencyKey(channelPrincipal, channelMessage); err != nil || !strings.Contains(key, target.BindingID) {
		t.Fatalf("channel idempotency key = %q, err=%v", key, err)
	}
	isolationStore, err := NewIdempotencyStore(IdempotencyConfig{MaxEntries: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := isolationStore.Begin(context.Background(), principal, InboundMessage{Content: "bad", ExternalMessageID: "id"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unnormalized idempotency message error = %v", err)
	}
}
