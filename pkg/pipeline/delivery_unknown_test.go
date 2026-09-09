package pipeline

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
)

type unknownDeliveryAdapter struct{}

func (unknownDeliveryAdapter) VerifySignature(*http.Request, *tenant.ChannelBinding) error {
	return nil
}

func (unknownDeliveryAdapter) ParseInbound(*http.Request, *tenant.ChannelBinding) (*channel.InboundMessage, error) {
	return nil, errors.New("not used")
}

func (unknownDeliveryAdapter) SendReply(context.Context, *tenant.ChannelBinding, *channel.OutboundMessage) error {
	return channel.UnknownDeliveryError(errors.New("simulated provider transport"))
}

func (unknownDeliveryAdapter) SendStreamChunk(context.Context, *tenant.ChannelBinding, *channel.StreamChunk) error {
	return errors.New("not supported")
}

func (unknownDeliveryAdapter) SupportsStreaming() bool             { return false }
func (unknownDeliveryAdapter) HandleRateLimit(error) time.Duration { return 0 }

type unknownDeliveryStore struct {
	*reliable.MemoryStore
	blockCalls int
	retryCalls int
}

func TestOutboxDeliveryIDIsStablePerChunk(t *testing.T) {
	first := outboxDeliveryID(99, 0)
	if first != "trpc-outbox-v1-99-0" {
		t.Fatalf("first delivery id=%q", first)
	}
	if got := outboxDeliveryID(99, 0); got != first {
		t.Fatalf("same chunk changed delivery id from %q to %q", first, got)
	}
	if got := outboxDeliveryID(99, 1); got == first {
		t.Fatalf("different chunks reused delivery id %q", got)
	}
}

func (s *unknownDeliveryStore) BlockOutbox(ctx context.Context, id int64, lease reliable.Lease, cause error) error {
	s.blockCalls++
	return s.MemoryStore.BlockOutbox(ctx, id, lease, cause)
}

func (s *unknownDeliveryStore) RetryOutbox(ctx context.Context, id int64, lease reliable.Lease, cause error, next time.Time, cursor int) error {
	s.retryCalls++
	return s.MemoryStore.RetryOutbox(ctx, id, lease, cause, next, cursor)
}

func (s *unknownDeliveryStore) RetryOutboxAfter(ctx context.Context, id int64, lease reliable.Lease, cause error, delay time.Duration, cursor int) error {
	s.retryCalls++
	return s.MemoryStore.RetryOutboxAfter(ctx, id, lease, cause, delay, cursor)
}

func TestDeliveryBlocksUnknownProviderOutcome(t *testing.T) {
	store := &unknownDeliveryStore{MemoryStore: reliable.NewMemoryStore()}
	tenantService := &fakeTenantService{value: &tenant.Tenant{
		ID: "tenant-a", Status: tenant.TenantStatusActive,
		Channels: []tenant.ChannelBinding{{AccountID: "corp-a", Type: string(channel.ChannelTypeWeWork)}},
	}}
	registry := channel.NewAdapterRegistry()
	registry.Register(channel.ChannelTypeWeWork, unknownDeliveryAdapter{})
	delivery, err := NewDelivery(store, tenantService, registry, DeliveryConfig{
		Owner: "delivery-test", LeaseDuration: 6 * time.Second, DeliveryTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	inbox := pipelineTestInbox()
	if _, err := store.EnqueueInbox(context.Background(), inbox); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimInbox(context.Background(), "consumer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteInbox(context.Background(), claim.ID, claim.Lease, reliable.OutboxReply{Content: "reply"}); err != nil {
		t.Fatal(err)
	}
	deliveryClaim, err := store.ClaimOutbox(context.Background(), "delivery-test", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	delivery.deliverOne(context.Background(), deliveryClaim)
	if store.blockCalls != 1 {
		t.Fatalf("unknown provider outcome block calls=%d, want 1", store.blockCalls)
	}
	if store.retryCalls != 0 {
		t.Fatalf("unknown provider outcome retry calls=%d, want 0", store.retryCalls)
	}
	if _, err := store.ClaimOutbox(context.Background(), "delivery-2", time.Minute); !errors.Is(err, reliable.ErrNoWork) {
		t.Fatalf("unknown provider outcome remained automatically claimable: %v", err)
	}
}
