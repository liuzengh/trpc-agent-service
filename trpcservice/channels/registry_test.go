package channels

import (
	"context"
	"errors"
	"testing"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
)

type registryAdapter struct{ id string }

func (a *registryAdapter) ID() string              { return a.id }
func (*registryAdapter) Run(context.Context) error { return nil }
func (*registryAdapter) PublicRoute(context.Context, channel.CallbackRequest) (channel.PublicRouteHint, error) {
	return channel.PublicRouteHint{}, nil
}
func (*registryAdapter) Verify(context.Context, channel.CallbackRequest, channel.ScopedVerifierHandle) (channel.VerifiedCallback, channel.VerificationReceipt, error) {
	return channel.VerifiedCallback{}, channel.VerificationReceipt{}, nil
}
func (*registryAdapter) Decode(context.Context, channel.VerifiedCallback) ([]channel.ProviderEvent, error) {
	return nil, nil
}
func (*registryAdapter) Deliver(context.Context, channel.DeliveryRequest) (channel.DeliveryResult, error) {
	return channel.DeliveryResult{Delivered: true}, nil
}
func (*registryAdapter) Capabilities() channel.Capabilities { return channel.Capabilities{Text: true} }

func TestRegistryIsTenantScopedAndImmutable(t *testing.T) {
	adapter := &registryAdapter{id: "fake"}
	registry, err := NewRegistry(BindingRuntime{TenantID: "tenant-a", ChannelBindingID: "binding", Channel: "fake", ExternalAccountID: "account", Adapter: adapter})
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := registry.ResolveAdapter(context.Background(), "tenant-a", "binding")
	if err != nil || resolved != adapter {
		t.Fatalf("resolved=%T err=%v", resolved, err)
	}
	if _, err := registry.ResolveAdapter(context.Background(), "tenant-b", "binding"); !errors.Is(err, runtime.ErrNotFound) {
		t.Fatalf("cross-tenant resolve err=%v", err)
	}
	if err := registry.Register(BindingRuntime{TenantID: "tenant-a", ChannelBindingID: "binding", Channel: "fake", ExternalAccountID: "other", Adapter: adapter}); !errors.Is(err, runtime.ErrIdempotencyCollision) {
		t.Fatalf("replacement err=%v", err)
	}
}
