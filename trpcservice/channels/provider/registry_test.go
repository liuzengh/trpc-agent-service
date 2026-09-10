package provider

import (
	"context"
	"errors"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/outbox"
	storage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
)

func TestRegistryIsTenantChannelScoped(t *testing.T) {
	digest, err := channels.DigestPublicRouteKey(channels.ChannelWeCom, "registry-route")
	if err != nil {
		t.Fatal(err)
	}
	binding, err := channels.NewBinding(channels.CreateInput{TenantID: "t_00000000000000000000000000", BindingKey: "registry", Channel: channels.ChannelWeCom, ProviderAccountID: "corp", PublicRouteKeyDigest: digest, AppID: "app_00000000000000000000000000", SecretRef: "secret/wecom", Protocol: channels.ProtocolConfiguration{WeCom: &channels.WeComProtocolConfiguration{CorpID: "corp"}}})
	if err != nil {
		t.Fatal(err)
	}
	registry := NewRegistry()
	factory := registryProviderFactory{}
	if err := registry.Register(binding.TenantID, binding.Channel, " "+binding.ProviderAccountID+" ", factory); err != nil {
		t.Fatal(err)
	}
	resolved, err := registry.Resolve(context.Background(), *binding)
	if err != nil || resolved == nil {
		t.Fatalf("Resolve() = %v, %v", resolved, err)
	}
	foreign := binding.Clone()
	foreign.TenantID = "t_00000000000000000000000001"
	if _, err := registry.Resolve(context.Background(), foreign); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("foreign Resolve() = %v", err)
	}
}

func TestRegistryCancellationAndClose(t *testing.T) {
	registry := NewRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := registry.Resolve(ctx, channels.Binding{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Resolve() = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if err := registry.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
	if err := registry.Register("t_00000000000000000000000000", channels.ChannelWeCom, "corp", registryProviderFactory{}); !errors.Is(err, ErrProviderRegistryClosed) {
		t.Fatalf("Register after Close() = %v", err)
	}
	var nilRegistry *Registry
	if err := nilRegistry.Close(); err != nil {
		t.Fatalf("nil Close() = %v", err)
	}
}

func TestRegistryRemovalAndValidationBoundaries(t *testing.T) {
	const tenantID = "t_00000000000000000000000000"
	registry := NewRegistry()
	if err := registry.Register(tenantID, channels.ChannelWeCom, "corp", registryProviderFactory{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Remove(tenantID, channels.ChannelWeCom, "corp"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(tenantID, channels.ChannelWeCom, "", registryProviderFactory{}); !errors.Is(err, channels.ErrInvalid) {
		t.Fatalf("empty account Register() = %v", err)
	}
	if err := registry.Remove("", channels.ChannelWeCom, "corp"); !errors.Is(err, channels.ErrInvalid) {
		t.Fatalf("empty tenant Remove() = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if err := registry.Remove(tenantID, channels.ChannelWeCom, "corp"); !errors.Is(err, ErrProviderRegistryClosed) {
		t.Fatalf("closed Remove() = %v", err)
	}
	var nilRegistry *Registry
	if _, err := registry.Resolve(nil, channels.Binding{}); !errors.Is(err, channels.ErrInvalid) {
		t.Fatalf("nil context Resolve() = %v", err)
	}
	if _, err := registry.Resolve(context.Background(), channels.Binding{}); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("invalid binding Resolve() = %v", err)
	}
	if _, err := nilRegistry.Resolve(context.Background(), channels.Binding{}); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("nil registry Resolve() = %v", err)
	}
}

type registryProviderFactory struct{}

func (registryProviderFactory) New(context.Context, channels.Binding) (outbox.Provider, error) {
	return registryProvider{}, nil
}

type registryProvider struct{}

func (registryProvider) Deliver(context.Context, storage.ReplyOutbox) (string, error) {
	return "id", nil
}

func (registryProvider) Reconcile(context.Context, storage.ReplyOutbox) (outbox.DeliveryStatus, string, error) {
	return outbox.DeliveryAccepted, "id", nil
}
