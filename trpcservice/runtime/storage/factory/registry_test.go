package factory

import (
	"context"
	"errors"
	"testing"

	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
)

func TestProviderRegistryIsTenantCapabilityScoped(t *testing.T) {
	registry := NewProviderRegistry()
	input := StorageFactoryInput{TenantID: "t_00000000000000000000000000"}
	binding := CapabilityBinding{Capability: CapabilitySession, Provider: "inmemory"}
	factory := &registryCapabilityProvider{}
	if err := registry.Register(input.TenantID, binding.Capability, binding.Provider, factory); err != nil {
		t.Fatal(err)
	}
	resolved, err := registry.Resolve(context.Background(), input, binding)
	if err != nil || resolved != factory {
		t.Fatalf("Resolve() = %v, %v", resolved, err)
	}
	foreign := input
	foreign.TenantID = "t_00000000000000000000000001"
	if _, err := registry.Resolve(context.Background(), foreign, binding); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("foreign Resolve() = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := registry.Resolve(ctx, input, binding); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Resolve() = %v", err)
	}
}

func TestProviderRegistryCloseFailsClosed(t *testing.T) {
	registry := NewProviderRegistry()
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("t_00000000000000000000000000", CapabilitySession, "memory", &registryCapabilityProvider{}); !errors.Is(err, ErrRegistryClosed) {
		t.Fatalf("Register after Close() = %v", err)
	}
}

func TestProviderRegistryRemovalAndValidationBoundaries(t *testing.T) {
	const tenantID = "t_00000000000000000000000000"
	registry := NewProviderRegistry()
	binding := CapabilityBinding{Capability: CapabilitySession, Provider: "memory"}
	input := StorageFactoryInput{TenantID: tenantID}
	if err := registry.Register(tenantID, CapabilitySession, "memory", &registryCapabilityProvider{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Remove(tenantID, CapabilitySession, "memory"); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Resolve(context.Background(), input, binding); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("removed Resolve() = %v", err)
	}
	if err := registry.Register(tenantID, CapabilitySession, "", &registryCapabilityProvider{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty provider Register() = %v", err)
	}
	if err := registry.Remove("", CapabilitySession, "memory"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty tenant Remove() = %v", err)
	}
	var nilRegistry *ProviderRegistry
	if _, err := registry.Resolve(nil, input, binding); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil context Resolve() = %v", err)
	}
	if err := registry.Register(tenantID, CapabilitySession, "memory", nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil provider Register() = %v", err)
	}
	if err := registry.Register(tenantID, "", "memory", &registryCapabilityProvider{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty capability Register() = %v", err)
	}
	if _, err := nilRegistry.Resolve(context.Background(), input, binding); !errors.Is(err, ErrProviderUnavailable) {
		t.Fatalf("nil registry Resolve() = %v", err)
	}
	if err := registry.Close(); err != nil {
		t.Fatal(err)
	}
	if err := registry.Remove(tenantID, CapabilitySession, "memory"); !errors.Is(err, ErrRegistryClosed) {
		t.Fatalf("closed Remove() = %v", err)
	}
}

type registryCapabilityProvider struct{}

func (registryCapabilityProvider) New(context.Context, StorageFactoryInput, CapabilityBinding, modelprofile.SecretValue) (any, error) {
	return struct{}{}, nil
}
