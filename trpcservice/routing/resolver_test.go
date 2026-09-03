package routing

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

func TestControlPlaneResolver(t *testing.T) {
	repository := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	t.Cleanup(func() { _ = repository.Close() })
	resolver, err := NewControlPlaneResolver(repository)
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	scope, err := resolver.Resolve(context.Background(), "tutorial-http")
	if err != nil {
		t.Fatalf("resolve binding: %v", err)
	}
	if scope.TenantID != "tutorial-tenant" || scope.AppID != "tutorial-app" ||
		scope.RevisionID != "tutorial-revision-1" {
		t.Fatalf("unexpected scope: %+v", scope)
	}
}

func TestControlPlaneResolverDoesNotAcceptUnknownBinding(t *testing.T) {
	repository := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	t.Cleanup(func() { _ = repository.Close() })
	resolver, err := NewControlPlaneResolver(repository)
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	if _, err := resolver.Resolve(context.Background(), "unknown"); !errors.Is(err, ErrBindingNotFound) {
		t.Fatalf("resolve error = %v", err)
	}
}

func TestControlPlaneResolverRejectsDisabledTenant(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.Tenants[0].Status = controlplane.StatusSuspended
	repository := controlplane.NewMemoryRepository(data)
	t.Cleanup(func() { _ = repository.Close() })
	resolver, err := NewControlPlaneResolver(repository)
	if err != nil {
		t.Fatalf("new resolver: %v", err)
	}
	if _, err := resolver.Resolve(context.Background(), "tutorial-http"); !errors.Is(err, ErrRouteDisabled) {
		t.Fatalf("resolve disabled tenant error = %v", err)
	}
}
