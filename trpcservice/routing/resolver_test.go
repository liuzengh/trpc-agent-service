package routing

import (
	"context"
	"encoding/json"
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

func TestControlPlaneResolverDeterministicCanary(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	canary := data.Revisions[0]
	canary.ID = "tutorial-revision-canary"
	canary.RevisionNo = 2
	canary.Checksum = controlplane.RevisionChecksum(canary)
	data.Revisions = append(data.Revisions, canary)
	data.Apps[0].RolloutPolicy = json.RawMessage(`{
        "canary_revision_id":"tutorial-revision-canary",
        "canary_percent":100,
        "salt":"rollout-1"
    }`)
	repository := controlplane.NewMemoryRepository(data)
	t.Cleanup(func() { _ = repository.Close() })
	resolver, _ := NewControlPlaneResolver(repository)
	stable, err := resolver.Resolve(context.Background(), "tutorial-http")
	if err != nil || stable.RevisionID != "tutorial-revision-1" {
		t.Fatalf("stable=%+v err=%v", stable, err)
	}
	first, err := resolver.ResolveFor(context.Background(), "tutorial-http", "alice/session")
	if err != nil || first.RevisionID != canary.ID {
		t.Fatalf("canary=%+v err=%v", first, err)
	}
	second, _ := resolver.ResolveFor(context.Background(), "tutorial-http", "alice/session")
	if second.RevisionID != first.RevisionID {
		t.Fatalf("canary routing is not deterministic: %+v %+v", first, second)
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
