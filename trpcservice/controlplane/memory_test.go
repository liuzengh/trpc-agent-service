package controlplane

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryRepositoryDefaultBootstrap(t *testing.T) {
	repository := NewMemoryRepository(DefaultBootstrapData())
	t.Cleanup(func() { _ = repository.Close() })
	tenant, err := repository.GetTenant(context.Background(), "tutorial-tenant")
	if err != nil || tenant.Status != StatusActive {
		t.Fatalf("tenant = %+v, err = %v", tenant, err)
	}
	app, err := repository.GetAgentApp(context.Background(), tenant.ID, "tutorial-app")
	if err != nil || app.StableRevisionID == "" {
		t.Fatalf("app = %+v, err = %v", app, err)
	}
	revision, err := repository.GetStableRevision(context.Background(), tenant.ID, app.ID)
	if err != nil || revision.ID != app.StableRevisionID || revision.Checksum == "" {
		t.Fatalf("revision = %+v, err = %v", revision, err)
	}
	binding, err := repository.GetChannelBindingByCallbackKey(
		context.Background(),
		"tutorial-http",
	)
	if err != nil || binding.TenantID != tenant.ID || binding.AppID != app.ID {
		t.Fatalf("binding = %+v, err = %v", binding, err)
	}
	backends, err := repository.ListBackendBindings(context.Background(), tenant.ID, app.ID)
	if err != nil || len(backends) != 3 {
		t.Fatalf("backends = %+v, err = %v", backends, err)
	}
}

func TestMemoryRepositoryTenantIsolation(t *testing.T) {
	repository := NewMemoryRepository(DefaultBootstrapData())
	t.Cleanup(func() { _ = repository.Close() })
	if _, err := repository.GetAgentApp(
		context.Background(),
		"other-tenant",
		"tutorial-app",
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant lookup error = %v", err)
	}
	if _, err := repository.GetRevision(
		context.Background(),
		"other-tenant",
		"tutorial-revision-1",
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant revision lookup error = %v", err)
	}
}

func TestMemoryRepositoryReturnsCopies(t *testing.T) {
	repository := NewMemoryRepository(DefaultBootstrapData())
	t.Cleanup(func() { _ = repository.Close() })
	first, err := repository.GetTenant(context.Background(), "tutorial-tenant")
	if err != nil {
		t.Fatalf("get tenant: %v", err)
	}
	first.QuotaConfig[0] = '['
	second, err := repository.GetTenant(context.Background(), "tutorial-tenant")
	if err != nil {
		t.Fatalf("get tenant again: %v", err)
	}
	if string(second.QuotaConfig) != "{}" {
		t.Fatalf("repository data was mutated: %s", second.QuotaConfig)
	}
}

func TestMemoryRepositoryClose(t *testing.T) {
	repository := NewMemoryRepository(DefaultBootstrapData())
	if err := repository.Close(); err != nil {
		t.Fatalf("close repository: %v", err)
	}
	if err := repository.Ready(context.Background()); !errors.Is(err, ErrRepositoryClosed) {
		t.Fatalf("ready after close error = %v", err)
	}
}
