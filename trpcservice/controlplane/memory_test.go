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

func TestMemoryRepositoryUpdatesChannelBindingWithVersion(t *testing.T) {
	repository := NewMemoryRepository(DefaultBootstrapData())
	t.Cleanup(func() { _ = repository.Close() })
	updated, err := repository.UpdateChannelBinding(
		context.Background(), "tutorial-tenant", "tutorial-http-binding",
		[]byte(`{"require_mention":true}`), StatusDisabled, 1,
	)
	if err != nil || updated.Version != 2 || updated.Status != StatusDisabled ||
		string(updated.Config) != `{"require_mention":true}` {
		t.Fatalf("updated=%+v err=%v", updated, err)
	}
	byCallback, err := repository.GetChannelBindingByCallbackKey(
		context.Background(), "tutorial-http",
	)
	if err != nil || byCallback.Version != 2 || byCallback.Status != StatusDisabled {
		t.Fatalf("callback binding=%+v err=%v", byCallback, err)
	}
	if _, err := repository.UpdateChannelBinding(
		context.Background(), "tutorial-tenant", "tutorial-http-binding",
		[]byte(`{}`), StatusActive, 1,
	); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale update error=%v", err)
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
