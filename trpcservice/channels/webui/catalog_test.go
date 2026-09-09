package webui

import (
	"context"
	"testing"

	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

func TestStoreCatalogListsActiveWebUIDesks(t *testing.T) {
	store := tenant.NewMemoryStore()
	mustCatalogUpsert(t, store)
	desks, err := StoreCatalog{Store: store}.ListDesks(context.Background())
	if err != nil {
		t.Fatalf("ListDesks() error = %v", err)
	}
	if len(desks) != 1 {
		t.Fatalf("desks = %#v, want 1 active webui desk", desks)
	}
	got := desks[0]
	if got.RouteKey != "binding-a" || got.TenantName != "Tenant A" || got.AppName != "support" {
		t.Fatalf("desk = %#v", got)
	}
	if got.SessionBackend != "redis" || got.MemoryBackend != "pgvector" {
		t.Fatalf("backends = %#v", got)
	}
}

func TestStoreCatalogOmitsInactiveAndOtherChannels(t *testing.T) {
	store := tenant.NewMemoryStore()
	mustCatalogUpsert(t, store)
	inactive := tenant.Tenant{
		ID: "tenant-b", Name: "Tenant B", IsActive: false,
		Quota:  tenant.Quota{DailyTokenLimit: 1, RatePerMinute: 1},
		Policy: tenant.Policy{AuditLevel: "full"},
	}
	if err := store.UpsertTenant(context.Background(), inactive); err != nil {
		t.Fatalf("upsert inactive tenant: %v", err)
	}
	desks, err := StoreCatalog{Store: store}.ListDesks(context.Background())
	if err != nil {
		t.Fatalf("ListDesks() error = %v", err)
	}
	if len(desks) != 1 || desks[0].TenantID != "tenant-a" {
		t.Fatalf("desks = %#v", desks)
	}
}

func mustCatalogUpsert(t *testing.T, store tenant.ConfigStore) {
	t.Helper()
	tenantA := tenant.Tenant{
		ID: "tenant-a", Name: "Tenant A", IsActive: true,
		Quota:  tenant.Quota{DailyTokenLimit: 1, RatePerMinute: 1},
		Policy: tenant.Policy{AuditLevel: "full"},
	}
	app := tenant.AgentApp{
		ID: "app-a", TenantID: "tenant-a", AppName: "support",
		Model: tenant.ModelConfig{
			Provider: "openai-compatible", Model: "test", APIKeyRef: "env:KEY",
		},
		Backends: tenant.BackendSelection{Session: "redis", Memory: "pgvector"},
	}
	web := tenant.ChannelBinding{
		ID: "binding-a", TenantID: "tenant-a", AppID: "app-a",
		Channel: "webui", RouteKey: "binding-a", IsActive: true,
	}
	feishu := tenant.ChannelBinding{
		ID: "binding-feishu", TenantID: "tenant-a", AppID: "app-a",
		Channel: "feishu", RouteKey: "feishu-a", IsActive: true,
	}
	inactive := tenant.ChannelBinding{
		ID: "binding-off", TenantID: "tenant-a", AppID: "app-a",
		Channel: "webui", RouteKey: "binding-off", IsActive: false,
	}
	if err := store.UpsertTenant(context.Background(), tenantA); err != nil {
		t.Fatal(err)
	}
	if _, err := store.UpsertApp(context.Background(), app); err != nil {
		t.Fatalf("upsert app: %v", err)
	}
	for _, binding := range []tenant.ChannelBinding{web, feishu, inactive} {
		if err := store.UpsertBinding(context.Background(), binding); err != nil {
			t.Fatal(err)
		}
	}
}
