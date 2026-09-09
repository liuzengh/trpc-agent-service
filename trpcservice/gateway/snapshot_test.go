package gateway

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/repository"
)

func publishedTenantYAML(id string, version int, enabled bool) []byte {
	return []byte(fmt.Sprintf(`schema_version: 1
tenants:
- tenant_id: %s
  name: %s
  enabled: %t
  config_version: %d
  audit: {enabled: true, retention_days: 30, store_content: false}
  apps:
  - app_id: assistant
    name: Assistant
    enabled: %t
    config: {instruction: Help the user.}
    model: {provider: mock, name: mock}
    tools: {allow: [echo], deny: [], require_approval: []}
    channels:
    - {binding_id: http, type: http, provider_account_id: local, enabled: true}
    storage:
      session: {type: inmemory}
      memory: {type: inmemory}
      summary: {type: inmemory}
      artifact: {type: inmemory}
      knowledge: {type: inmemory}
      audit: {type: inmemory}
`, id, id, enabled, version, enabled))
}

func TestStoreSnapshotResolverPinsImmutableVersions(t *testing.T) {
	store := repository.NewMemoryStore()
	ctx := context.Background()
	if _, err := store.PublishConfig(ctx, repository.ConfigRecord{TenantID: "tenant-a", Payload: publishedTenantYAML("tenant-a", 1, true)}, 0); err != nil {
		t.Fatal(err)
	}
	// v2 disables the app: new requests must be rejected, old pins must still work.
	if _, err := store.PublishConfig(ctx, repository.ConfigRecord{TenantID: "tenant-a", Payload: publishedTenantYAML("tenant-a", 2, false)}, 1); err != nil {
		t.Fatal(err)
	}
	published, err := config.NewPublishedCache(store)
	if err != nil {
		t.Fatal(err)
	}
	resolver := StoreSnapshotResolver{Published: published}
	old, err := resolver.Resolve(ctx, "tenant-a", "assistant", 1)
	if err != nil {
		t.Fatalf("old pinned version must stay resolvable: %v", err)
	}
	if old.Version() != 1 {
		t.Fatalf("version=%d", old.Version())
	}
	if _, err := resolver.Resolve(ctx, "tenant-a", "assistant", 2); err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("disabled version must reject new work: %v", err)
	}
	if _, err := resolver.Resolve(ctx, "tenant-a", "assistant", 99); err == nil {
		t.Fatal("unknown version must fail")
	}
	if _, err := resolver.Resolve(ctx, "tenant-b", "assistant", 1); err == nil {
		t.Fatal("cross-tenant resolve must fail")
	}
}

func TestStoreSnapshotResolverCurrentFollowsPublishedHead(t *testing.T) {
	store := repository.NewMemoryStore()
	ctx := context.Background()
	if _, err := store.PublishConfig(ctx, repository.ConfigRecord{TenantID: "tenant-a", Payload: publishedTenantYAML("tenant-a", 1, true)}, 0); err != nil {
		t.Fatal(err)
	}
	published, _ := config.NewPublishedCache(store)
	if file, err := published.Current(ctx, "tenant-a"); err != nil || file.Tenants[0].ConfigVersion != 1 {
		t.Fatalf("current=%v err=%v", file, err)
	}
	if _, err := store.PublishConfig(ctx, repository.ConfigRecord{TenantID: "tenant-a", Payload: publishedTenantYAML("tenant-a", 2, true)}, 1); err != nil {
		t.Fatal(err)
	}
	// The head is never cached: a publish is visible without a restart.
	file, err := published.Current(ctx, "tenant-a")
	if err != nil || file.Tenants[0].ConfigVersion != 2 {
		t.Fatalf("current after publish=%v err=%v", file, err)
	}
}
