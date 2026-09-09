package admin

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/repository"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storagemigration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func tenantYAML(id string, configVersion int) []byte {
	return []byte(fmt.Sprintf(`schema_version: 1
tenants:
- tenant_id: %s
  name: %s
  enabled: true
  config_version: %d
  audit: {enabled: true, retention_days: 30, store_content: false}
  apps:
  - app_id: assistant
    name: Assistant
    enabled: true
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
`, id, id, configVersion))
}

func storageRouteYAML(id string, version int, cutover bool) []byte {
	session := `
        type: postgres
        endpoint: postgres://source.example/runtime
        migration_target:
          type: postgres
          endpoint: postgres://target.example/runtime`
	if cutover {
		session = `{type: postgres, endpoint: postgres://target.example/runtime}`
	}
	return []byte(fmt.Sprintf(`schema_version: 1
tenants:
- tenant_id: %s
  name: %s
  enabled: true
  config_version: %d
  audit: {enabled: true, retention_days: 30, store_content: false}
  apps:
  - app_id: assistant
    name: Assistant
    enabled: true
    config: {instruction: Help the user.}
    model: {provider: mock, name: mock}
    tools: {allow: [echo], deny: [], require_approval: []}
    channels: [{binding_id: http, type: http, provider_account_id: local, enabled: true}]
    storage:
      session: %s
      memory: {type: postgres}
      summary: %s
      artifact: {type: postgres}
      knowledge: {type: postgres}
      audit: {type: postgres}
`, id, id, version, session, session))
}

func TestStorageCutoverRequiresCompletedMatchingMigration(t *testing.T) {
	configs := repository.NewMemoryStore()
	migrations := storagemigration.NewMemoryStore()
	service, err := NewService(configs, WithMigrationStore(migrations))
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithActor(context.Background(), "ops")
	if _, err := service.Publish(ctx, "tenant-a", 0, storageRouteYAML("tenant-a", 1, false)); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Publish(ctx, "tenant-a", 1, storageRouteYAML("tenant-a", 2, true)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unverified cutover error=%v", err)
	}
	job, err := service.PlanMigration(ctx, "tenant-a", "assistant", storagemigration.DomainSession, 1)
	if err != nil {
		t.Fatal(err)
	}
	claim, ok, err := migrations.Claim(ctx, "worker", time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim=%v ok=%v", err, ok)
	}
	if err := migrations.Save(ctx, claim, storagemigration.Progress{Done: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Publish(ctx, "tenant-a", 1, storageRouteYAML("tenant-a", 2, true)); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Rollback(ctx, "tenant-a", 2, 1); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("unsafe storage rollback error=%v job=%s", err, job.JobID)
	}
}

func TestStorageRouteIdentityIncludesNamespace(t *testing.T) {
	left := tenant.BackendConfig{Type: tenant.BackendRedis, Endpoint: "redis://redis:6379/0", Namespace: "tenant-a"}
	right := left.Clone()
	right.Namespace = "tenant-b"
	if samePrimaryRoute(left, right) {
		t.Fatal("different Redis namespaces were treated as the same cutover route")
	}
}

func TestServicePublishIsolationAndRollback(t *testing.T) {
	service, err := NewService(repository.NewMemoryStore())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := service.Publish(ctx, "tenant-a", 0, tenantYAML("tenant-a", 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Publish(ctx, "tenant-b", 0, tenantYAML("tenant-b", 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Publish(ctx, "tenant-a", 1, tenantYAML("tenant-a", 2)); err != nil {
		t.Fatal(err)
	}

	a, err := service.Versions(ctx, "tenant-a")
	if err != nil || len(a) != 2 {
		t.Fatalf("tenant-a versions = %v, %v", a, err)
	}
	b, err := service.Versions(ctx, "tenant-b")
	if err != nil || len(b) != 1 {
		t.Fatalf("tenant-b versions = %v, %v", b, err)
	}
	rolled, err := service.Rollback(ctx, "tenant-a", 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if rolled.Version != 3 || rolled.RolledBackFrom == nil || *rolled.RolledBackFrom != 1 {
		t.Fatalf("rollback = %+v", rolled)
	}
}

func TestConcurrentPublishHasOneWinner(t *testing.T) {
	service, _ := NewService(repository.NewMemoryStore())
	ctx := context.Background()
	if _, err := service.Publish(ctx, "tenant-a", 0, tenantYAML("tenant-a", 1)); err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := service.Publish(ctx, "tenant-a", 1, tenantYAML("tenant-a", 2))
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	var successes, conflicts int
	for err := range results {
		if err == nil {
			successes++
		} else if errors.Is(err, repository.ErrVersionConflict) {
			conflicts++
		} else {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
}

func TestPublishRejectsCrossTenantAndInvalidConfig(t *testing.T) {
	service, _ := NewService(repository.NewMemoryStore())
	if _, err := service.Publish(context.Background(), "tenant-b", tenant.ConfigVersion(0), tenantYAML("tenant-a", 1)); err == nil {
		t.Fatal("cross-tenant publish succeeded")
	}
	if _, err := service.Publish(context.Background(), "tenant-a", 0, []byte("secret: plaintext")); err == nil {
		t.Fatal("invalid config published")
	}
}
