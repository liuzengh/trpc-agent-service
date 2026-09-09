package repository_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/repository"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func openIntegrationDB(t *testing.T) (*sql.DB, context.Context) {
	t.Helper()
	dsn := os.Getenv("TRPC_AGENT_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set TRPC_AGENT_POSTGRES_TEST_DSN to run PostgreSQL integration tests")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	if err := repository.Migrate(ctx, func(ctx context.Context, script string) error {
		_, err := db.ExecContext(ctx, script)
		return err
	}, repository.DirectionUp); err != nil {
		t.Fatal(err)
	}
	return db, ctx
}

func configPayload(tenantID string, version int) []byte {
	return []byte(fmt.Sprintf(`schema_version: 1
tenants:
- tenant_id: %s
  name: %s
  enabled: false
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
`, tenantID, tenantID, version))
}

func TestSQLStorePublishListCurrentAndRollback(t *testing.T) {
	db, ctx := openIntegrationDB(t)
	store, err := repository.NewSQLStore(db)
	if err != nil {
		t.Fatal(err)
	}
	tenantID := fmt.Sprintf("repo-%d", time.Now().UnixNano())

	first, err := store.PublishConfig(ctx, repository.ConfigRecord{TenantID: tenantID, TenantName: tenantID, TenantEnabled: false, Payload: configPayload(tenantID, 1), SHA256: "v1-hash", CreatedBy: "ops"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if first.Version != 1 || first.CreatedBy != "ops" {
		t.Fatalf("first=%+v", first)
	}
	if _, err := store.PublishConfig(ctx, repository.ConfigRecord{TenantID: tenantID, Payload: configPayload(tenantID, 2), SHA256: "v2-hash"}, 1); err != nil {
		t.Fatal(err)
	}
	// Stale expected_version loses with a conflict.
	if _, err := store.PublishConfig(ctx, repository.ConfigRecord{TenantID: tenantID, Payload: configPayload(tenantID, 2), SHA256: "dup"}, 1); !errors.Is(err, repository.ErrVersionConflict) {
		t.Fatalf("stale publish err=%v", err)
	}
	current, err := store.GetCurrentConfig(ctx, tenantID)
	if err != nil || current.Version != 2 || current.SHA256 != "v2-hash" {
		t.Fatalf("current=%+v err=%v", current, err)
	}
	target := tenant.ConfigVersion(1)
	rolled, err := store.PublishConfig(ctx, repository.ConfigRecord{TenantID: tenantID, Payload: configPayload(tenantID, 3), SHA256: "v3-hash", RolledBackFrom: &target, CreatedBy: "ops"}, 2)
	if err != nil {
		t.Fatal(err)
	}
	if rolled.Version != 3 || rolled.RolledBackFrom == nil || *rolled.RolledBackFrom != 1 {
		t.Fatalf("rolled=%+v", rolled)
	}
	versions, err := store.ListConfigVersions(ctx, tenantID)
	if err != nil || len(versions) != 3 {
		t.Fatalf("versions=%v err=%v", versions, err)
	}
	// History is immutable: v1 keeps its original hash and actor.
	if versions[2].Version != 1 || versions[2].SHA256 != "v1-hash" || versions[2].CreatedBy != "ops" {
		t.Fatalf("history mutated=%+v", versions[2])
	}
	// Cross-tenant isolation.
	other, err := store.ListConfigVersions(ctx, tenantID+"-other")
	if err != nil || len(other) != 0 {
		t.Fatalf("cross tenant=%v err=%v", other, err)
	}
	if _, err := store.GetCurrentConfig(ctx, tenantID+"-other"); !errors.Is(err, repository.ErrNotFound) {
		t.Fatalf("cross tenant current err=%v", err)
	}
}

func TestSQLStoreConcurrentPublishSingleWinner(t *testing.T) {
	db, ctx := openIntegrationDB(t)
	store, err := repository.NewSQLStore(db)
	if err != nil {
		t.Fatal(err)
	}
	tenantID := fmt.Sprintf("race-%d", time.Now().UnixNano())
	if _, err := store.PublishConfig(ctx, repository.ConfigRecord{TenantID: tenantID, Payload: configPayload(tenantID, 1), SHA256: "v1"}, 0); err != nil {
		t.Fatal(err)
	}
	const racers = 6
	var wg sync.WaitGroup
	results := make(chan error, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			_, err := store.PublishConfig(ctx, repository.ConfigRecord{TenantID: tenantID, Payload: configPayload(tenantID, 2), SHA256: fmt.Sprintf("v2-%d", index)}, 1)
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)
	succeeded, conflicted := 0, 0
	for err := range results {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, repository.ErrVersionConflict):
			conflicted++
		default:
			t.Fatalf("unexpected err=%v", err)
		}
	}
	if succeeded != 1 || conflicted != racers-1 {
		t.Fatalf("succeeded=%d conflicted=%d", succeeded, conflicted)
	}
	current, err := store.GetCurrentConfig(ctx, tenantID)
	if err != nil || current.Version != 2 {
		t.Fatalf("current=%+v err=%v", current, err)
	}
}
