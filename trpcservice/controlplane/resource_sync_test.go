package controlplane

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"os"
)

func TestResourceProofCannotCoverUnverifiedSubjects(t *testing.T) {
	m := BackendMigration{ID: "m", SourceBindingID: "source", TargetBindingID: "target"}
	s := emptyResourceSync()
	s.Subjects["one"] = true
	s.Subjects["two"] = true
	RecordResourceProof(&s, m, "one", "verified")
	if validResourceProof(s, m) {
		t.Fatal("partial inventory accepted")
	}
	RecordResourceProof(&s, m, "two", "verified")
	if !validResourceProof(s, m) {
		t.Fatal("complete proof rejected")
	}
	s.Epoch++
	if validResourceProof(s, m) {
		t.Fatal("stale epoch accepted")
	}
}
func TestResourceSyncPostgresNestedReadsAndPersistence(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("isolated PostgreSQL not configured")
	}
	ctx := context.Background()
	repo, err := New(ctx, config.ControlPlaneConfig{Backend: "postgres", PostgresURL: dsn, AutoMigrate: true, MaxOpenConns: 1, MaxIdleConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(repo)
	provider := repo.(*PostgresRepository)
	if err := SeedBootstrap(ctx, provider.SQLDB(), DefaultBootstrapData()); err != nil {
		t.Fatal(err)
	}
	if err := provider.WithResourceSync(ctx, "tutorial-tenant", "tutorial-app", "session", func(ctx context.Context, s *ResourceSync, save func() error) error {
		if _, err := repo.GetTenant(ctx, "tutorial-tenant"); err != nil {
			return err
		}
		s.Aliases["synthetic"] = "generation"
		s.Subjects["synthetic"] = true
		return save()
	}); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := provider.SQLDB().QueryRowContext(ctx, "SELECT state FROM resource_sync WHERE tenant_id='tutorial-tenant' AND app_id='tutorial-app' AND resource_type='session'").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var s ResourceSync
	if json.Unmarshal(raw, &s) != nil || s.Aliases["synthetic"] != "generation" {
		t.Fatal("generation pointer was not durable")
	}
}
