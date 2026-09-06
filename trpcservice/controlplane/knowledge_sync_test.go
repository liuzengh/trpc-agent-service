package controlplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func TestKnowledgeSyncMemoryContract(t *testing.T) {
	repo := NewMemoryRepository(DefaultBootstrapData())
	defer repo.Close()
	testKnowledgeSync(t, repo, repo, "tutorial-tenant", "tutorial-app")
}
func TestKnowledgeSyncPostgresContract(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("isolated PostgreSQL not configured")
	}
	r, err := New(context.Background(), config.ControlPlaneConfig{Backend: "postgres", PostgresURL: dsn, AutoMigrate: true, MaxOpenConns: 10, MaxIdleConns: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	other, err := New(context.Background(), config.ControlPlaneConfig{Backend: "postgres", PostgresURL: dsn, MaxOpenConns: 10, MaxIdleConns: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	id := fmt.Sprintf("knowledge-%x", time.Now().UnixNano())
	now := time.Now().UTC()
	mutable := r.(MutableRepository)
	if err := mutable.CreateTenant(context.Background(), Tenant{ID: id, DisplayName: id, Status: StatusActive, Region: "test", QuotaConfig: json.RawMessage(`{}`), AuditPolicy: json.RawMessage(`{}`), Version: 1, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	if err := mutable.CreateAgentApp(context.Background(), AgentApp{ID: id, TenantID: id, Name: id, Status: StatusActive, RolloutPolicy: json.RawMessage(`{}`), Version: 1, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	testKnowledgeSync(t, r.(KnowledgeSyncRepository), other.(KnowledgeSyncRepository), id, id)
	policy := r.(TenantPolicyRepository)
	if err := policy.UpdateTenantPolicies(context.Background(), id, 1, []byte(`{"daily_prompt_tokens":100}`), []byte(`{"level":"security"}`), "synthetic-admin", ""); err != nil {
		t.Fatal(err)
	}
	if err := policy.UpdateTenantPolicies(context.Background(), id, 1, []byte(`{}`), []byte(`{}`), "synthetic-admin", ""); !errors.Is(err, ErrConflict) {
		t.Fatal("stale policy version accepted")
	}
	tenant, err := r.GetTenant(context.Background(), id)
	if err != nil || tenant.Version != 2 {
		t.Fatal("policy update missing")
	}
}
func testKnowledgeSync(t *testing.T, a, b KnowledgeSyncRepository, tenantID, appID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := a
			if i%2 == 0 {
				r = b
			}
			err := r.WithKnowledgeSync(ctx, tenantID, appID, func(_ context.Context, s *KnowledgeSync, save func() error) error { s.Epoch++; return save() })
			if err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if err := b.WithKnowledgeSync(ctx, tenantID, appID, func(_ context.Context, s *KnowledgeSync, _ func() error) error {
		if s.Epoch != 24 {
			t.Fatalf("lost concurrent epoch: %d", s.Epoch)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := a.WithKnowledgeSync(ctx, tenantID, appID, func(_ context.Context, s *KnowledgeSync, save func() error) error {
		s.Intents["pending"] = KnowledgeIntent{RevisionID: "test", Deleted: true}
		if err := save(); err != nil {
			return err
		}
		return errors.New("crashed after durable intent")
	}); err == nil {
		t.Fatal("synthetic failure lost")
	}
	if err := b.WithKnowledgeSync(ctx, tenantID, appID, func(_ context.Context, s *KnowledgeSync, _ func() error) error {
		if !s.Intents["pending"].Deleted {
			t.Fatal("repair intent lost across instances")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
