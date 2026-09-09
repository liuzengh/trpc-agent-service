package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/repository"
)

// TestFeishuBindingPublishDisableRollbackAndIsolation drives the full
// control-plane loop for Feishu bindings against a real PostgreSQL: publish,
// provider resolution, disable, rollback, and two tenants sharing one
// binding_id and one Feishu app_id without leaking across tenants.
func TestFeishuBindingPublishDisableRollbackAndIsolation(t *testing.T) {
	dsn := os.Getenv("TRPC_AGENT_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("set TRPC_AGENT_POSTGRES_TEST_DSN to run PostgreSQL integration tests")
	}
	t.Setenv("PR10_FEISHU_VERIFICATION_TOKEN", "verification-fixture")
	t.Setenv("PR10_FEISHU_APP_SECRET", "app-secret-fixture")
	t.Setenv("PR10_FEISHU_ENCRYPT_KEY", "encrypt-fixture")

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	// Register connection cleanup before fixture cleanup. Testing cleanups run
	// in LIFO order, so the later fixture cleanup still has a live connection.
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := repository.Migrate(ctx, func(ctx context.Context, script string) error {
		_, err := db.ExecContext(ctx, script)
		return err
	}, repository.DirectionUp); err != nil {
		t.Fatal(err)
	}

	store, err := repository.NewSQLStore(db)
	if err != nil {
		t.Fatal(err)
	}
	service, err := admin.NewService(store)
	if err != nil {
		t.Fatal(err)
	}
	published, err := config.NewPublishedCache(store)
	if err != nil {
		t.Fatal(err)
	}
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	tenantA, tenantB := "feishu-a-"+suffix, "feishu-b-"+suffix
	t.Cleanup(func() {
		// These tenants are unique to this test. Removing their complete control-
		// plane fixture keeps a persistent Compose test database production-
		// bootable: retaining an enabled historical payload with InMemory routes
		// would correctly trip production storage fail-fast on the next restart.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		tx, cleanupErr := db.BeginTx(cleanupCtx, nil)
		if cleanupErr != nil {
			t.Errorf("begin fixture cleanup: %v", cleanupErr)
			return
		}
		defer tx.Rollback()
		for _, statement := range []string{
			`DELETE FROM audit_logs WHERE tenant_id IN ($1,$2)`,
			`DELETE FROM channel_bindings WHERE tenant_id IN ($1,$2)`,
			`DELETE FROM agent_apps WHERE tenant_id IN ($1,$2)`,
			`DELETE FROM config_versions WHERE tenant_id IN ($1,$2)`,
			`DELETE FROM tenants WHERE tenant_id IN ($1,$2)`,
		} {
			if _, cleanupErr = tx.ExecContext(cleanupCtx, statement, tenantA, tenantB); cleanupErr != nil {
				t.Errorf("clean fixture: %v", cleanupErr)
				return
			}
		}
		if cleanupErr = tx.Commit(); cleanupErr != nil {
			t.Errorf("commit fixture cleanup: %v", cleanupErr)
		}
	})
	provider := feishuBindingProvider(db, published)
	payload := func(tenantID string, version int, enabled bool) []byte {
		return feishuTenantPayload(tenantID, version, "cli_shared_app", enabled)
	}
	// The payload fixture uses the shared binding_id "feishu-a" for every
	// tenant, which exercises cross-tenant disambiguation below.
	if _, err := service.Publish(ctx, tenantA, 0, payload(tenantA, 1, true)); err != nil {
		t.Fatal(err)
	}
	candidates := provider("feishu-a")
	resolvedA, ok := candidateForTenant(candidates, tenantA)
	if !ok {
		t.Fatalf("candidates=%+v", candidates)
	}
	if resolvedA.VerificationToken != "verification-fixture" || resolvedA.EncryptKey != "encrypt-fixture" || resolvedA.FeishuAppID != "cli_shared_app" || resolvedA.ConfigVersion != 1 {
		t.Fatalf("resolved=%+v", resolvedA)
	}

	// Disable: new callbacks resolve nothing.
	if _, err := service.Publish(ctx, tenantA, 1, payload(tenantA, 2, false)); err != nil {
		t.Fatal(err)
	}
	if got := provider("feishu-a"); hasTenantCandidate(got, tenantA) {
		t.Fatalf("disabled candidates=%+v", got)
	}

	// Rollback creates a new version and re-enables the binding.
	rolled, err := service.Rollback(ctx, tenantA, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	if rolled.Version != 3 || rolled.RolledBackFrom == nil || *rolled.RolledBackFrom != 1 {
		t.Fatalf("rolled=%+v", rolled)
	}
	candidates = provider("feishu-a")
	resolvedA, ok = candidateForTenant(candidates, tenantA)
	if !ok || resolvedA.ConfigVersion != 3 {
		t.Fatalf("rollback candidates=%+v", candidates)
	}

	// A second tenant with the same binding_id and the same Feishu app_id is
	// materialized independently; both candidates resolve with their own
	// tenant scope.
	if _, err := service.Publish(ctx, tenantB, 0, payload(tenantB, 1, true)); err != nil {
		t.Fatal(err)
	}
	candidates = provider("feishu-a")
	seen := map[string]bool{}
	for _, candidate := range candidates {
		seen[candidate.TenantID] = true
	}
	if !seen[tenantA] || !seen[tenantB] {
		t.Fatalf("tenant scope=%v", seen)
	}

	// The materialized row is tenant-scoped in PostgreSQL.
	var count int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM channel_bindings WHERE tenant_id=$1 AND binding_id='feishu-a' AND channel_type='feishu'`, tenantA).Scan(&count); err != nil || count != 1 {
		t.Fatalf("channel_bindings count=%d err=%v", count, err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM channel_bindings WHERE tenant_id=$1`, tenantB).Scan(&count); err != nil || count != 1 {
		t.Fatalf("tenant-b bindings count=%d err=%v", count, err)
	}
}

func candidateForTenant(candidates []feishu.Binding, tenantID string) (feishu.Binding, bool) {
	for _, candidate := range candidates {
		if candidate.TenantID == tenantID {
			return candidate, true
		}
	}
	return feishu.Binding{}, false
}

func hasTenantCandidate(candidates []feishu.Binding, tenantID string) bool {
	_, ok := candidateForTenant(candidates, tenantID)
	return ok
}
