//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	platformadmin "github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestPostgresAdminReadViewsAreScopedAndSecretSafe(t *testing.T) {
	pool := openIntegrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tenantID := fmt.Sprintf("admin-read-%d", time.Now().UnixNano())
	appID := "support"
	config := integrationAppConfig("v1", "admin-read-model")
	config.TenantID, config.AppID = tenantID, appID
	config.BackendConfig.Session.Options["endpoint"] = "postgres.internal:5432"
	config.BackendConfig.Session.Options["password"] = "raw-secret"
	if err := store.CreateTenant(ctx, tenant.Tenant{ID: tenantID, Name: tenantID, Status: tenant.StatusActive}); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	if err := store.CreateAgentApp(ctx, tenant.AgentApp{
		TenantID: tenantID, AppID: appID, Name: "Support",
		ActiveConfigVersion: config.Version, Status: tenant.StatusActive,
	}, config); err != nil {
		t.Fatalf("create app: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM platform.app_config_version WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM platform.agent_app WHERE tenant_id = $1 AND app_id = $2`, tenantID, appID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM platform.tenant WHERE tenant_id = $1`, tenantID)
	})

	api := platformadmin.API{Repository: store}
	principal := platformadmin.AdminPrincipal{Role: platformadmin.RoleSystemAdmin, ActorID: "test-admin"}
	tenants, err := api.ListTenantsForPrincipal(ctx, principal, platformadmin.ListOptions{TenantID: tenantID})
	if err != nil || len(tenants) != 1 || tenants[0].ID != tenantID {
		t.Fatalf("tenant view = %#v, err=%v", tenants, err)
	}
	apps, err := api.ListAgentAppsForPrincipal(ctx, principal, platformadmin.ListOptions{TenantID: tenantID})
	if err != nil || len(apps) != 1 || apps[0].AppID != appID {
		t.Fatalf("app view = %#v, err=%v", apps, err)
	}
	if len(apps[0].Backends) != 1 || apps[0].Backends[0].SecretRef.Name != "" {
		t.Fatalf("app backend summary = %#v", apps[0].Backends)
	}
	configs, err := api.ListAppConfigsForPrincipal(ctx, principal, platformadmin.ListOptions{TenantID: tenantID, AppID: appID})
	if err != nil || len(configs) != 1 || !configs[0].Active || configs[0].Config.Version != config.Version {
		t.Fatalf("config view = %#v, err=%v", configs, err)
	}
	encoded, err := json.Marshal(configs)
	if err != nil {
		t.Fatalf("marshal config view: %v", err)
	}
	if strings.Contains(string(encoded), "raw-secret") {
		t.Fatalf("admin config view leaked a raw secret: %s", encoded)
	}
	if string(encoded) == "" || strings.Contains(string(encoded), "password") {
		t.Fatalf("admin config view retained a sensitive option: %s", encoded)
	}
	executions, err := api.ListExecutionsForPrincipal(ctx, principal, platformadmin.ListOptions{TenantID: tenantID, AppID: appID})
	if err != nil {
		t.Fatalf("list scoped executions: %v", err)
	}
	if executions == nil {
		t.Fatalf("scoped executions should be a non-nil list")
	}
	migrations, err := api.ListDataMigrationsForPrincipal(ctx, principal, platformadmin.ListOptions{TenantID: tenantID, AppID: appID})
	if err != nil {
		t.Fatalf("list scoped migrations: %v", err)
	}
	if migrations == nil {
		t.Fatalf("scoped migrations should be a non-nil list")
	}
	operations, err := store.OperationsSummaryForTenants(ctx, []string{tenantID})
	if err != nil {
		t.Fatalf("read scoped operations: %v", err)
	}
	if operations.ChannelReadiness != "NOT_READY" || operations.RecentErrors == nil {
		t.Fatalf("scoped operations = %#v", operations)
	}
}
