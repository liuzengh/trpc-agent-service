//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	platformpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestAuditRetentionIsTenantScopedAndBatched(t *testing.T) {
	pool := openIntegrationPool(t)
	defer pool.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	store, err := platformpostgres.New(pool)
	if err != nil {
		t.Fatalf("new postgres store: %v", err)
	}
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	retainedTenant := fmt.Sprintf("audit-retention-%d", time.Now().UnixNano())
	foreignTenant := retainedTenant + "-foreign"
	for _, value := range []tenant.Tenant{
		{ID: retainedTenant, Name: retainedTenant, Status: tenant.StatusActive, Audit: tenant.AuditPolicy{Enabled: true, RecordExecutions: true, RetentionDays: 1}},
		{ID: foreignTenant, Name: foreignTenant, Status: tenant.StatusActive, Audit: tenant.AuditPolicy{Enabled: true, RecordExecutions: true}},
	} {
		if err := store.CreateTenant(ctx, value); err != nil {
			t.Fatalf("create tenant %s: %v", value.ID, err)
		}
	}
	config := integrationAppConfig("v1", "audit-retention-model")
	config.Audit = tenant.AuditPolicy{Enabled: true, RecordExecutions: true, RetentionDays: 1}
	for _, item := range []struct {
		tenantID      string
		retentionDays int
	}{
		{tenantID: retainedTenant, retentionDays: 1},
		{tenantID: foreignTenant, retentionDays: 0},
	} {
		config.TenantID = item.tenantID
		config.AppID = "audit"
		config.Audit.RetentionDays = item.retentionDays
		if err := store.CreateAgentApp(ctx, tenant.AgentApp{
			TenantID: item.tenantID, AppID: "audit", Name: "Audit", ActiveConfigVersion: config.Version, Status: tenant.StatusActive,
		}, config); err != nil {
			t.Fatalf("create app %s: %v", item.tenantID, err)
		}
	}
	old := time.Now().UTC().Add(-48 * time.Hour)
	current := time.Now().UTC()
	for _, event := range []platformaudit.Event{
		{TenantID: retainedTenant, AppID: "audit", Decision: "completed", EventType: platformaudit.ExecutionCompleted, TraceID: "old", RequestID: "old", ConfigVersion: "v1", CreatedAt: old},
		{TenantID: retainedTenant, AppID: "audit", Decision: "completed", EventType: platformaudit.ExecutionCompleted, TraceID: "new", RequestID: "new", ConfigVersion: "v1", CreatedAt: current},
		{TenantID: foreignTenant, AppID: "audit", Decision: "completed", EventType: platformaudit.ExecutionCompleted, TraceID: "foreign", RequestID: "foreign", ConfigVersion: "v1", CreatedAt: old},
	} {
		if err := store.Record(ctx, event); err != nil {
			t.Fatalf("record audit event: %v", err)
		}
	}
	deleted, err := store.PurgeExpiredAuditEvents(ctx, 1)
	if err != nil {
		t.Fatalf("purge audit events: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	var retained, foreign int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM platform.audit_event WHERE tenant_id = $1`, retainedTenant).Scan(&retained); err != nil {
		t.Fatalf("count retained tenant events: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM platform.audit_event WHERE tenant_id = $1`, foreignTenant).Scan(&foreign); err != nil {
		t.Fatalf("count foreign tenant events: %v", err)
	}
	if retained != 1 || foreign != 1 {
		t.Fatalf("retained=%d foreign=%d, want 1/1", retained, foreign)
	}
}
