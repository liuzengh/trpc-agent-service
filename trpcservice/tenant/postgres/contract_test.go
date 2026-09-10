package postgres

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/liuzengh/trpc-agent-service/trpcservice/redaction"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestTenantStatusCASAndOutboxFactsPostgreSQL16(t *testing.T) {
	db := openContractDB(t)
	ctx := context.Background()
	const tenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAG"
	metadata := tenant.ChangeMetadata{ActorType: "test", ActorID: "tenant", ReasonCode: "contract", CorrelationID: "contract", TraceID: "contract"}
	repository := New(db)
	created, err := repository.Create(ctx, tenant.CreateInput{Tenant: tenant.Tenant{TenantID: tenantID, TenantKey: "tenant-contract", DisplayName: "Tenant Contract"}, ChangeMetadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for _, next := range []tenant.Status{tenant.StatusSuspended, tenant.StatusDisabled} {
		next := next
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, transitionErr := repository.TransitionStatus(ctx, tenant.TransitionStatusInput{TenantID: tenantID, ExpectedVersion: created.Version, NextStatus: next, ChangeMetadata: metadata})
			results <- transitionErr
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	successes, conflicts := 0, 0
	for transitionErr := range results {
		switch {
		case transitionErr == nil:
			successes++
		case errors.Is(transitionErr, tenant.ErrVersionConflict):
			conflicts++
		default:
			t.Fatalf("transition=%v", transitionErr)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
	current, err := repository.Get(ctx, tenantID)
	if err != nil || current.Version != created.Version+1 || current.Status == tenant.StatusActive {
		t.Fatalf("current=%#v err=%v", current, err)
	}
	var auditCount, controlCount int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FILTER (WHERE kind='audit'),count(*) FILTER (WHERE kind='tenant-control') FROM outbox WHERE tenant_id=$1`, tenantID).Scan(&auditCount, &controlCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 2 || controlCount != 2 {
		t.Fatalf("audit=%d control=%d", auditCount, controlCount)
	}
}

func TestTenantConfigurationPublishesRedactionRulesWithCASPostgreSQL16(t *testing.T) {
	db := openContractDB(t)
	ctx := context.Background()
	const tenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAZ"
	metadata := tenant.ChangeMetadata{ActorType: "test", ActorID: "tenant", ReasonCode: "contract", CorrelationID: "redaction-contract", TraceID: "redaction-contract"}
	repository := New(db)
	created, err := repository.Create(ctx, tenant.CreateInput{Tenant: tenant.Tenant{TenantID: tenantID, TenantKey: "tenant-redaction-policy", DisplayName: "Tenant Redaction"}, ChangeMetadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	next := created
	next.RedactionRules = []redaction.Rule{{ID: "case-reference", KeyFragments: []string{"case_ref"}, TextPattern: `CASE-[0-9]{6}`}}
	updated, err := repository.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{Tenant: next, ExpectedVersion: created.Version, ChangeMetadata: metadata})
	if err != nil {
		t.Fatal(err)
	}
	if updated.Tenant.Version != created.Version+1 || len(updated.Tenant.RedactionRules) != 1 || updated.Tenant.RedactionRules[0].ID != "case-reference" {
		t.Fatalf("updated=%#v", updated.Tenant)
	}
	if _, err := repository.UpdateConfiguration(ctx, tenant.UpdateConfigurationInput{Tenant: next, ExpectedVersion: created.Version, ChangeMetadata: metadata}); !errors.Is(err, tenant.ErrVersionConflict) {
		t.Fatalf("expected version conflict, got %v", err)
	}
}

func openContractDB(t *testing.T) *sql.DB {
	t.Helper()
	if os.Getenv("TRPC_MIGRATION_TEST") != "1" {
		t.Skip("requires explicit disposable PostgreSQL migration test")
	}
	dsn := os.Getenv("TRPC_POSTGRES_TEST_DSN")
	if dsn == "" {
		t.Skip("TRPC_POSTGRES_TEST_DSN is not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	var databaseName string
	var serverMajor int
	if err := db.QueryRowContext(context.Background(), `SELECT current_database(),current_setting('server_version_num')::int/10000`).Scan(&databaseName, &serverMajor); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(databaseName, "trpc_agent_service_test_") || serverMajor != 16 {
		t.Fatalf("refusing database=%q PostgreSQL=%d", databaseName, serverMajor)
	}
	return db
}
