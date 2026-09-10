package bootstrap

import (
	"context"
	"database/sql"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/XnLemon/trpc-agent-service/migrations"
	storagepostgres "github.com/XnLemon/trpc-agent-service/trpcservice/storage/postgres"
)

type initAttempt struct {
	result InitResult
	err    error
}

// TestPostgreSQLInitializeConcurrent uses a separately provisioned disposable
// database to prove that the initialization advisory lock spans real
// transactions. It intentionally skips when the dedicated test DSN is absent.
func TestPostgreSQLInitializeConcurrent(t *testing.T) {
	ctx, db := openPostgresInitTestDB(t)

	const attempts = 16
	config := InitConfig{
		TenantKey: "concurrent-init", TenantDisplayName: "Concurrent Init",
		AppKey: "assistant", AppDisplayName: "Assistant",
	}
	start := make(chan struct{})
	results := make(chan initAttempt, attempts)
	var group sync.WaitGroup
	group.Add(attempts)
	for i := 0; i < attempts; i++ {
		go func() {
			defer group.Done()
			<-start
			result, initErr := Initialize(ctx, db, config)
			results <- initAttempt{result: result, err: initErr}
		}()
	}
	close(start)
	group.Wait()
	close(results)

	first := assertConcurrentInitResults(t, results)
	assertInitPairCounts(t, ctx, db)

	repeat, err := Initialize(ctx, db, InitConfig{
		TenantKey: "different", TenantDisplayName: "Ignored", AppKey: "different", AppDisplayName: "Ignored",
	})
	if err != nil || repeat.Created || repeat.TenantID != first.TenantID || repeat.AppID != first.AppID {
		t.Fatalf("repeat initialization = %+v, err=%v", repeat, err)
	}
}

// TestPostgreSQLInitializeDemo exercises the complete local graph against the
// same disposable PostgreSQL service used by the initialization tests. It is
// intentionally skipped outside CI when POSTGRES_INIT_TEST_DSN is absent.
func TestPostgreSQLInitializeDemo(t *testing.T) {
	ctx, db := openPostgresDemoTestDB(t)
	defer func() { _, _ = db.ExecContext(context.Background(), "TRUNCATE TABLE public.tenant CASCADE") }()
	if _, err := db.ExecContext(ctx, "TRUNCATE TABLE public.tenant CASCADE"); err != nil {
		t.Fatal(err)
	}

	first, err := InitializeDemo(ctx, db, DefaultDemoConfig())
	if err != nil {
		t.Fatalf("first demo bootstrap: %v", err)
	}
	if !first.Created || first.TenantID == "" || first.AppID == "" || first.ModelProfileID == "" || first.BackendProfileID == "" || first.Revision < 1 {
		t.Fatalf("first demo result = %+v", first)
	}
	second, err := InitializeDemo(ctx, db, DefaultDemoConfig())
	if err != nil {
		t.Fatalf("repeat demo bootstrap: %v", err)
	}
	if second.Created || second.TenantID != first.TenantID || second.AppID != first.AppID || second.ModelProfileID != first.ModelProfileID || second.BackendProfileID != first.BackendProfileID || second.Revision != first.Revision {
		t.Fatalf("repeat demo result = %+v, first = %+v", second, first)
	}
	assertDemoRowCount(t, ctx, db, "model_profile", first.TenantID, first.ModelProfileID)
	assertDemoRowCount(t, ctx, db, "backend_profile", first.TenantID, first.BackendProfileID)
	var revisions int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM public.agent_app_revision WHERE tenant_id = $1 AND app_id = $2", first.TenantID, first.AppID).Scan(&revisions); err != nil {
		t.Fatal(err)
	}
	if revisions != 1 {
		t.Fatalf("demo revision count = %d, want 1", revisions)
	}
}

func openPostgresDemoTestDB(t *testing.T) (context.Context, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("POSTGRES_INIT_TEST_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_INIT_TEST_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	db, err := storagepostgres.Open(ctx, dsn, storagepostgres.Options{MaxOpenConns: 16, MaxIdleConns: 16})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := migrations.Apply(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Verify(ctx, db); err != nil {
		t.Fatal(err)
	}
	return ctx, db
}

func assertDemoRowCount(t *testing.T, ctx context.Context, db *sql.DB, table, tenantID, profileID string) {
	t.Helper()
	var count int
	query := ""
	switch table {
	case "model_profile":
		query = "SELECT COUNT(*) FROM public.model_profile WHERE tenant_id = $1 AND profile_id = $2"
	case "backend_profile":
		query = "SELECT COUNT(*) FROM public.backend_profile WHERE tenant_id = $1 AND profile_id = $2"
	default:
		t.Fatalf("unsupported demo table %q", table)
	}
	if err := db.QueryRowContext(ctx, query, tenantID, profileID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("%s count = %d, want 1", table, count)
	}
}

func openPostgresInitTestDB(t *testing.T) (context.Context, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("POSTGRES_INIT_TEST_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_INIT_TEST_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	db, err := storagepostgres.Open(ctx, dsn, storagepostgres.Options{MaxOpenConns: 16, MaxIdleConns: 16})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := migrations.Apply(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Verify(ctx, db); err != nil {
		t.Fatal(err)
	}
	if !isEmptyInitDatabase(t, ctx, db) {
		t.Skip("POSTGRES_INIT_TEST_DSN must point to an empty disposable database")
	}
	return ctx, db
}

func isEmptyInitDatabase(t *testing.T, ctx context.Context, db *sql.DB) bool {
	t.Helper()
	var tenantCount, appCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM public.tenant").Scan(&tenantCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM public.agent_app").Scan(&appCount); err != nil {
		t.Fatal(err)
	}
	return tenantCount == 0 && appCount == 0
}

func assertConcurrentInitResults(t *testing.T, results <-chan initAttempt) InitResult {
	t.Helper()
	var first InitResult
	created := 0
	for value := range results {
		if value.err != nil {
			t.Fatalf("concurrent initialization error = %v", value.err)
		}
		if first.TenantID == "" {
			first = value.result
		}
		if value.result.TenantID != first.TenantID || value.result.AppID != first.AppID {
			t.Fatalf("concurrent initialization result = %+v, first = %+v", value.result, first)
		}
		if value.result.Created {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("concurrent initialization creators = %d, want 1", created)
	}
	return first
}

func assertInitPairCounts(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	var tenantTotal, appTotal int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM public.tenant").Scan(&tenantTotal); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM public.agent_app").Scan(&appTotal); err != nil {
		t.Fatal(err)
	}
	if tenantTotal != 1 || appTotal != 1 {
		t.Fatalf("concurrent initialization totals = tenant:%d app:%d", tenantTotal, appTotal)
	}
}
