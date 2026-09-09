package platform

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRedisIntegrationProfile(t *testing.T) {
	address := os.Getenv("TRPC_TEST_REDIS_ADDR")
	if address == "" {
		t.Skip("TRPC_TEST_REDIS_ADDR is not set")
	}
	store := NewRedisStore(address)
	defer store.Close()
	if health := store.Health(context.Background()); health.Status != "healthy" {
		t.Fatalf("health=%#v", health)
	}
}

func TestPostgreSQLCompatibilityProfile(t *testing.T) {
	dsn := os.Getenv("TRPC_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TRPC_TEST_POSTGRES_DSN is not set")
	}
	store, err := NewPostgresStore(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	runDataStoreContract(t, store, "postgres-contract-"+time.Now().UTC().Format("150405.000000000"))
	tenant := "profile-tenant"
	session := "profile-session"
	event := SessionEvent{TenantID: tenant, SessionID: session, IdempotencyKey: "profile-event", Type: "message", Payload: []byte("postgres")}
	if err := store.AppendSessionEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	events, err := store.ListSessionEvents(context.Background(), tenant, session, 0)
	if err != nil || len(events) == 0 {
		t.Fatalf("events=%#v err=%v", events, err)
	}
}

func TestPostgreSQLConcurrentStorageInitialization(t *testing.T) {
	dsn := os.Getenv("TRPC_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TRPC_TEST_POSTGRES_DSN is not set")
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := "storage_init_" + stableID(time.Now().UTC().String())
	if _, err := admin.ExecContext(context.Background(), `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.ExecContext(context.Background(), `DROP SCHEMA `+schema+` CASCADE`) })
	separator := "?"
	if strings.Contains(dsn, "?") {
		separator = "&"
	}
	scopedDSN := dsn + separator + "search_path=" + schema
	if !strings.Contains(dsn, "://") {
		scopedDSN = dsn + " search_path=" + schema
	}
	start := make(chan struct{})
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			store, err := NewPostgresStore(scopedDSN)
			if err == nil {
				err = store.Close()
			}
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}
