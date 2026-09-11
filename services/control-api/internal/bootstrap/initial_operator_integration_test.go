package bootstrap

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	sharedpostgres "github.com/liuzengh/trpc-agent-service/services/control-api/internal/infra/postgres"
	"github.com/liuzengh/trpc-agent-service/services/control-api/migrations"
)

func TestInitialOperatorBootstrapAgainstPostgreSQL(t *testing.T) {
	databaseURL := os.Getenv("CONTROL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("CONTROL_TEST_DATABASE_URL is not set")
	}

	t.Run("is atomic and idempotent", func(t *testing.T) {
		pool := bootstrapTestPool(t, databaseURL)
		config := Config{
			BootstrapMode: "auto", BootstrapUsername: "platform-admin",
			BootstrapDisplayName: "Platform Admin", BootstrapPassword: "Initial-pass-1234",
		}
		if err := ensureInitialPlatformOperator(context.Background(), pool, config); err != nil {
			t.Fatalf("first bootstrap: %v", err)
		}
		config.BootstrapUsername = "must-not-replace-existing"
		config.BootstrapPassword = "Different-pass-5678"
		if err := ensureInitialPlatformOperator(context.Background(), pool, config); err != nil {
			t.Fatalf("idempotent bootstrap: %v", err)
		}
		assertTableCount(t, pool, "user_accounts", 1)
		assertTableCount(t, pool, "password_credentials", 1)
		assertTableCount(t, pool, "platform_operator_grants", 1)
		var username string
		if err := pool.QueryRow(context.Background(), "SELECT username FROM user_accounts").Scan(&username); err != nil {
			t.Fatal(err)
		}
		if username != "platform-admin" {
			t.Fatalf("username = %q", username)
		}
	})

	t.Run("rolls back account when grant fails", func(t *testing.T) {
		pool := bootstrapTestPool(t, databaseURL)
		_, err := pool.Exec(context.Background(), `
			CREATE FUNCTION reject_operator_grant() RETURNS trigger
			LANGUAGE plpgsql AS $$
			BEGIN
				RAISE EXCEPTION 'injected operator grant failure';
			END
			$$;
			CREATE TRIGGER reject_operator_grant
			BEFORE INSERT ON platform_operator_grants
			FOR EACH ROW EXECUTE FUNCTION reject_operator_grant();
		`)
		if err != nil {
			t.Fatalf("install failure trigger: %v", err)
		}
		config := Config{
			BootstrapMode: "auto", BootstrapUsername: "platform-admin",
			BootstrapPassword: "Initial-pass-1234",
		}
		if err := ensureInitialPlatformOperator(context.Background(), pool, config); err == nil {
			t.Fatal("bootstrap error = nil, want injected failure")
		}
		assertTableCount(t, pool, "user_accounts", 0)
		assertTableCount(t, pool, "password_credentials", 0)
		assertTableCount(t, pool, "platform_operator_grants", 0)
	})
}

func bootstrapTestPool(t *testing.T, databaseURL string) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	adminPool, err := sharedpostgres.Open(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(adminPool.Close)
	schema := fmt.Sprintf("bootstrap_v1_test_%d", time.Now().UnixNano())
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = adminPool.Exec(context.Background(), "DROP SCHEMA "+quotedSchema+" CASCADE")
	})
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := sharedpostgres.Migrate(ctx, pool, migrations.Files); err != nil {
		t.Fatal(err)
	}
	return pool
}

func assertTableCount(t *testing.T, pool *pgxpool.Pool, table string, want int) {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), "SELECT count(*)::int FROM "+table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("%s count = %d, want %d", table, count, want)
	}
}
