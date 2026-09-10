package migrations

import (
	"context"
	"os"
	"testing"

	storage "github.com/XnLemon/trpc-agent-service/trpcservice/storage/mysql"
)

func TestMySQLControlPlaneMigrationLive(t *testing.T) {
	dsn := os.Getenv("MYSQL_CONTROL_PLANE_MIGRATION_TEST_DSN")
	if dsn == "" {
		t.Skip("MYSQL_CONTROL_PLANE_MIGRATION_TEST_DSN is not configured")
	}
	db, err := storage.Open(context.Background(), dsn, storage.Options{MaxOpenConns: 4, MaxIdleConns: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := ApplyMySQL(context.Background(), db); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := VerifyMySQL(context.Background(), db); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	var collation string
	if err := db.QueryRowContext(context.Background(), `SELECT TABLE_COLLATION FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'tenant'`).Scan(&collation); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if collation != "utf8mb4_bin" {
		_ = db.Close()
		t.Fatalf("tenant collation = %q, want utf8mb4_bin", collation)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopening the pool exercises the restart/re-discovery path. Verify is
	// intentionally read-only and must not need the migration lock.
	restarted, err := storage.Open(context.Background(), dsn, storage.Options{MaxOpenConns: 4, MaxIdleConns: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = restarted.Close() }()
	if err := VerifyMySQL(context.Background(), restarted); err != nil {
		t.Fatal(err)
	}
}
