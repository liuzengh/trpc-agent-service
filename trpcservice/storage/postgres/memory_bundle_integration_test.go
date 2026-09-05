//go:build integration

package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type pgxBundle struct {
	pool     *pgxpool.Pool
	base     *pgxpool.Pool
	schema   string
	migrator Migrator
}

func openMigratedBundle(t *testing.T, pgURL string) *pgxBundle {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	base, err := NewPool(ctx, PostgresConfig{URL: pgURL, MaxConns: 4, MinConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	schema := "p106dp_memory_" + strings.ToLower(fmt.Sprintf("%x", time.Now().UnixNano()))
	if _, err = base.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		base.Close()
		t.Fatal(err)
	}
	cfg := PostgresConfig{URL: pgURL, SearchPath: schema, MaxConns: 4, MinConns: 1, AllowDestructiveDown: true}
	pool, err := NewPool(ctx, cfg)
	if err != nil {
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		t.Fatal(err)
	}
	_, file, _, _ := runtime.Caller(0)
	sourceFS := os.DirFS(filepath.Join(filepath.Dir(file), "../../../migrations"))
	const migrationCount = 8
	migrator, err := NewMigratorWithPool(pool, cfg, sourceFS)
	if err != nil {
		t.Fatal(err)
	}
	if err = migrator.Up(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		if err := migrator.Down(cleanupCtx, migrationCount); err != nil {
			t.Logf("migration cleanup failed: %v", err)
		}
		pool.Close()
		_, _ = base.Exec(cleanupCtx, "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
	})
	return &pgxBundle{pool: pool, base: base, schema: schema, migrator: migrator}
}
