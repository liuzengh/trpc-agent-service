package memoryvisibility

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cyl6/trpc-agent-service/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func newPostgresVisibilityStores(t *testing.T) (*Postgres, *Postgres) {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if strings.TrimSpace(dsn) == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "memory_visibility_it_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	newPool := func() *pgxpool.Pool {
		parsed, parseErr := pgxpool.ParseConfig(dsn)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		if parsed.ConnConfig.RuntimeParams == nil {
			parsed.ConnConfig.RuntimeParams = map[string]string{}
		}
		parsed.ConnConfig.RuntimeParams["search_path"] = schema
		pool, poolErr := pgxpool.NewWithConfig(ctx, parsed)
		if poolErr != nil {
			t.Fatal(poolErr)
		}
		return pool
	}
	poolA, poolB := newPool(), newPool()
	t.Cleanup(func() {
		poolA.Close()
		poolB.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(cleanupCtx, "DROP SCHEMA "+quoted+" CASCADE")
		admin.Close()
	})
	tx, err := poolA.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := migrations.ApplyAll(ctx, tx); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	a, err := NewPostgres(poolA, "node-a")
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewPostgres(poolB, "node-b")
	if err != nil {
		t.Fatal(err)
	}
	return a, b
}

func TestPostgresVisibilityWatermarkCrossNodeReadAfterWrite(t *testing.T) {
	a, b := newPostgresVisibilityStores(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	scope := Scope{TenantID: "tenant-a", AppName: "app-a", PrincipalID: "principal-a"}
	written, err := a.WriteWatermark(ctx, scope)
	if err != nil || written.Value != 1 || !written.CrossNode {
		t.Fatalf("WriteWatermark() = %#v, %v", written, err)
	}
	read, err := b.WaitUntilVisible(ctx, scope, written.Value)
	if err != nil || !read.Visible || read.Watermark.Value != written.Value || !read.Watermark.CrossNode {
		t.Fatalf("cross-node watermark read = %#v, %v", read, err)
	}
	if _, err := b.ReadAtLeast(ctx, Scope{TenantID: "tenant-b", AppName: "app-a", PrincipalID: "principal-a"}, 1); err != nil {
		t.Fatal(err)
	}
}
