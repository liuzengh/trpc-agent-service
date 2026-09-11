package datamigration

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cyl6/trpc-agent-service/migrations"
)

func TestPostgresIntegrationMigrationGenerationCAS(t *testing.T) {
	dsn, ok := os.LookupEnv("TEST_POSTGRES_DSN")
	if !ok || strings.TrimSpace(dsn) == "" {
		t.Skip("TEST_POSTGRES_DSN is not configured")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	tx, err := pool.Begin(ctx)
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

	store, err := NewPostgresStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	id := "test-" + uuid.NewString()
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM data_migrations WHERE migration_id = $1`, id)
	})
	job := testJob()
	job.MigrationID = id
	if err := store.Create(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := store.PutObject(ctx, ObjectRecord{
		MigrationID: id, ObjectKey: "session/user/1", SourceVer: "42",
		SourceHash: strings.Repeat("a", 64), TargetHash: strings.Repeat("a", 64),
		Status: ObjectVerified,
	}); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Get(ctx, id)
	if err != nil || loaded.Generation != 0 || loaded.Phase != PhasePrepare {
		t.Fatalf("loaded job = %+v, err=%v", loaded, err)
	}
	loaded.Status = StatusRunning
	loaded.Checkpoint = map[string]any{"cursor": "safe-opaque-cursor"}
	if err := store.CompareAndSwap(ctx, 0, loaded); err != nil {
		t.Fatal(err)
	}
	if err := store.CompareAndSwap(ctx, 0, loaded); err != ErrConflict {
		t.Fatalf("stale generation error = %v", err)
	}
}
