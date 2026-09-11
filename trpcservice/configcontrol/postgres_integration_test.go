package configcontrol

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cyl6/trpc-agent-service/migrations"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
)

func newPostgresControlStore(t *testing.T) (*PostgresStore, *PostgresStore, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := "config_control_it_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	parsed, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	parsed.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, parsed)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(cleanupCtx, "DROP SCHEMA "+quoted+" CASCADE")
		admin.Close()
	})
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
	return NewPostgresWithTTL(pool, time.Minute), NewPostgresWithTTL(pool, time.Minute), pool
}

func TestPostgresControlPlaneCASAndVerifiedRelease(t *testing.T) {
	storeA, storeB, _ := newPostgresControlStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	v1 := controlTenant("tenant-pg", "v1", "one")
	if err := storeA.Bootstrap(ctx, []config.TenantConfig{v1}, "test", "bootstrap"); err != nil {
		t.Fatal(err)
	}
	for _, node := range []NodeIdentity{{NodeID: "node-a", BootID: "boot-a"}, {NodeID: "node-b", BootID: "boot-b"}} {
		if err := storeA.Heartbeat(ctx, NodeHeartbeat{NodeID: node.NodeID, BootID: node.BootID, Ready: true}); err != nil {
			t.Fatal(err)
		}
	}
	v2 := v1
	v2.Version = "v2"
	v2.App.Instruction = "two"
	if _, err := storeB.PutRevision(ctx, RevisionInput{TenantID: v2.TenantID, Revision: v2.Version, Tenant: v2}); err != nil {
		t.Fatal(err)
	}
	v3 := v1
	v3.Version = "v3"
	v3.App.Instruction = "three"
	if _, err := storeB.PutRevision(ctx, RevisionInput{TenantID: v3.TenantID, Revision: v3.Version, Tenant: v3}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := 0
	for _, revision := range []string{"v2", "v3"} {
		wg.Add(1)
		go func(revision string) {
			defer wg.Done()
			_, err := storeA.CreateRelease(ctx, CreateReleaseRequest{TenantID: "tenant-pg", Kind: ReleaseFull, TargetRevision: revision, ExpectedGeneration: 1})
			if err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			} else if !errors.Is(err, ErrReleaseConflict) && !errors.Is(err, ErrGenerationConflict) {
				t.Errorf("unexpected concurrent release error: %v", err)
			}
		}(revision)
	}
	wg.Wait()
	if successes != 1 {
		t.Fatalf("concurrent PostgreSQL releases succeeded = %d, want 1", successes)
	}
	pending, err := storeB.ListPendingReleases(ctx)
	if err != nil || len(pending) != 1 {
		t.Fatalf("pending releases = %d, %v", len(pending), err)
	}
	release := pending[0]
	for _, node := range []NodeIdentity{{NodeID: "node-a", BootID: "boot-a"}, {NodeID: "node-b", BootID: "boot-b"}} {
		if err := storeB.AckPrepared(ctx, release.ReleaseID, node, release.TargetRevision, 1, nil); err != nil {
			t.Fatal(err)
		}
	}
	activated, err := storeA.ActivateIfReady(ctx, release.ReleaseID)
	if err != nil || activated.Status != ReleaseActivePending {
		t.Fatalf("activated release = %+v, %v", activated, err)
	}
	for _, node := range []NodeIdentity{{NodeID: "node-a", BootID: "boot-a"}, {NodeID: "node-b", BootID: "boot-b"}} {
		if err := storeA.AckApplied(ctx, release.ReleaseID, node, release.TargetRevision, 2, nil); err != nil {
			t.Fatal(err)
		}
	}
	verified, err := storeB.GetRelease(ctx, release.ReleaseID)
	if err != nil || verified.Status != ReleaseVerified {
		t.Fatalf("verified release = %+v, %v", verified, err)
	}
	historical, err := storeB.GetRevision(ctx, "tenant-pg", "v1")
	if err != nil || historical.Tenant.App.Instruction != "one" {
		t.Fatalf("historical revision after restart = %+v, %v", historical, err)
	}
}
