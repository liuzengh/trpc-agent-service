package configpub

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	pgstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type configPubPGFixture struct {
	ctx    context.Context
	pool   *pgxpool.Pool
	repo   *PostgresRepository
	co     *Coordinator
	tenant tenant.TenantContext
}

func newConfigPubPGFixture(t *testing.T) *configPubPGFixture {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set; P1-08 PostgreSQL integration is unavailable")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	base, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: url, MaxConns: 8, MinConns: 1})
	if err != nil {
		cancel()
		t.Fatalf("PostgreSQL fixture unavailable")
	}
	schema := fmt.Sprintf("p108_cfg_%d", time.Now().UnixNano())
	if _, err := base.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		base.Close()
		cancel()
		t.Fatalf("PostgreSQL fixture schema unavailable")
	}
	cfg := pgstore.PostgresConfig{URL: url, SearchPath: schema, MaxConns: 8, MinConns: 1, AllowDestructiveDown: true}
	pool, err := pgstore.NewPool(ctx, cfg)
	if err != nil {
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
		t.Fatalf("PostgreSQL fixture pool unavailable")
	}
	_, file, _, _ := runtime.Caller(0)
	migrator, err := pgstore.NewMigratorWithPool(pool, cfg, os.DirFS(filepath.Join(filepath.Dir(file), "../../migrations")))
	if err != nil {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
		t.Fatalf("P1-08 migration fixture unavailable")
	}
	if err := migrator.Up(ctx); err != nil {
		migrator.Close()
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
		t.Fatalf("P1-08 migration fixture unavailable")
	}
	migrator.Close()
	if _, err := pool.Exec(ctx, `INSERT INTO tenant (tenant_id, name, config_version, backend_config)
		VALUES ('p108-tenant-a', 'P1-08 test tenant', 1, '{"session":"memory","memory":"memory","vector":"none","object":"none"}')`); err != nil {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
		t.Fatalf("P1-08 tenant fixture unavailable")
	}
	if _, err := pool.Exec(ctx, `INSERT INTO channel_binding (tenant_id, channel, binding_id, external_app_id, secret_ref)
		VALUES ('p108-tenant-a', 'lark', 'binding-1', 'app-1', 'env://P1_08_TEST_SECRET')`); err != nil {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
		t.Fatalf("P1-08 binding fixture unavailable")
	}
	repo, err := NewPostgresRepository(pool)
	if err != nil {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
		t.Fatalf("P1-08 repository unavailable")
	}
	co, err := NewCoordinator(repo, repo, nil)
	if err != nil {
		t.Fatal("P1-08 coordinator unavailable")
	}
	tc := testTenantContext("p108-tenant-a", 1)
	t.Cleanup(func() {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		cancel()
	})
	return &configPubPGFixture{ctx: ctx, pool: pool, repo: repo, co: co, tenant: tc}
}

func TestPostgresConfigPublicationLifecycleAndIsolation(t *testing.T) {
	f := newConfigPubPGFixture(t)
	doc1 := validDocument()
	doc2 := doc1
	doc2.Agent.ToolPolicyRef = "policy-v2"
	if _, err := f.co.CreateRevision(f.ctx, f.tenant, CreateRevisionInput{Version: 1, Document: doc1, ActorCategory: "operator"}); err != nil {
		t.Fatalf("create v1: %v", err)
	}
	if _, err := f.co.CreateRevision(f.ctx, f.tenant, CreateRevisionInput{Version: 1, Document: doc1, ActorCategory: "operator"}); err != nil {
		t.Fatalf("idempotent create v1: %v", err)
	}
	if _, err := f.co.ValidateRevision(f.ctx, f.tenant, 1, "operator"); err != nil {
		t.Fatalf("validate v1: %v", err)
	}
	if _, err := f.co.Publish(f.ctx, f.tenant, PublishInput{Version: 1, ExpectedActiveVersion: 0, Percentage: 100, OperationID: "p108-publish-v1", ActorCategory: "operator"}); err != nil {
		t.Fatalf("publish v1: %v", err)
	}
	if _, err := f.co.CreateRevision(f.ctx, f.tenant, CreateRevisionInput{Version: 2, Document: doc2, ActorCategory: "operator"}); err != nil {
		t.Fatalf("create v2: %v", err)
	}
	if _, err := f.co.CreateRevision(f.ctx, f.tenant, CreateRevisionInput{Version: 3, Document: doc2, ActorCategory: "operator"}); !errors.Is(err, ErrDuplicateContent) {
		t.Fatalf("duplicate content error=%v", err)
	}
	if _, err := f.co.ValidateRevision(f.ctx, f.tenant, 2, "operator"); err != nil {
		t.Fatalf("validate v2: %v", err)
	}
	first, err := f.co.Publish(f.ctx, f.tenant, PublishInput{Version: 2, ExpectedActiveVersion: 1, Percentage: 25, OperationID: "p108-publish-v2", ActorCategory: "operator"})
	if err != nil {
		t.Fatalf("canary publish v2: %v", err)
	}
	if first.ResultActiveVersion != 2 {
		t.Fatalf("active version=%d, want 2", first.ResultActiveVersion)
	}
	state, managed, err := f.co.RolloutState(f.ctx, f.tenant.TenantID)
	if err != nil || !managed || state.ActiveVersion != 2 || state.BaselineVersion != 1 || state.Percentage != 25 {
		t.Fatalf("rollout=%+v managed=%v err=%v", state, managed, err)
	}
	if _, err := f.co.Publish(f.ctx, f.tenant, PublishInput{Version: 2, ExpectedActiveVersion: 1, Percentage: 25, OperationID: "p108-publish-v2", ActorCategory: "operator"}); err != nil {
		t.Fatalf("idempotent publish replay: %v", err)
	}
	if _, err := f.co.Publish(f.ctx, f.tenant, PublishInput{Version: 2, ExpectedActiveVersion: 1, Percentage: 50, OperationID: "p108-publish-v2", ActorCategory: "operator"}); !errors.Is(err, ErrOperationMismatch) {
		t.Fatalf("changed operation payload error=%v", err)
	}
	if _, err := f.co.Rollback(f.ctx, f.tenant, RollbackInput{TargetVersion: 1, ExpectedActiveVersion: 2, OperationID: "p108-rollback-v2", ActorCategory: "operator"}); err != nil {
		t.Fatalf("rollback v2: %v", err)
	}
	state, _, _ = f.co.RolloutState(f.ctx, f.tenant.TenantID)
	if state.ActiveVersion != 1 || state.BaselineVersion != 1 || state.Percentage != 100 {
		t.Fatalf("rollback rollout=%+v", state)
	}
	var published int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM tenant_config_version WHERE tenant_id=$1 AND status='published'`, f.tenant.TenantID).Scan(&published); err != nil || published != 1 {
		t.Fatalf("published count=%d err=%v", published, err)
	}
	if _, err := f.co.Snapshot(f.ctx, f.tenant.TenantID, 2); err != nil {
		// Recalled/superseded snapshots remain readable for in-flight jobs.
		t.Fatalf("historical snapshot read: %v", err)
	}
	if _, err := f.co.Snapshot(f.ctx, "p108-tenant-b", 1); !errors.Is(err, ErrRevisionNotFound) {
		t.Fatalf("cross-tenant snapshot error=%v", err)
	}
}

func TestPostgresConfigPublicationConcurrentCASAndImmutableRevision(t *testing.T) {
	f := newConfigPubPGFixture(t)
	docs := make([]Document, 4)
	for i := range docs {
		docs[i] = validDocument()
		docs[i].Agent.ToolPolicyRef = fmt.Sprintf("policy-%d", i)
		version := int64(i + 1)
		if _, err := f.co.CreateRevision(f.ctx, f.tenant, CreateRevisionInput{Version: version, Document: docs[i], ActorCategory: "operator"}); err != nil {
			t.Fatalf("create v%d: %v", version, err)
		}
		if _, err := f.co.ValidateRevision(f.ctx, f.tenant, version, "operator"); err != nil {
			t.Fatalf("validate v%d: %v", version, err)
		}
	}
	if _, err := f.co.Publish(f.ctx, f.tenant, PublishInput{Version: 1, ExpectedActiveVersion: 0, Percentage: 100, OperationID: "p108-cas-seed", ActorCategory: "operator"}); err != nil {
		t.Fatalf("seed publish: %v", err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, version := range []int64{2, 3} {
		version := version
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := f.co.Publish(f.ctx, f.tenant, PublishInput{Version: version, ExpectedActiveVersion: 1, Percentage: 100, OperationID: fmt.Sprintf("p108-cas-v%d", version), ActorCategory: "operator"})
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	successes, stale := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrStaleExpectedVersion):
			stale++
		default:
			t.Fatalf("concurrent CAS error=%v", err)
		}
	}
	if successes != 1 || stale != 1 {
		t.Fatalf("concurrent CAS successes=%d stale=%d", successes, stale)
	}
	var activeCount int
	if err := f.pool.QueryRow(f.ctx, `SELECT count(*) FROM tenant_config_version WHERE tenant_id=$1 AND status='published'`, f.tenant.TenantID).Scan(&activeCount); err != nil || activeCount != 1 {
		t.Fatalf("active count=%d err=%v", activeCount, err)
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE tenant_config_version SET config='{}' WHERE tenant_id=$1 AND config_version=1`, f.tenant.TenantID); err == nil {
		t.Fatalf("immutable config update unexpectedly succeeded")
	}
	if _, err := f.pool.Exec(f.ctx, `UPDATE tenant_config_version SET status='draft' WHERE tenant_id=$1 AND config_version=1`, f.tenant.TenantID); err == nil {
		t.Fatalf("invalid revision transition unexpectedly succeeded")
	}
}

func TestPostgresConfigPublicationDownRefusesExtendedHistory(t *testing.T) {
	f := newConfigPubPGFixture(t)
	doc := validDocument()
	if _, err := f.co.CreateRevision(f.ctx, f.tenant, CreateRevisionInput{Version: 1, Document: doc, ActorCategory: "operator"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := f.co.ValidateRevision(f.ctx, f.tenant, 1, "operator"); err != nil {
		t.Fatalf("validate: %v", err)
	}
	down, err := os.ReadFile(filepath.Join(repoRootForConfigPubTest(t), "migrations", "000009_p1_08_config_publication.down.sql"))
	if err != nil {
		t.Fatal("down migration source unavailable")
	}
	if _, err := f.pool.Exec(f.ctx, string(down)); err == nil {
		t.Fatalf("down migration unexpectedly destroyed extended history")
	}
	var exists bool
	if err := f.pool.QueryRow(f.ctx, `SELECT to_regclass('tenant_config_rollout') IS NOT NULL`).Scan(&exists); err != nil || !exists {
		t.Fatalf("down refusal did not preserve P1-08 schema: exists=%v err=%v", exists, err)
	}
}

func repoRootForConfigPubTest(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate configpub test")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
}
