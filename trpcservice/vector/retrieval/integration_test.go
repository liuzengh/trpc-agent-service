//go:build integration

package retrieval

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/internal/testinfra"
	pgstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
)

type retrievalFixture struct {
	pool *pgxpool.Pool
	tc   tenant.TenantContext
	cfg  Config
}

func retrievalFixtureSetup(t *testing.T) *retrievalFixture {
	t.Helper()
	pgURL := os.Getenv("TEST_DATABASE_URL")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if pgURL == "" {
		lab := testinfra.NewDockerLab(t)
		lab.Start(ctx)
		lab.WaitHealthy(ctx)
		pgURL = lab.PostgresURL(ctx)
	}
	base, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: pgURL, MaxConns: 4, MinConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	schema := "p106d_retrieval_" + strings.ToLower(fmt.Sprintf("%x", time.Now().UnixNano()))
	if _, err = base.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		base.Close()
		t.Fatal(err)
	}
	cfg := pgstore.PostgresConfig{URL: pgURL, SearchPath: schema, MaxConns: 4, MinConns: 1, AllowDestructiveDown: true}
	pool, err := pgstore.NewPool(ctx, cfg)
	if err != nil {
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		t.Fatal(err)
	}
	_, file, _, _ := runtime.Caller(0)
	sourceFS := os.DirFS(filepath.Join(filepath.Dir(file), "../../../migrations"))
	const migrationCount = 8
	migrator, err := pgstore.NewMigratorWithPool(pool, cfg, sourceFS)
	if err != nil {
		t.Fatal(err)
	}
	if err = migrator.Up(ctx); err != nil {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO tenant (tenant_id, name, status, config_version) VALUES ('tenant-a', 'retrieval a', 'active', 1), ('tenant-b', 'retrieval b', 'active', 1)`); err != nil {
		t.Fatal(err)
	}
	retrievalCfg, err := testConfig().WithDefaults()
	if err != nil {
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
	return &retrievalFixture{pool: pool, tc: testTenant("tenant-a"), cfg: retrievalCfg}
}

func (f *retrievalFixture) hydrator(t *testing.T) *PostgresHydrator {
	t.Helper()
	hydrator, err := NewPostgresHydrator(f.pool, HydratorConfig{})
	if err != nil {
		t.Fatal(err)
	}
	return hydrator
}

func (f *retrievalFixture) insertMemory(t *testing.T, tenantID, memoryID, scope, content string, version int64, deleted bool) {
	t.Helper()
	var sessionArg interface{}
	if scope == "session" {
		sessionArg = "session-" + memoryID
	}
	if _, err := f.pool.Exec(context.Background(), `INSERT INTO memory (tenant_id, memory_id, scope, scope_id, session_id, kind, content, version, source_seq, deleted)
        VALUES ($1,$2,$3,$4,$5,'fact',$6,$7,0,$8) ON CONFLICT (tenant_id, memory_id) DO UPDATE SET content=EXCLUDED.content, version=EXCLUDED.version, deleted=EXCLUDED.deleted, updated_at=now()`,
		tenantID, memoryID, scope, "scope-"+scope, sessionArg, content, version, deleted); err != nil {
		t.Fatal(err)
	}
}

func mustHit(t *testing.T, tc tenant.TenantContext, cfg Config, sourceID, content string, version int64, score float64) vector.SearchResult {
	t.Helper()
	source := vector.SourceDocument{SourceType: vector.SourceTypeMemory, SourceID: sourceID, ProjectionScope: "memory:" + "user", SourceVersion: version, SourceSequence: 0, Content: content, Model: cfg.Model, ModelVersion: cfg.ModelVersion, Dimension: cfg.Dimension, SchemaVersion: cfg.SchemaVersion}
	ref, err := vector.BuildDocumentRef(tenant.WithContext(context.Background(), tc), source)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := ref.SafeMetadata()
	if err != nil {
		t.Fatal(err)
	}
	return vector.SearchResult{Ref: ref, Score: score, Metadata: metadata}
}

func TestRetrievalHydratesAuthoritativeMemory(t *testing.T) {
	fixture := retrievalFixtureSetup(t)
	fixture.insertMemory(t, "tenant-a", "mem-1", "user", "active memory one", 1, false)
	fixture.insertMemory(t, "tenant-a", "mem-2", "user", "stale memory", 2, false)
	fixture.insertMemory(t, "tenant-a", "mem-3", "user", "deleted memory", 1, true)
	fixture.insertMemory(t, "tenant-b", "mem-4", "user", "tenant b memory", 1, false)

	hitActive := mustHit(t, fixture.tc, fixture.cfg, "mem-1", "active memory one", 1, 0.9)
	hitStale := mustHit(t, fixture.tc, fixture.cfg, "mem-2", "stale memory", 1, 0.8)
	hitDeleted := mustHit(t, fixture.tc, fixture.cfg, "mem-3", "deleted memory", 1, 0.7)
	hitMissing := mustHit(t, fixture.tc, fixture.cfg, "mem-missing", "no such memory", 1, 0.6)
	hitTenantB := mustHit(t, testTenant("tenant-b"), fixture.cfg, "mem-4", "tenant b memory", 1, 0.95)

	store := &fakeStore{hits: []vector.SearchResult{hitActive, hitStale, hitDeleted, hitMissing, hitTenantB}}
	embedder := &fakeEmbedder{embedding: queryEmbedding(fixture.cfg)}
	service := newTestService(t, fixture.cfg, store, embedder, fixture.hydrator(t))
	results, err := service.Retrieve(tenant.WithContext(context.Background(), fixture.tc), fixture.tc, Request{Query: "query one", TopK: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].MemoryID != "mem-1" || results[0].Content != "active memory one" || results[0].Score != 0.9 {
		t.Fatalf("unexpected hydrated results: %+v", results)
	}
}

func TestRetrievalHydrationTenantIsolationConcurrent(t *testing.T) {
	fixture := retrievalFixtureSetup(t)
	fixture.insertMemory(t, "tenant-a", "mem-a", "user", "mem-a content", 1, false)
	fixture.insertMemory(t, "tenant-b", "mem-b", "user", "mem-b content", 1, false)
	hydrator := fixture.hydrator(t)
	embedder := &fakeEmbedder{embedding: queryEmbedding(fixture.cfg)}

	run := func(tc tenant.TenantContext, want string) func(t *testing.T) {
		return func(t *testing.T) {
			hit := mustHit(t, tc, fixture.cfg, want, want+" content", 1, 0.9)
			_ = hit
			store := &fakeStore{hits: []vector.SearchResult{hit}}
			service := newTestService(t, fixture.cfg, store, embedder, hydrator)
			results, err := service.Retrieve(tenant.WithContext(context.Background(), tc), tc, Request{Query: "query", TopK: 3})
			if err != nil {
				t.Fatal(err)
			}
			if len(results) != 1 || results[0].MemoryID != want {
				t.Fatalf("unexpected results for %s: %+v", tc.TenantID, results)
			}
			if !strings.Contains(results[0].Content, tc.TenantID[len(tc.TenantID)-1:]) {
				t.Fatalf("cross-tenant content leaked: %+v", results[0])
			}
		}
	}
	t.Run("tenant-a", run(fixture.tc, "mem-a"))
	t.Run("tenant-b", run(testTenant("tenant-b"), "mem-b"))

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			hit := mustHit(t, fixture.tc, fixture.cfg, "mem-a", "tenant a content", 1, 0.9)
			store := &fakeStore{hits: []vector.SearchResult{hit}}
			service := newTestService(t, fixture.cfg, store, embedder, hydrator)
			if _, err := service.Retrieve(tenant.WithContext(context.Background(), fixture.tc), fixture.tc, Request{Query: "query", TopK: 1}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}

func TestRetrievalCrossTenantHitNeverHydrated(t *testing.T) {
	fixture := retrievalFixtureSetup(t)
	fixture.insertMemory(t, "tenant-b", "mem-b", "user", "tenant b content", 1, false)
	hit := mustHit(t, testTenant("tenant-b"), fixture.cfg, "mem-b", "tenant b content", 1, 0.99)
	store := &fakeStore{hits: []vector.SearchResult{hit}}
	embedder := &fakeEmbedder{embedding: queryEmbedding(fixture.cfg)}
	service := newTestService(t, fixture.cfg, store, embedder, fixture.hydrator(t))
	results, err := service.Retrieve(tenant.WithContext(context.Background(), fixture.tc), fixture.tc, Request{Query: "query", TopK: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("cross-tenant candidate hydrated: %+v", results)
	}
}

func TestRetrievalPostgresUnavailableAndRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	lab := testinfra.NewDockerLab(t)
	lab.Start(ctx)
	lab.WaitHealthy(ctx)
	pgURL := lab.PostgresURL(ctx)
	base, err := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: pgURL, MaxConns: 4, MinConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	schema := "p106d_restart_" + strings.ToLower(fmt.Sprintf("%x", time.Now().UnixNano()))
	if _, err = base.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		base.Close()
		t.Fatal(err)
	}
	cfg := pgstore.PostgresConfig{URL: pgURL, SearchPath: schema, MaxConns: 4, MinConns: 1, AllowDestructiveDown: true}
	pool, err := pgstore.NewPool(ctx, cfg)
	if err != nil {
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+schema+" CASCADE")
		base.Close()
		t.Fatal(err)
	}
	_, file, _, _ := runtime.Caller(0)
	sourceFS := os.DirFS(filepath.Join(filepath.Dir(file), "../../../migrations"))
	const migrationCount = 8
	migrator, err := pgstore.NewMigratorWithPool(pool, cfg, sourceFS)
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
	if _, err = pool.Exec(ctx, `INSERT INTO tenant (tenant_id, name, status, config_version) VALUES ('tenant-a', 'restart a', 'active', 1)`); err != nil {
		t.Fatal(err)
	}
	retrievalCfg, err := testConfig().WithDefaults()
	if err != nil {
		t.Fatal(err)
	}
	tc := testTenant("tenant-a")
	if _, err = pool.Exec(ctx, `INSERT INTO memory (tenant_id, memory_id, scope, scope_id, kind, content, version, source_seq, deleted) VALUES ('tenant-a', 'mem-1', 'user', 'scope-user', 'fact', 'active memory one', 1, 0, false)`); err != nil {
		t.Fatal(err)
	}
	hydrator, err := NewPostgresHydrator(pool, HydratorConfig{})
	if err != nil {
		t.Fatal(err)
	}
	hit := mustHit(t, tc, retrievalCfg, "mem-1", "active memory one", 1, 0.9)
	store := &fakeStore{hits: []vector.SearchResult{hit}}
	embedder := &fakeEmbedder{embedding: queryEmbedding(retrievalCfg)}
	service := newTestService(t, retrievalCfg, store, embedder, hydrator)

	results, err := service.Retrieve(tenant.WithContext(context.Background(), tc), tc, Request{Query: "query", TopK: 1})
	if err != nil || len(results) != 1 {
		t.Fatalf("baseline retrieval failed: %v %+v", err, results)
	}
	lab.StopPostgres(ctx)
	if _, err = service.Retrieve(tenant.WithContext(context.Background(), tc), tc, Request{Query: "query", TopK: 1}); err != nil && !errors.Is(err, ErrUnavailable) {
		t.Fatalf("postgres outage not classified: %v", err)
	}
	lab.RestartPostgres(ctx)
	t.Logf("diag: restarted, state=%s", func() string {
		out, err := exec.Command("docker", "inspect", "--format", "{{.State.Status}}", func() string { _, id := lab.ContainerIDs(); return id }()).Output()
		if err != nil {
			return "inspect_error"
		}
		return strings.TrimSpace(string(out))
	}())
	// Docker may reassign the dynamic host port after a container restart, so
	// the recovery loop re-resolves the endpoint each round and rebuilds the
	// pool and hydrator whenever it moves (documented P1-06B behavior).
	deadline := time.Now().Add(90 * time.Second)
	prefix, rest, found := strings.Cut(pgURL, "@127.0.0.1:")
	if !found {
		t.Fatal("unexpected postgres url shape")
	}
	originalPort := rest[:strings.Index(rest, "/")]
	currentPort := originalPort
	for {
		mappedPort, ok := ownedPostgresPort(t, lab)
		if ok && mappedPort != currentPort {
			rebuilt, poolErr := pgstore.NewPool(ctx, pgstore.PostgresConfig{URL: prefix + "@127.0.0.1:" + mappedPort + rest[strings.Index(rest, "/"):], SearchPath: schema, MaxConns: 4, MinConns: 1})
			if poolErr == nil {
				rebuiltHydrator, hydratorErr := NewPostgresHydrator(rebuilt, HydratorConfig{})
				if hydratorErr == nil {
					service = newTestService(t, retrievalCfg, store, embedder, rebuiltHydrator)
					currentPort = mappedPort
				}
			}
		}
		results, err = service.Retrieve(tenant.WithContext(context.Background(), tc), tc, Request{Query: "query", TopK: 1})
		if err == nil && len(results) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("postgres recovery retrieval failed: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	if results[0].MemoryID != "mem-1" {
		t.Fatalf("unexpected recovered results: %+v", results)
	}
}

// ownedPostgresPort resolves the current host port of the owned lab postgres.
// It returns false while Docker has no binding (container stopped or starting)
// instead of failing the test, so the recovery loop can retry bounded.
func ownedPostgresPort(t *testing.T, lab *testinfra.DockerLab) (string, bool) {
	t.Helper()
	_, postgresID := lab.ContainerIDs()
	if postgresID == "" {
		return "", false
	}
	command := exec.Command("docker", "port", postgresID, "5432/tcp")
	output, err := command.Output()
	if err != nil {
		return "", false
	}
	parts := strings.Split(strings.TrimSpace(string(output)), ":")
	if len(parts) == 0 || parts[len(parts)-1] == "" {
		return "", false
	}
	if _, err := strconv.Atoi(parts[len(parts)-1]); err != nil {
		return "", false
	}
	return parts[len(parts)-1], true
}

func TestRetrievalCancellationDuringHydration(t *testing.T) {
	fixture := retrievalFixtureSetup(t)
	fixture.insertMemory(t, "tenant-a", "mem-1", "user", "active memory one", 1, false)
	hit := mustHit(t, fixture.tc, fixture.cfg, "mem-1", "active memory one", 1, 0.9)
	store := &fakeStore{hits: []vector.SearchResult{hit}}
	embedder := &fakeEmbedder{embedding: queryEmbedding(fixture.cfg)}
	hydrator := fixture.hydrator(t)
	service := newTestService(t, fixture.cfg, store, embedder, hydrator)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.Retrieve(tenant.WithContext(ctx, fixture.tc), fixture.tc, Request{Query: "query", TopK: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation not preserved: %v", err)
	}
}
