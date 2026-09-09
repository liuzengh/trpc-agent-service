//go:build integration

package rebuild

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector/milvus"
	vectortask "github.com/liuzengh/trpc-agent-service/trpcservice/vector/task"
)

type e2eFixture struct {
	f           *rebuildFixture
	baseStore   vector.VectorStore
	identity    vector.IdentityReader
	targetStore vector.IdentityReader
	counting    *countingStore
	tasks       *e2eTasks
	router      *Router
	worker      *vectortask.Worker
	coordinator *Coordinator
	runs        *PostgresRunRepository
	taskRepo    *vectortask.PostgresRepository
	pool        *pgxpool.Pool
	tc          tenant.TenantContext
}

type e2eTasks struct {
	inner    rebuildTasks
	mu       sync.Mutex
	redrives int
}

func (a *e2eTasks) EnqueueTx(ctx context.Context, tx pgx.Tx, tc tenant.TenantContext, ref vector.VectorDocumentRef, now time.Time) (bool, error) {
	return a.inner.EnqueueTx(ctx, tx, tc, ref, now)
}

func (a *e2eTasks) RedriveTx(ctx context.Context, tx pgx.Tx, tc tenant.TenantContext, ref vector.VectorDocumentRef, now time.Time) (bool, error) {
	a.mu.Lock()
	a.redrives++
	a.mu.Unlock()
	return a.inner.RedriveTx(ctx, tx, tc, ref, now)
}

type e2eLeaseStore struct{ inner rebuildLeases }

func (e2eLeaseStore) Acquire(context.Context, tenant.TenantContext, string, string, time.Duration) (storage.Lease, error) {
	return storage.Lease{OwnerID: "e2e-owner", FenceToken: 1, Epoch: 1, ExpiresAt: time.Now().Add(time.Minute)}, nil
}
func (l e2eLeaseStore) Renew(_ context.Context, _ tenant.TenantContext, lease storage.Lease, ttl time.Duration) (storage.Lease, error) {
	lease.ExpiresAt = time.Now().Add(ttl)
	return lease, nil
}
func (e2eLeaseStore) Release(context.Context, tenant.TenantContext, storage.Lease) error { return nil }
func (e2eLeaseStore) Validate(context.Context, tenant.TenantContext, storage.Lease) error {
	return nil
}

func milvusE2ESetup(t *testing.T) *e2eFixture {
	t.Helper()
	address := os.Getenv("P106E_MILVUS_ADDRESS")
	if address == "" || os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("P106E_MILVUS_ADDRESS and TEST_DATABASE_URL are required")
	}
	password := os.Getenv("P106E_MILVUS_PASSWORD")
	f := rebuildFixtureSetup(t)
	credentials := milvus.Credentials{Username: "root", Password: password}
	baseConfig := vector.BackendConfig{Mode: "milvus", EndpointRef: "env:MILVUS_ENDPOINT", Collection: "p106e_base_" + f.schema, CredentialRef: "env:MILVUS_PASSWORD", Model: "test-model", ModelVersion: "v1", Dimension: 8, SchemaVersion: "schema-v1", Metric: "cosine", MaxInputBytes: vector.MaxContentBytes, MaxMetadataBytes: vector.MaxMetadataBytes, MaxTopK: vector.MaxTopK, OperationTimeout: 8 * time.Second, ReadinessTimeout: 12 * time.Second}
	targetConfig := baseConfig
	targetConfig.Collection = "p106e_target_" + f.schema
	targetConfig.ModelVersion = "v2"
	if err := milvus.ProvisionTestCollection(context.Background(), baseConfig, address, credentials); err != nil {
		t.Fatal(err)
	}
	if err := milvus.ProvisionTestCollection(context.Background(), targetConfig, address, credentials); err != nil {
		t.Fatal(err)
	}
	baseStore, err := milvus.New(context.Background(), baseConfig, staticEndpoint{address}, staticCredentials{credentials})
	if err != nil {
		t.Fatal(err)
	}
	targetStore, err := milvus.New(context.Background(), targetConfig, staticEndpoint{address}, staticCredentials{credentials})
	if err != nil {
		t.Fatal(err)
	}
	baseIdentityReader, readerOK := baseStore.(vector.IdentityReader)
	if !readerOK {
		t.Fatal("milvus store does not implement the identity reader boundary")
	}
	targetIdentityReader, targetOK := targetStore.(vector.IdentityReader)
	if !targetOK {
		t.Fatal("milvus target store does not implement the identity reader boundary")
	}
	t.Cleanup(func() {
		_ = baseStore.Close(context.Background())
		_ = targetStore.Close(context.Background())
	})
	baseProjection := testProjection()
	targetProjection := testProjection()
	targetProjection.ModelVersion = "v2"
	baseFingerprint, err := ProjectionFingerprint(baseProjection)
	if err != nil {
		t.Fatal(err)
	}
	targetFingerprint, err := ProjectionFingerprint(targetProjection)
	if err != nil {
		t.Fatal(err)
	}
	router, err := NewRouter([]ProjectionEntry{
		{Fingerprint: baseFingerprint, Config: baseProjection, Store: baseStore},
		{Fingerprint: targetFingerprint, Config: targetProjection, Store: targetStore},
	})
	if err != nil {
		t.Fatal(err)
	}
	embedder, err := newE2EEmbedder(baseProjection)
	if err != nil {
		t.Fatal(err)
	}
	counting := &countingStore{inner: router}
	registry := &e2eEmbedderRegistry{base: embedder, target: mustTargetEmbedder(t, targetProjection)}
	worker, err := vectortask.NewWorker(vectortask.Config{WorkerID: "rebuild-e2e-worker", Concurrency: 1, PollInterval: 10 * time.Millisecond, LeaseTTL: 5 * time.Second, LeaseRenewInterval: time.Second, TaskTimeout: 30 * time.Second, ShutdownTimeout: 5 * time.Second}, vectortask.Dependencies{
		Repository: f.taskRepo, Leases: e2eLeaseStore{}, Store: counting, Embedder: embedder, Embedders: registry,
		Source: vectortask.SourceProjectorFunc(func(ctx context.Context, tc tenant.TenantContext, task vectortask.Task) (vector.SourceDocument, error) {
			var content string
			var deleted bool
			var version, sequence int64
			var scope string
			if err := f.pool.QueryRow(ctx, `SELECT content, deleted, version, source_seq, scope FROM memory WHERE tenant_id=$1 AND memory_id=$2`, task.TenantID, task.SourceID).Scan(&content, &deleted, &version, &sequence, &scope); err != nil {
				return vector.SourceDocument{}, vector.ErrNotFound
			}
			if task.Operation == vector.OperationDelete {
				deleted = true
				content = ""
			} else if deleted {
				return vector.SourceDocument{}, vector.ErrStale
			}
			return vector.SourceDocument{SourceType: vector.SourceTypeMemory, SourceID: task.SourceID, ProjectionScope: task.ProjectionScope, SourceVersion: version, SourceSequence: sequence, Content: content, Deleted: deleted, Model: c2Model(task), ModelVersion: c2Version(task), Dimension: task.Dimension, SchemaVersion: task.SchemaVersion}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	reader := &flakyReader{inner: router}
	tasks := &e2eTasks{inner: rebuildTasks{tasks: f.taskRepo}}
	coordinator, err := NewCoordinator(Config{Owner: "rebuild-e2e", BatchSize: 10}, Dependencies{
		Pool: f.pool, Runs: f.runs, Tasks: tasks, Leases: e2eLeaseStore{},
		Reader: reader, Projection: baseProjection,
	})
	_ = counting
	if err != nil {
		t.Fatal(err)
	}
	targetCoordinator, err := NewCoordinator(Config{Owner: "rebuild-e2e-target", BatchSize: 10}, Dependencies{
		Pool: f.pool, Runs: f.runs, Tasks: rebuildTasks{tasks: f.taskRepo}, Leases: e2eLeaseStore{},
		Reader: router, Projection: targetProjection,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = targetCoordinator
	if err = worker.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = worker.Stop()
	})
	e2e := &e2eFixture{f: f, baseStore: baseStore, identity: baseIdentityReader, targetStore: targetIdentityReader, counting: counting, router: router, worker: worker, coordinator: coordinator, runs: f.runs, taskRepo: f.taskRepo, pool: f.pool, tc: f.tc, tasks: tasks}
	return e2e
}

func c2Model(task vectortask.Task) string   { return task.Model }
func c2Version(task vectortask.Task) string { return task.ModelVersion }

// flakyReader wraps the identity reader with an injectable outage switch for
// failure-path verification. It never weakens validation.
type flakyReader struct {
	inner vector.IdentityReader
	mu    sync.Mutex
	fail  bool
}

func (r *flakyReader) InspectIdentities(ctx context.Context, documentIDs []string) (map[string]vector.DocumentIdentity, error) {
	r.mu.Lock()
	fail := r.fail
	r.mu.Unlock()
	if fail {
		return nil, errors.New("inspector down")
	}
	return r.inner.InspectIdentities(ctx, documentIDs)
}

func (r *flakyReader) ListIdentities(ctx context.Context, limit int) ([]vector.DocumentIdentity, error) {
	r.mu.Lock()
	fail := r.fail
	r.mu.Unlock()
	if fail {
		return nil, errors.New("inspector down")
	}
	return r.inner.ListIdentities(ctx, limit)
}

func (r *flakyReader) setFail(fail bool) {
	r.mu.Lock()
	r.fail = fail
	r.mu.Unlock()
}

// countingStore records upsert/delete invocations through the router.
type countingStore struct {
	inner     vector.VectorStore
	mu        sync.Mutex
	upserts   int
	lastModel string
}

func (c *countingStore) Ready(ctx context.Context) error { return c.inner.Ready(ctx) }
func (c *countingStore) Upsert(ctx context.Context, request vector.UpsertRequest) error {
	err := c.inner.Upsert(ctx, request)
	c.mu.Lock()
	c.upserts++
	c.lastModel = request.Ref.ModelVersion
	c.mu.Unlock()
	return err
}
func (c *countingStore) Delete(ctx context.Context, request vector.DeleteRequest) error {
	return c.inner.Delete(ctx, request)
}
func (c *countingStore) Search(ctx context.Context, request vector.SearchRequest) ([]vector.SearchResult, error) {
	return c.inner.Search(ctx, request)
}
func (c *countingStore) Close(ctx context.Context) error { return c.inner.Close(ctx) }
func (c *countingStore) InspectIdentities(ctx context.Context, documentIDs []string) (map[string]vector.DocumentIdentity, error) {
	if reader, ok := c.inner.(vector.IdentityReader); ok {
		return reader.InspectIdentities(ctx, documentIDs)
	}
	return nil, vector.ErrInvalidConfig
}
func (c *countingStore) ListIdentities(ctx context.Context, limit int) ([]vector.DocumentIdentity, error) {
	if reader, ok := c.inner.(vector.IdentityReader); ok {
		return reader.ListIdentities(ctx, limit)
	}
	return nil, vector.ErrInvalidConfig
}

type staticEndpoint struct{ address string }

func (s staticEndpoint) Resolve(context.Context, string) (string, error) { return s.address, nil }

type staticCredentials struct{ credentials milvus.Credentials }

func (s staticCredentials) Resolve(context.Context, string) (milvus.Credentials, error) {
	return s.credentials, nil
}

func newE2EEmbedder(projection vector.ProjectionConfig) (vector.EmbeddingProvider, error) {
	return vector.NewDeterministicEmbedder(projection.Model, projection.ModelVersion, projection.Dimension, 1024)
}

func mustTargetEmbedder(t *testing.T, projection vector.ProjectionConfig) vector.EmbeddingProvider {
	t.Helper()
	embedder, err := vector.NewDeterministicEmbedder(projection.Model, projection.ModelVersion, projection.Dimension, 1024)
	if err != nil {
		t.Fatal(err)
	}
	return embedder
}

// e2eEmbedderRegistry is the server-owned per-projection embedder registry for
// the integration fixture. Production has no embedder implementations; this
// registry exists only inside the integration fixture.
type e2eEmbedderRegistry struct {
	base   vector.EmbeddingProvider
	target vector.EmbeddingProvider
}

func (r *e2eEmbedderRegistry) EmbedderFor(model, modelVersion string, dimension int) (vector.EmbeddingProvider, error) {
	if model == "test-model" && dimension == 8 {
		if modelVersion == "v1" {
			return r.base, nil
		}
		if modelVersion == "v2" {
			return r.target, nil
		}
	}
	return nil, vector.ErrInvalidModel
}

func dumpTaskRows(t *testing.T, pool *pgxpool.Pool, tenantID string) {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT source_id, status, attempt, coalesce(last_error_category,'-') FROM vector_projection_task WHERE tenant_id=$1 ORDER BY source_id, source_version`, tenantID)
	if err != nil {
		t.Logf("diag: task query failed: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var sourceID, status string
		var attempt int
		var category string
		_ = rows.Scan(&sourceID, &status, &attempt, &category)
		t.Logf("diag: task source=%s status=%s attempt=%d category=%s", sourceID, status, attempt, category)
	}
}

func TestRebuildMilvusEndToEndWithDriftRepair(t *testing.T) {
	e := milvusE2ESetup(t)
	ctx := tenant.WithContext(context.Background(), e.tc)
	for _, item := range []struct{ id, content string }{
		{"mem-r1", "rebuild content one"},
		{"mem-r2", "rebuild content two"},
		{"mem-r3", "rebuild content three"},
	} {
		e.f.putMemory(t, "tenant-a", item.id, item.content, 1, false)
	}
	run, err := e.coordinator.CreateRun(ctx, e.tc, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.coordinator.Scan(ctx, e.tc, run.RunID); err != nil {
		t.Fatal(err)
	}
	waitNoPendingTasks(t, e.pool, e.tc.TenantID, 45*time.Second)
	// Drift injection: lose one derived document behind PostgreSQL's back.
	var documentID string
	if err = e.pool.QueryRow(ctx, `SELECT document_id FROM vector_projection_task WHERE tenant_id=$1 AND source_id='mem-r2'`, e.tc.TenantID).Scan(&documentID); err != nil {
		t.Fatal(err)
	}
	deleteSource := vector.SourceDocument{SourceType: vector.SourceTypeMemory, SourceID: "mem-r2", ProjectionScope: "memory:user", SourceVersion: 1, SourceSequence: 0, Content: "", Deleted: true, Model: e.f.projection.Model, ModelVersion: e.f.projection.ModelVersion, Dimension: e.f.projection.Dimension, SchemaVersion: e.f.projection.SchemaVersion}
	deleteRequest, deleteErr := vector.BuildDeleteRequest(tenant.WithContext(ctx, e.tc), deleteSource)
	if deleteErr != nil {
		t.Fatal(deleteErr)
	}
	if err = e.baseStore.Delete(ctx, deleteRequest); err != nil {
		t.Fatalf("direct delete drift injection failed: %v", err)
	}
	report, err := e.coordinator.Reconcile(ctx, e.tc, run.RunID, ReconcilePolicy{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if report.Missing != 1 {
		t.Fatalf("expected one missing hit, report=%+v", report)
	}
	// Repair via the normal task boundary, then converge.
	repaired, err := e.coordinator.Reconcile(ctx, e.tc, run.RunID, ReconcilePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if repaired.Repaired != 1 {
		t.Fatalf("repair did not enqueue: %+v", repaired)
	}
	waitNoPendingTasks(t, e.pool, e.tc.TenantID, 45*time.Second)
	if _, err = e.coordinator.Reconcile(ctx, e.tc, run.RunID, ReconcilePolicy{DryRun: true}); err != nil {
		t.Fatal(err)
	}
	e.counting.mu.Lock()
	t.Logf("diag: worker upserts=%d lastModel=%s", e.counting.upserts, e.counting.lastModel)
	e.counting.mu.Unlock()
	e.tasks.mu.Lock()
	t.Logf("diag: redrive calls=%d", e.tasks.redrives)
	e.tasks.mu.Unlock()
	baseList, baseErr := e.identity.ListIdentities(ctx, 200)
	t.Logf("diag: base list err=%v count=%d", baseErr, len(baseList))
	for _, identity := range baseList {
		t.Logf("diag: base has %s v%d", identity.DocumentID[:16], identity.SourceVersion)
	}
	targetList, targetErr := e.targetStore.ListIdentities(ctx, 200)
	t.Logf("diag: target list err=%v count=%d", targetErr, len(targetList))
	for _, identity := range targetList {
		t.Logf("diag: target has %s v%d", identity.DocumentID[:16], identity.SourceVersion)
	}
	completedRun, finalReport, err := e.coordinator.ConvergeAndComplete(ctx, e.tc, run.RunID, ReconcilePolicy{DryRun: true})
	if err != nil {
		t.Fatalf("converge failed: %v report=%+v", err, finalReport)
	}
	if completedRun.Phase != PhaseCompleted {
		t.Fatalf("unexpected final phase: %+v", completedRun)
	}
}

func TestRebuildMilvusTombstoneAndProjectionTarget(t *testing.T) {
	e := milvusE2ESetup(t)
	ctx := tenant.WithContext(context.Background(), e.tc)
	e.f.putMemory(t, "tenant-a", "mem-t1", "tombstone target content", 1, false)
	run, err := e.coordinator.CreateRun(ctx, e.tc, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.coordinator.Scan(ctx, e.tc, run.RunID); err != nil {
		t.Fatal(err)
	}
	// Projection migration: pre-provisioned target rebuild converges; the
	// cutover itself stays BLOCKED (no atomic alias protocol), and the old
	// projection is preserved.
	targetProjection := testProjection()
	targetProjection.ModelVersion = "v2"
	targetFingerprint, err := ProjectionFingerprint(targetProjection)
	if err != nil {
		t.Fatal(err)
	}
	targetRun, err := e.runs.Create(ctx, e.tc, Run{TenantID: e.tc.TenantID, RunID: NewRunID(e.tc.TenantID, targetFingerprint, time.Now().UTC()), Fingerprint: targetFingerprint, Mode: "rebuild", MaxAttempts: 20, DeadlineAt: time.Now().Add(10 * time.Minute)}, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	targetCoordinator, err := NewCoordinator(Config{Owner: "rebuild-e2e-target", BatchSize: 10}, Dependencies{
		Pool: e.pool, Runs: e.runs, Tasks: rebuildTasks{tasks: e.taskRepo}, Leases: e2eLeaseStore{},
		Reader: e.router, Projection: targetProjection,
	})
	if err != nil {
		t.Fatal(err)
	}
	targetScan, err := targetCoordinator.Scan(ctx, e.tc, targetRun.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if targetScan.Enqueued != 1 {
		var diagCount int64
		_ = e.pool.QueryRow(ctx, `SELECT count(*) FROM vector_projection_task WHERE tenant_id=$1 AND source_id=$2`, e.tc.TenantID, "mem-t1").Scan(&diagCount)
		t.Fatalf("target rebuild enqueued %d tasks (scan=%+v taskRows=%d)", targetScan.Enqueued, targetScan, diagCount)
	}
	waitNoPendingTasks(t, e.pool, e.tc.TenantID, 45*time.Second)
	completedRun, finalReport, err := targetCoordinator.ConvergeAndComplete(ctx, e.tc, targetRun.RunID, ReconcilePolicy{DryRun: true})
	if err != nil {
		t.Fatalf("target converge failed: %v report=%+v", err, finalReport)
	}
	if completedRun.Phase != PhaseCompleted || finalReport.Consistent != 1 {
		t.Fatalf("unexpected target completion: %+v report=%+v", completedRun, finalReport)
	}
	waitNoPendingTasks(t, e.pool, e.tc.TenantID, 45*time.Second)
	// Tombstone the source: the derived index now holds a live hit that
	// reconciliation must repair via a delete task through the same task
	// boundary, before the base run completes.
	if _, err = e.pool.Exec(ctx, `UPDATE memory SET deleted=true, content='', version=2 WHERE tenant_id=$1 AND memory_id='mem-t1'`, e.tc.TenantID); err != nil {
		t.Fatal(err)
	}
	report, err := e.coordinator.Reconcile(ctx, e.tc, run.RunID, ReconcilePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if report.Tombstoned != 1 || report.Repaired != 1 {
		t.Fatalf("expected tombstoned-present repair: %+v", report)
	}
	waitNoPendingTasks(t, e.pool, e.tc.TenantID, 45*time.Second)
	if _, _, err = e.coordinator.ConvergeAndComplete(ctx, e.tc, run.RunID, ReconcilePolicy{DryRun: true}); err != nil {
		t.Fatalf("base converge failed: %v", err)
	}
	// Cutover is explicitly not performed: no alias protocol exists, the old
	// projection stays intact, and retrieval composition still points at the
	// base projection. Recorded as BLOCKED in the phase report.
	t.Logf("projection cutover: BLOCKED (pre-provisioned target rebuild verified)")
}

func TestRebuildMilvusOutageFailsSafely(t *testing.T) {
	e := milvusE2ESetup(t)
	ctx := tenant.WithContext(context.Background(), e.tc)
	e.f.putMemory(t, "tenant-a", "mem-o1", "outage content", 1, false)
	run, err := e.coordinator.CreateRun(ctx, e.tc, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = e.coordinator.Scan(ctx, e.tc, run.RunID); err != nil {
		t.Fatal(err)
	}
	waitNoPendingTasks(t, e.pool, e.tc.TenantID, 45*time.Second)
	reader := e.coordinator.deps.Reader.(*flakyReader)
	reader.setFail(true)
	if _, err = e.coordinator.Reconcile(ctx, e.tc, run.RunID, ReconcilePolicy{DryRun: true}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("inspector outage not classified: %v", err)
	}
	reader.setFail(false)
	if _, _, err = e.coordinator.ConvergeAndComplete(ctx, e.tc, run.RunID, ReconcilePolicy{DryRun: true}); err != nil {
		t.Fatalf("recovery converge failed: %v", err)
	}
}

func mustUpsertRef(t *testing.T, e *e2eFixture, sourceID, content string, version int64) vector.VectorDocumentRef {
	t.Helper()
	source := vector.SourceDocument{SourceType: vector.SourceTypeMemory, SourceID: sourceID, ProjectionScope: "memory:user", SourceVersion: version, SourceSequence: 0, Content: content, Model: e.f.projection.Model, ModelVersion: e.f.projection.ModelVersion, Dimension: e.f.projection.Dimension, SchemaVersion: e.f.projection.SchemaVersion}
	ref, err := vector.BuildDocumentRef(tenant.WithContext(context.Background(), e.tc), source)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func waitNoPendingTasks(t *testing.T, pool *pgxpool.Pool, tenantID string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		var pending int64
		if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM vector_projection_task WHERE tenant_id=$1 AND status IN ('pending','retry_wait','running')`, tenantID).Scan(&pending); err != nil {
			t.Fatal(err)
		}
		if pending == 0 {
			var bad int64
			if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM vector_projection_task WHERE tenant_id=$1 AND status IN ('dead_letter','stale','cancelled')`, tenantID).Scan(&bad); err != nil {
				t.Fatal(err)
			}
			if bad != 0 {
				var statuses []string
				rows, err := pool.Query(context.Background(), `SELECT status || ':' || source_id || ':' || operation || ':' || coalesce(last_error_category,'-') || ':v' || source_version FROM vector_projection_task WHERE tenant_id=$1 AND status IN ('dead_letter','stale','cancelled')`, tenantID)
				if err == nil {
					for rows.Next() {
						var value string
						_ = rows.Scan(&value)
						statuses = append(statuses, value)
					}
					rows.Close()
				}
				t.Fatalf("tasks reached failure terminals: %v", statuses)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("tasks did not reach terminal state")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
