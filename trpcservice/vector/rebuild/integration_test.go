//go:build integration

package rebuild

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/internal/testinfra"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	pgstore "github.com/liuzengh/trpc-agent-service/trpcservice/storage/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
	vectortask "github.com/liuzengh/trpc-agent-service/trpcservice/vector/task"
)

type rebuildFixture struct {
	pool        *pgxpool.Pool
	runs        *PostgresRunRepository
	coordinator *Coordinator
	taskRepo    *vectortask.PostgresRepository
	tc          tenant.TenantContext
	projection  vector.ProjectionConfig
	base        *pgxpool.Pool
	schema      string
	migrator    interface {
		Down(context.Context, int) error
	}
}

type rebuildLeases struct{}

func (rebuildLeases) Acquire(context.Context, tenant.TenantContext, string, string, time.Duration) (storage.Lease, error) {
	return storage.Lease{OwnerID: "test-owner", FenceToken: 1, Epoch: 1, ExpiresAt: time.Now().Add(time.Minute)}, nil
}
func (l rebuildLeases) Renew(_ context.Context, _ tenant.TenantContext, lease storage.Lease, ttl time.Duration) (storage.Lease, error) {
	lease.ExpiresAt = time.Now().Add(ttl)
	return lease, nil
}
func (rebuildLeases) Release(context.Context, tenant.TenantContext, storage.Lease) error {
	return nil
}
func (rebuildLeases) Validate(context.Context, tenant.TenantContext, storage.Lease) error {
	return nil
}

type rebuildTasks struct {
	tasks *vectortask.PostgresRepository
}

func (a rebuildTasks) EnqueueTx(ctx context.Context, tx pgx.Tx, tc tenant.TenantContext, ref vector.VectorDocumentRef, now time.Time) (bool, error) {
	outcome, err := a.tasks.EnqueueTx(ctx, tx, tc, ref, now)
	if err != nil {
		return false, err
	}
	return outcome.Created, nil
}

func (a rebuildTasks) RedriveTx(ctx context.Context, tx pgx.Tx, tc tenant.TenantContext, ref vector.VectorDocumentRef, now time.Time) (bool, error) {
	return a.tasks.RedriveTx(ctx, tx, tc, ref, now)
}

func rebuildFixtureSetup(t *testing.T) *rebuildFixture {
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
	schema := "p106e_rebuild_" + strings.ToLower(fmt.Sprintf("%x", time.Now().UnixNano()))
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
	if _, err = pool.Exec(ctx, `INSERT INTO tenant (tenant_id, name, status, config_version) VALUES ('tenant-a', 'rebuild a', 'active', 1), ('tenant-b', 'rebuild b', 'active', 1)`); err != nil {
		t.Fatal(err)
	}
	projection := testProjection()
	taskRepo, err := vectortask.NewPostgresRepository(pool, vectortask.RepositoryConfig{MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	runs, err := NewPostgresRunRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewCoordinator(Config{Owner: "rebuild-owner", BatchSize: 10}, Dependencies{
		Pool: pool, Runs: runs, Tasks: rebuildTasks{tasks: taskRepo}, Leases: rebuildLeases{},
		Reader:     &fixtureReader{identities: map[string]vector.DocumentIdentity{}, listed: nil},
		Projection: projection,
	})
	if err != nil {
		t.Fatal(err)
	}
	tc := tenant.TenantContext{TenantID: "tenant-a", AgentAppID: "vector-worker", BindingID: "vector-worker", Channel: "vector", RequestID: "req", MessageID: "msg", TraceID: "trace", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "postgres"}}
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
	return &rebuildFixture{pool: pool, runs: runs, coordinator: coordinator, taskRepo: taskRepo, tc: tc, projection: projection, base: base, schema: schema, migrator: migrator}
}

func (f *rebuildFixture) putMemory(t *testing.T, tenantID, id, content string, version int64, deleted bool) {
	t.Helper()
	if _, err := f.pool.Exec(context.Background(), `INSERT INTO memory (tenant_id, memory_id, scope, scope_id, kind, content, version, source_seq, deleted)
        VALUES ($1,$2,'user','scope-user','fact',$3,$4,0,$5)
        ON CONFLICT (tenant_id, memory_id) DO UPDATE SET content=EXCLUDED.content, version=EXCLUDED.version, deleted=EXCLUDED.deleted, updated_at=now()`,
		tenantID, id, content, version, deleted); err != nil {
		t.Fatal(err)
	}
}

type fixtureReader struct {
	mu         sync.Mutex
	identities map[string]vector.DocumentIdentity
	listed     []vector.DocumentIdentity
	fail       bool
}

func (r *fixtureReader) InspectIdentities(_ context.Context, documentIDs []string) (map[string]vector.DocumentIdentity, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail {
		return nil, errors.New("inspector down")
	}
	result := make(map[string]vector.DocumentIdentity, len(documentIDs))
	for _, id := range documentIDs {
		if identity, ok := r.identities[id]; ok {
			result[id] = identity
		}
	}
	return result, nil
}

func (r *fixtureReader) ListIdentities(_ context.Context, limit int) ([]vector.DocumentIdentity, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail {
		return nil, errors.New("inspector down")
	}
	if len(r.listed) > limit {
		return r.listed[:limit], nil
	}
	return r.listed, nil
}

func TestRebuildScanEnqueuesAndCompletesCursor(t *testing.T) {
	f := rebuildFixtureSetup(t)
	for i := 0; i < 25; i++ {
		f.putMemory(t, "tenant-a", fmt.Sprintf("mem-%03d", i), fmt.Sprintf("content %d", i), 1, false)
	}
	now := time.Now().UTC()
	run, err := f.coordinator.CreateRun(context.Background(), f.tc, now)
	if err != nil {
		t.Fatal(err)
	}
	scanned, err := f.coordinator.Scan(context.Background(), f.tc, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if scanned.Phase != PhaseScanned || scanned.Scanned != 25 || scanned.Enqueued != 25 || scanned.Cursor != "mem-024" {
		t.Fatalf("unexpected scan result: %+v", scanned)
	}
	var pendingCount int64
	if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM vector_projection_task WHERE tenant_id=$1`, f.tc.TenantID).Scan(&pendingCount); err != nil {
		t.Fatal(err)
	}
	if pendingCount != 25 {
		t.Fatalf("unexpected task count: %d", pendingCount)
	}
	// Repeated Scan is idempotent for a scanned run.
	again, err := f.coordinator.Scan(context.Background(), f.tc, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if again.Phase != PhaseScanned {
		t.Fatalf("rescan mutated phase: %+v", again)
	}
}

func TestRebuildTombstoneScanProducesDeleteTasks(t *testing.T) {
	f := rebuildFixtureSetup(t)
	f.putMemory(t, "tenant-a", "mem-live", "live", 1, false)
	f.putMemory(t, "tenant-a", "mem-dead", "", 1, true)
	run, err := f.coordinator.CreateRun(context.Background(), f.tc, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	scanned, err := f.coordinator.Scan(context.Background(), f.tc, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if scanned.Tombstoned != 1 || scanned.Enqueued != 1 {
		t.Fatalf("unexpected counters: %+v", scanned)
	}
	var operation string
	if err := f.pool.QueryRow(context.Background(), `SELECT operation FROM vector_projection_task WHERE tenant_id=$1 AND source_id='mem-dead'`, f.tc.TenantID).Scan(&operation); err != nil {
		t.Fatal(err)
	}
	if operation != "delete" {
		t.Fatalf("expected delete task, got %s", operation)
	}
}

func TestRebuildResumeFromDurableCursor(t *testing.T) {
	f := rebuildFixtureSetup(t)
	for i := 0; i < 12; i++ {
		f.putMemory(t, "tenant-a", fmt.Sprintf("mem-%02d", i), fmt.Sprintf("content %d", i), 1, false)
	}
	run, err := f.coordinator.CreateRun(context.Background(), f.tc, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a crash after the first batch: run one scan with a one-batch
	// budget by claiming manually and advancing only part of the keyset.
	leaseRef := LeaseRef{Owner: "crashed-owner", Epoch: 1, Fence: 7, ExpiresAt: time.Now().Add(-time.Second)}
	if _, err = f.runs.Claim(context.Background(), f.tc, run.RunID, leaseRef, 20, time.Now().UTC().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err = f.pool.Exec(context.Background(), `UPDATE vector_rebuild_run SET cursor_memory_id='mem-04', scanned=5, enqueued=5 WHERE tenant_id=$1 AND run_id=$2`, f.tc.TenantID, run.RunID); err != nil {
		t.Fatal(err)
	}
	var tasksBefore int64
	if err = f.pool.QueryRow(context.Background(), `SELECT count(*) FROM vector_projection_task WHERE tenant_id=$1`, f.tc.TenantID).Scan(&tasksBefore); err != nil {
		t.Fatal(err)
	}
	if tasksBefore != 0 {
		t.Fatalf("crash simulation enqueued tasks: %d", tasksBefore)
	}
	scanned, err := f.coordinator.Scan(context.Background(), f.tc, run.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if scanned.Cursor != "mem-11" || scanned.Scanned != 12 {
		t.Fatalf("resume did not continue from cursor: %+v", scanned)
	}
	var tasksAfter int64
	if err = f.pool.QueryRow(context.Background(), `SELECT count(*) FROM vector_projection_task WHERE tenant_id=$1`, f.tc.TenantID).Scan(&tasksAfter); err != nil {
		t.Fatal(err)
	}
	if tasksAfter != 7 {
		t.Fatalf("resume enqueued wrong task count: %d", tasksAfter)
	}
}

func TestRebuildFencingRejectsExpiredOwner(t *testing.T) {
	f := rebuildFixtureSetup(t)
	f.putMemory(t, "tenant-a", "mem-1", "content", 1, false)
	run, err := f.coordinator.CreateRun(context.Background(), f.tc, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	oldLease := LeaseRef{Owner: "old-owner", Epoch: 1, Fence: 3, ExpiresAt: time.Now().Add(-time.Minute)}
	if _, err = f.runs.Claim(context.Background(), f.tc, run.RunID, oldLease, 20, time.Now().UTC().Add(-2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	newLease := LeaseRef{Owner: "new-owner", Epoch: 1, Fence: 4, ExpiresAt: time.Now().Add(time.Minute)}
	if _, err = f.runs.Claim(context.Background(), f.tc, run.RunID, newLease, 20, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	// Expired old owner cannot advance the cursor.
	tx, err := f.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = f.runs.AdvanceCursorTx(context.Background(), tx, f.tc, run.RunID, oldLease, "mem-1", 1, 1, 0, time.Now().UTC()); !errors.Is(err, ErrConflictOwner) {
		t.Fatalf("expired owner advanced cursor: %v", err)
	}
	_ = tx.Rollback(context.Background())
	// Old owner cannot complete or fail the run.
	if _, err = f.runs.Complete(context.Background(), f.tc, run.RunID, oldLease, time.Now().UTC()); !errors.Is(err, ErrConflictOwner) {
		t.Fatalf("expired owner completed run: %v", err)
	}
	if _, err = f.runs.Fail(context.Background(), f.tc, run.RunID, oldLease, CategoryDeadline, time.Now().UTC()); !errors.Is(err, ErrConflictOwner) {
		t.Fatalf("expired owner failed run: %v", err)
	}
	// Current owner still can advance and complete.
	tx, err = f.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = f.runs.AdvanceCursorTx(context.Background(), tx, f.tc, run.RunID, newLease, "mem-1", 1, 1, 0, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err = f.runs.MarkScanned(context.Background(), f.tc, run.RunID, newLease, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	completed, err := f.runs.Complete(context.Background(), f.tc, run.RunID, newLease, time.Now().UTC())
	if err != nil || completed.Phase != PhaseCompleted {
		t.Fatalf("current owner completion failed: %+v err=%v", completed, err)
	}
}

func TestRebuildAttemptBudgetFailsClosed(t *testing.T) {
	f := rebuildFixtureSetup(t)
	run, err := f.coordinator.CreateRun(context.Background(), f.tc, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		lease := LeaseRef{Owner: fmt.Sprintf("owner-%d", i), Epoch: 1, Fence: uint64(i + 1), ExpiresAt: time.Now().Add(-2 * time.Minute)}
		if _, err = f.runs.Claim(context.Background(), f.tc, run.RunID, lease, 20, time.Now().UTC().Add(-time.Minute)); err != nil {
			t.Fatalf("claim %d failed: %v", i, err)
		}
	}
	lease := LeaseRef{Owner: "owner-21", Epoch: 1, Fence: 99, ExpiresAt: time.Now().Add(time.Minute)}
	if _, err = f.runs.Claim(context.Background(), f.tc, run.RunID, lease, 20, time.Now().UTC()); !errors.Is(err, ErrConflictOwner) {
		t.Fatalf("attempt budget not enforced: %v", err)
	}
	var attempt int
	var phase string
	if err = f.pool.QueryRow(context.Background(), `SELECT attempt, phase FROM vector_rebuild_run WHERE tenant_id=$1 AND run_id=$2`, f.tc.TenantID, run.RunID).Scan(&attempt, &phase); err != nil {
		t.Fatal(err)
	}
	if attempt != 20 {
		t.Fatalf("unexpected attempts: %d", attempt)
	}
	_ = phase
}

func TestRebuildTenantIsolation(t *testing.T) {
	f := rebuildFixtureSetup(t)
	f.putMemory(t, "tenant-a", "mem-a", "tenant a", 1, false)
	f.putMemory(t, "tenant-b", "mem-b", "tenant b", 1, false)
	runA, err := f.coordinator.CreateRun(context.Background(), f.tc, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.coordinator.Scan(context.Background(), f.tc, runA.RunID); err != nil {
		t.Fatal(err)
	}
	var countA int64
	if err = f.pool.QueryRow(context.Background(), `SELECT count(*) FROM vector_projection_task WHERE tenant_id=$1`, "tenant-a").Scan(&countA); err != nil {
		t.Fatal(err)
	}
	if countA != 1 {
		t.Fatalf("tenant-a task count: %d", countA)
	}
	var countB int64
	if err = f.pool.QueryRow(context.Background(), `SELECT count(*) FROM vector_projection_task WHERE tenant_id=$1`, "tenant-b").Scan(&countB); err != nil {
		t.Fatal(err)
	}
	if countB != 0 {
		t.Fatalf("tenant-b contaminated: %d", countB)
	}
	// Cross-tenant run access is rejected.
	other := tenant.TenantContext{TenantID: "tenant-b", AgentAppID: "a", BindingID: "b", Channel: "vector", RequestID: "r", MessageID: "m", TraceID: "t", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "postgres"}}
	if _, err = f.runs.Get(context.Background(), other, runA.RunID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant run readable: %v", err)
	}
}

func TestReconcileDryRunReportsWithoutTasks(t *testing.T) {
	f := rebuildFixtureSetup(t)
	f.putMemory(t, "tenant-a", "mem-1", "content one", 2, false)
	run, err := f.coordinator.CreateRun(context.Background(), f.tc, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.coordinator.Scan(context.Background(), f.tc, run.RunID); err != nil {
		t.Fatal(err)
	}
	report, err := f.coordinator.Reconcile(context.Background(), f.tc, run.RunID, ReconcilePolicy{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if !report.DryRun || report.Missing != 1 {
		t.Fatalf("unexpected dry-run report: %+v", report)
	}
	var taskCount int64
	if err = f.pool.QueryRow(context.Background(), `SELECT count(*) FROM vector_projection_task WHERE tenant_id=$1 AND source_id='mem-1' AND source_version=3`, f.tc.TenantID).Scan(&taskCount); err != nil {
		t.Fatal(err)
	}
	if taskCount != 0 {
		t.Fatal("dry run produced repair tasks")
	}
}

func TestReconcileRepairConvergesAndCompletes(t *testing.T) {
	f := rebuildFixtureSetup(t)
	f.putMemory(t, "tenant-a", "mem-1", "content one", 2, false)
	run, err := f.coordinator.CreateRun(context.Background(), f.tc, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.coordinator.Scan(context.Background(), f.tc, run.RunID); err != nil {
		t.Fatal(err)
	}
	// Inspector knows nothing: everything missing → repair round enqueues
	// refresh tasks, then converges once the reader reflects the index.
	report, err := f.coordinator.Reconcile(context.Background(), f.tc, run.RunID, ReconcilePolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if report.Missing != 1 || report.Repaired != 1 {
		t.Fatalf("unexpected repair report: %+v", report)
	}
	// Simulate convergence: reader now reports the projected identity.
	var taskRow vectortask.Task
	var lastError, leaseOwner *string
	var leaseEpoch, leaseFence *int64
	var leaseExpires, claimed, completed, deadLettered *time.Time
	var operation, status string
	if err = f.pool.QueryRow(context.Background(), `SELECT tenant_id, task_id, source_type, source_id, projection_scope, document_id, operation, source_version, source_sequence, content_hash, model, model_version, dimension, schema_version, status, attempt, max_attempts, next_attempt_at, last_error_category, lease_owner, lease_epoch, lease_fence, lease_expires_at, claimed_at, completed_at, dead_lettered_at, created_at, updated_at
		FROM vector_projection_task WHERE tenant_id=$1 AND source_id='mem-1' ORDER BY source_version DESC LIMIT 1`, f.tc.TenantID).Scan(
		&taskRow.TenantID, &taskRow.TaskID, &taskRow.SourceType, &taskRow.SourceID, &taskRow.ProjectionScope, &taskRow.DocumentID, &operation, &taskRow.SourceVersion, &taskRow.SourceSequence, &taskRow.ContentHash,
		&taskRow.Model, &taskRow.ModelVersion, &taskRow.Dimension, &taskRow.SchemaVersion, &status, &taskRow.Attempt, &taskRow.MaxAttempts, &taskRow.NextAttemptAt, &lastError, &leaseOwner,
		&leaseEpoch, &leaseFence, &leaseExpires, &claimed, &completed, &deadLettered, &taskRow.CreatedAt, &taskRow.UpdatedAt); err != nil {
		t.Fatal(err)
	}
	identity := vector.DocumentIdentity{
		TenantID: taskRow.TenantID, DocumentID: taskRow.DocumentID, SourceType: taskRow.SourceType,
		SourceID: taskRow.SourceID, ProjectionScope: taskRow.ProjectionScope,
		SourceVersion: taskRow.SourceVersion, SourceSequence: taskRow.SourceSequence,
		ContentHash: taskRow.ContentHash, Operation: vector.OperationUpsert,
		Model: taskRow.Model, ModelVersion: taskRow.ModelVersion,
		Dimension: taskRow.Dimension, SchemaVersion: taskRow.SchemaVersion,
	}
	reader := f.coordinator.deps.Reader.(*fixtureReader)
	reader.mu.Lock()
	reader.identities[identity.DocumentID] = identity
	reader.mu.Unlock()
	// Mark the pending repair task succeeded so the convergence gate passes.
	if _, err = f.pool.Exec(context.Background(), `UPDATE vector_projection_task SET status='succeeded', completed_at=now() WHERE tenant_id=$1 AND source_id='mem-1'`, f.tc.TenantID); err != nil {
		t.Fatal(err)
	}
	completedRun, finalReport, err := f.coordinator.ConvergeAndComplete(context.Background(), f.tc, run.RunID, ReconcilePolicy{DryRun: true})
	if err != nil {
		t.Fatalf("converge failed: %v report=%+v", err, finalReport)
	}
	if completedRun.Phase != PhaseCompleted || finalReport.Consistent != 1 {
		t.Fatalf("unexpected completion: %+v report=%+v", completedRun, finalReport)
	}
}
