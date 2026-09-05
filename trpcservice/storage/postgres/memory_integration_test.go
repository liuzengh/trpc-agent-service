//go:build integration

package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/internal/testinfra"
	"github.com/liuzengh/trpc-agent-service/trpcservice/memory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
	vectortask "github.com/liuzengh/trpc-agent-service/trpcservice/vector/task"
)

func memoryIntegrationFixture(t *testing.T) (*pgxBundle, *MemoryRepository, *vectortask.PostgresRepository, tenant.TenantContext, MemoryProjectionConfig) {
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
	bundle := openMigratedBundle(t, pgURL)
	if _, err := bundle.pool.Exec(ctx, `INSERT INTO tenant (tenant_id, name, status, config_version) VALUES ('tenant-a', 'memory a', 'active', 1), ('tenant-b', 'memory b', 'active', 1) ON CONFLICT DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	projection := MemoryProjectionConfig{Model: "test-model", ModelVersion: "v1", SchemaVersion: "schema-v1", Dimension: 8}
	taskRepo, err := vectortask.NewPostgresRepository(bundle.pool, vectortask.RepositoryConfig{MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	repo, err := NewMemoryRepository(bundle.pool, memoryTaskEnqueuer{tasks: taskRepo}, projection)
	if err != nil {
		t.Fatal(err)
	}
	tc := tenant.TenantContext{TenantID: "tenant-a", AgentAppID: "vector-worker", BindingID: "vector-worker", Channel: "vector", RequestID: "req", MessageID: "msg", TraceID: "trace", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "postgres"}}
	return bundle, repo, taskRepo, tc, projection
}

func memoryValue(tenantID, id, content string) memory.Memory {
	return memory.Memory{TenantID: tenantID, ID: id, Scope: memory.ScopeUser, ScopeID: "scope-user", Kind: "fact", Content: content}
}

func TestMemoryPutEnqueuesTaskAtomically(t *testing.T) {
	bundle, repo, taskRepo, tc, projection := memoryIntegrationFixture(t)
	ctx := tenant.WithContext(context.Background(), tc)
	if err := repo.Put(ctx, tc, memoryValue("tenant-a", "mem-1", "first content")); err != nil {
		t.Fatal(err)
	}
	value, err := repo.Get(ctx, tc, "mem-1")
	if err != nil {
		t.Fatal(err)
	}
	if value.Version != 1 || value.SourceSeq != 0 || value.Deleted {
		t.Fatalf("unexpected first version: %+v", value)
	}
	var taskState string
	var taskVersion, taskSequence int64
	if err := bundle.pool.QueryRow(ctx, `SELECT status, source_version, source_sequence FROM vector_projection_task WHERE tenant_id=$1 AND source_id=$2`, "tenant-a", "mem-1").Scan(&taskState, &taskVersion, &taskSequence); err != nil {
		t.Fatal(err)
	}
	if taskState != "pending" || taskVersion != 1 || taskSequence != 0 {
		t.Fatalf("unexpected task: state=%s version=%d sequence=%d", taskState, taskVersion, taskSequence)
	}
	_ = projection
	// Each mutation version carries its own task identity.
	if err := repo.Put(ctx, tc, memoryValue("tenant-a", "mem-1", "first content")); err != nil {
		t.Fatal(err)
	}
	var taskCount int
	if err := bundle.pool.QueryRow(ctx, `SELECT count(*) FROM vector_projection_task WHERE tenant_id=$1 AND source_id=$2`, "tenant-a", "mem-1").Scan(&taskCount); err != nil {
		t.Fatal(err)
	}
	if taskCount != 2 {
		t.Fatalf("expected one task per mutation version, got %d", taskCount)
	}
	// Re-enqueueing the identical logical task (same ref) converges without a
	// second row, proving the P1-06C dedup key is unchanged.
	second, err := repo.Get(ctx, tc, "mem-1")
	if err != nil {
		t.Fatal(err)
	}
	ref, err := buildMemoryRef(tenant.WithContext(ctx, tc), second, projection)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = taskRepo.Enqueue(ctx, tc, ref, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := bundle.pool.QueryRow(ctx, `SELECT count(*) FROM vector_projection_task WHERE tenant_id=$1 AND source_id=$2`, "tenant-a", "mem-1").Scan(&taskCount); err != nil {
		t.Fatal(err)
	}
	if taskCount != 2 {
		t.Fatalf("duplicate enqueue created a new task: %d", taskCount)
	}
}

func TestMemoryRollbackLeavesNoTask(t *testing.T) {
	bundle, repo, _, tc, _ := memoryIntegrationFixture(t)
	ctx := tenant.WithContext(context.Background(), tc)
	if err := repo.Put(ctx, tc, memoryValue("tenant-a", "mem-r", "rollback content")); err != nil {
		t.Fatal(err)
	}
	// Force the task insert to fail inside the transaction by exceeding the
	// projection config: an invalid caller value must abort the whole write.
	bad := memoryValue("tenant-a", "mem-bad", "x")
	bad.Scope = "bogus"
	if err := repo.Put(ctx, tc, bad); !errors.Is(err, storage.ErrInvalidArgument) {
		t.Fatalf("invalid memory accepted: %v", err)
	}
	var memoryCount, taskCount int
	if err := bundle.pool.QueryRow(ctx, `SELECT count(*) FROM memory WHERE tenant_id=$1 AND memory_id=$2`, "tenant-a", "mem-bad").Scan(&memoryCount); err != nil {
		t.Fatal(err)
	}
	if err := bundle.pool.QueryRow(ctx, `SELECT count(*) FROM vector_projection_task WHERE tenant_id=$1 AND source_id=$2`, "tenant-a", "mem-bad").Scan(&taskCount); err != nil {
		t.Fatal(err)
	}
	if memoryCount != 0 || taskCount != 0 {
		t.Fatalf("invalid mutation left rows: memory=%d task=%d", memoryCount, taskCount)
	}
}

func TestMemoryVersionProgressionAndTombstone(t *testing.T) {
	bundle, repo, _, tc, _ := memoryIntegrationFixture(t)
	ctx := tenant.WithContext(context.Background(), tc)
	if err := repo.Put(ctx, tc, memoryValue("tenant-a", "mem-t", "content v1")); err != nil {
		t.Fatal(err)
	}
	if err := repo.Put(ctx, tc, memoryValue("tenant-a", "mem-t", "content v2")); err != nil {
		t.Fatal(err)
	}
	value, err := repo.Get(ctx, tc, "mem-t")
	if err != nil {
		t.Fatal(err)
	}
	if value.Version != 2 || value.SourceSeq != 1 || value.Content != "content v2" {
		t.Fatalf("unexpected progression: %+v", value)
	}
	if err := repo.Delete(ctx, tc, "mem-t"); err != nil {
		t.Fatal(err)
	}
	value, err = repo.Get(ctx, tc, "mem-t")
	if err != nil {
		t.Fatal(err)
	}
	if !value.Deleted || value.Version != 3 || value.Content != "" {
		t.Fatalf("unexpected tombstone: %+v", value)
	}
	var operation string
	if err := bundle.pool.QueryRow(ctx, `SELECT operation FROM vector_projection_task WHERE tenant_id=$1 AND source_id=$2 ORDER BY source_version DESC LIMIT 1`, "tenant-a", "mem-t").Scan(&operation); err != nil {
		t.Fatal(err)
	}
	if operation != "delete" {
		t.Fatalf("expected delete task, got %s", operation)
	}
	// Idempotent tombstone: no new task version.
	if err := repo.Delete(ctx, tc, "mem-t"); err != nil {
		t.Fatal(err)
	}
	var deleteCount int
	if err := bundle.pool.QueryRow(ctx, `SELECT count(*) FROM vector_projection_task WHERE tenant_id=$1 AND source_id=$2 AND operation='delete'`, "tenant-a", "mem-t").Scan(&deleteCount); err != nil {
		t.Fatal(err)
	}
	if deleteCount != 1 {
		t.Fatalf("repeated delete created extra tasks: %d", deleteCount)
	}
	if err := repo.Delete(ctx, tc, "mem-missing"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("missing delete not classified: %v", err)
	}
}

func TestMemoryTenantIsolation(t *testing.T) {
	_, repo, _, tc, _ := memoryIntegrationFixture(t)
	other := tenant.TenantContext{TenantID: "tenant-b", AgentAppID: "vector-worker", BindingID: "vector-worker", Channel: "vector", RequestID: "req", MessageID: "msg", TraceID: "trace", ConfigVersion: 1, BackendPolicy: tenant.BackendPolicy{Session: "postgres", Memory: "postgres", Vector: "none", Object: "postgres"}}
	ctx := tenant.WithContext(context.Background(), tc)
	if err := repo.Put(ctx, tc, memoryValue("tenant-a", "mem-iso", "tenant a secret content")); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Get(tenant.WithContext(context.Background(), other), other, "mem-iso"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("cross-tenant read not rejected: %v", err)
	}
	if err := repo.Delete(tenant.WithContext(context.Background(), other), other, "mem-iso"); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("cross-tenant delete not rejected: %v", err)
	}
	if err := repo.Put(tenant.WithContext(context.Background(), other), other, memoryValue("tenant-a", "mem-cross", "cross")); !errors.Is(err, storage.ErrTenantMismatch) {
		t.Fatalf("cross-tenant write not rejected: %v", err)
	}
	results, err := repo.Search(tenant.WithContext(context.Background(), other), other, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("cross-tenant search returned rows: %d", len(results))
	}
}

func TestMemorySearchBoundedAndActiveOnly(t *testing.T) {
	_, repo, _, tc, _ := memoryIntegrationFixture(t)
	ctx := tenant.WithContext(context.Background(), tc)
	for i := 0; i < 5; i++ {
		if err := repo.Put(ctx, tc, memoryValue("tenant-a", fmt.Sprintf("mem-s%d", i), fmt.Sprintf("content %d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.Delete(ctx, tc, "mem-s4"); err != nil {
		t.Fatal(err)
	}
	results, err := repo.Search(ctx, tc, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 4 {
		t.Fatalf("tombstoned row returned by search: %d", len(results))
	}
	if _, err := repo.Search(ctx, tc, "", 0); !errors.Is(err, storage.ErrInvalidArgument) {
		t.Fatalf("unbounded limit accepted: %v", err)
	}
	if _, err := repo.Search(ctx, tc, "", 101); !errors.Is(err, storage.ErrInvalidArgument) {
		t.Fatalf("over-limit accepted: %v", err)
	}
}

func TestSourceProjectorLoadsAuthoritativeMemory(t *testing.T) {
	bundle, repo, _, tc, projection := memoryIntegrationFixture(t)
	projector, err := vectortask.NewPostgresSourceProjector(bundle.pool, vectortask.SourceProjectorConfig{Model: projection.Model, ModelVersion: projection.ModelVersion, Dimension: projection.Dimension, SchemaVersion: projection.SchemaVersion})
	if err != nil {
		t.Fatal(err)
	}
	ctx := tenant.WithContext(context.Background(), tc)
	if err := repo.Put(ctx, tc, memoryValue("tenant-a", "mem-p", "projected content")); err != nil {
		t.Fatal(err)
	}
	var taskRow vectortask.Task
	if err := scanTaskRow(bundle.pool, ctx, "SELECT "+memoryTaskColumns+" FROM vector_projection_task WHERE tenant_id=$1 AND source_id=$2", "tenant-a", "mem-p", &taskRow); err != nil {
		t.Fatal(err)
	}
	source, err := projector.Load(ctx, tc, taskRow)
	if err != nil {
		t.Fatal(err)
	}
	if source.Content != "projected content" || source.Deleted || source.SourceVersion != 1 {
		t.Fatalf("unexpected source: %+v", source)
	}
	bogus := taskRow
	bogus.SourceType = "knowledge"
	if _, err := projector.Load(ctx, tc, bogus); !isVectorCategory(err) {
		t.Fatalf("unknown source type not rejected: %v", err)
	}
	stale := taskRow
	stale.SourceVersion = 99
	if _, err := projector.Load(ctx, tc, stale); err == nil {
		t.Fatal("stale version accepted")
	}
	if err := repo.Delete(ctx, tc, "mem-p"); err != nil {
		t.Fatal(err)
	}
	var deleteRow vectortask.Task
	if err := scanTaskRow(bundle.pool, ctx, "SELECT "+memoryTaskColumns+" FROM vector_projection_task WHERE tenant_id=$1 AND source_id=$2 AND operation='delete'", "tenant-a", "mem-p", &deleteRow); err != nil {
		t.Fatal(err)
	}
	deleteSource, err := projector.Load(ctx, tc, deleteRow)
	if err != nil {
		t.Fatal(err)
	}
	if !deleteSource.Deleted || deleteSource.Content != "" {
		t.Fatalf("delete source carries content: %+v", deleteSource)
	}
	if _, err := projector.Load(ctx, tc, taskRow); err == nil {
		t.Fatal("upsert task against tombstoned row accepted")
	}
}

// memoryTaskEnqueuer adapts the durable vector task repository to the narrow
// projection enqueue interface inside this test build.
type memoryTaskEnqueuer struct {
	tasks *vectortask.PostgresRepository
}

func (a memoryTaskEnqueuer) EnqueueTx(ctx context.Context, tx pgx.Tx, tc tenant.TenantContext, ref vector.VectorDocumentRef, now time.Time) (bool, error) {
	outcome, err := a.tasks.EnqueueTx(ctx, tx, tc, ref, now)
	if err != nil {
		return false, err
	}
	return outcome.Created, nil
}

func isVectorCategory(err error) bool {
	return err != nil && strings.Contains(err.Error(), "vector:")
}

func buildMemoryRef(ctx context.Context, value memory.Memory, projection MemoryProjectionConfig) (vector.VectorDocumentRef, error) {
	source := vector.SourceDocument{
		SourceType:      vector.SourceTypeMemory,
		SourceID:        value.ID,
		ProjectionScope: "memory:" + string(value.Scope),
		SourceVersion:   value.Version,
		SourceSequence:  value.SourceSeq,
		Content:         value.Content,
		Deleted:         value.Deleted,
		Model:           projection.Model,
		ModelVersion:    projection.ModelVersion,
		Dimension:       projection.Dimension,
		SchemaVersion:   projection.SchemaVersion,
	}
	return vector.BuildDocumentRef(ctx, source)
}

const memoryTaskColumns = `tenant_id, task_id, source_type, source_id, projection_scope,
	document_id, operation, source_version, source_sequence, content_hash,
	model, model_version, dimension, schema_version, status, attempt,
	max_attempts, next_attempt_at, last_error_category, lease_owner,
	lease_epoch, lease_fence, lease_expires_at, claimed_at, completed_at,
	dead_lettered_at, created_at, updated_at`

func scanTaskRow(pool *pgxpool.Pool, ctx context.Context, query string, tenantID, sourceID string, row *vectortask.Task) error {
	var lastError, leaseOwner *string
	var leaseEpoch, leaseFence *int64
	var leaseExpires, claimed, completed, deadLettered *time.Time
	var operation, status string
	err := pool.QueryRow(ctx, query, tenantID, sourceID).Scan(
		&row.TenantID, &row.TaskID, &row.SourceType, &row.SourceID, &row.ProjectionScope, &row.DocumentID, &operation, &row.SourceVersion, &row.SourceSequence, &row.ContentHash,
		&row.Model, &row.ModelVersion, &row.Dimension, &row.SchemaVersion, &status, &row.Attempt, &row.MaxAttempts, &row.NextAttemptAt, &lastError, &leaseOwner,
		&leaseEpoch, &leaseFence, &leaseExpires, &claimed, &completed, &deadLettered, &row.CreatedAt, &row.UpdatedAt)
	if err != nil {
		return err
	}
	row.Operation = vector.VectorOperation(operation)
	row.State = vectortask.State(status)
	if lastError != nil {
		row.LastError = *lastError
	}
	if leaseOwner != nil {
		row.LeaseOwner = *leaseOwner
	}
	if leaseEpoch != nil {
		row.LeaseEpoch = storage.Epoch(*leaseEpoch)
	}
	if leaseFence != nil {
		row.LeaseFence = uint64(*leaseFence)
	}
	if leaseExpires != nil {
		row.LeaseExpiresAt = *leaseExpires
	}
	if claimed != nil {
		row.ClaimedAt = *claimed
	}
	if completed != nil {
		row.CompletedAt = *completed
	}
	if deadLettered != nil {
		row.DeadLetteredAt = *deadLettered
	}
	return nil
}
