package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/internal/testinfra"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type outboxFixture struct {
	base   *pgxpool.Pool
	pool   *pgxpool.Pool
	repo   *OutboxRepository
	ctx    context.Context
	cancel context.CancelFunc
	tc     tenant.TenantContext
	schema string
}

func newOutboxFixture(t *testing.T, lockDuration time.Duration) *outboxFixture {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL is not set; PostgreSQL OutboxRepository not verified")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	cfg := PostgresConfig{URL: url, MaxConns: 8, MinConns: 1, AllowDestructiveDown: true}
	base, err := NewPool(ctx, cfg)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	schema := fmt.Sprintf("p009d_outbox_%d", time.Now().UnixNano())
	if _, err := base.Exec(ctx, "CREATE SCHEMA "+quoteSchema(schema)); err != nil {
		base.Close()
		cancel()
		t.Fatal(err)
	}
	cfg.SearchPath = schema
	pool, err := NewPool(ctx, cfg)
	if err != nil {
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+quoteSchema(schema)+" CASCADE")
		base.Close()
		cancel()
		t.Fatal(err)
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+quoteSchema(schema)+" CASCADE")
		base.Close()
		cancel()
		t.Fatal("cannot locate repository root")
	}
	migrator, err := NewMigratorWithPool(pool, cfg, os.DirFS(filepath.Join(filepath.Dir(file), "../../../migrations")))
	if err != nil {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+quoteSchema(schema)+" CASCADE")
		base.Close()
		cancel()
		t.Fatal(err)
	}
	if err := migrator.Up(ctx); err != nil {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+quoteSchema(schema)+" CASCADE")
		base.Close()
		cancel()
		t.Fatal(err)
	}
	repo, err := NewOutboxRepository(pool, OutboxRepositoryConfig{LockDuration: lockDuration, MaxBatchSize: 100})
	if err != nil {
		pool.Close()
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+quoteSchema(schema)+" CASCADE")
		base.Close()
		cancel()
		t.Fatal(err)
	}
	fixture := &outboxFixture{
		base: base, pool: pool, repo: repo, ctx: ctx, cancel: cancel, schema: schema,
		tc: outboxTenantContext("p009d-a"),
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = base.Exec(cleanupCtx, "DROP SCHEMA "+quoteSchema(schema)+" CASCADE")
		base.Close()
		cancel()
	})
	return fixture
}

func outboxTenantContext(id string) tenant.TenantContext {
	return tenant.TenantContext{
		TenantID: id, AgentAppID: "agent-a", BindingID: "binding-a", Channel: "web",
		RequestID: "request-a", MessageID: "message-a", TraceID: "trace-a", ConfigVersion: 1,
		BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "memory"},
	}
}

func enqueueOutbox(t *testing.T, fixture *outboxFixture, id, dedup string) storage.OutboxMessage {
	t.Helper()
	value := storage.OutboxMessage{
		TenantID: fixture.tc.TenantID, ID: id, Kind: "reply", AggregateID: "aggregate-" + id,
		DedupKey: dedup, Payload: []byte(`{"message":"hello","attempt":1}`),
	}
	if err := fixture.repo.Enqueue(fixture.ctx, fixture.tc, value); err != nil {
		t.Fatal(err)
	}
	return value
}

func alternatePool(t *testing.T, fixture *outboxFixture) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	pool, err := NewPool(fixture.ctx, PostgresConfig{URL: url, SearchPath: fixture.schema, MaxConns: 4, MinConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func TestPostgresOutboxEnqueueDedupAndPayload(t *testing.T) {
	fixture := newOutboxFixture(t, 2*time.Second)
	original := []byte(`{"message":"original","nested":{"n":1}}`)
	value := storage.OutboxMessage{
		TenantID: fixture.tc.TenantID, ID: "outbox-enqueue", Kind: "reply", AggregateID: "aggregate-a",
		DedupKey: "reply-message-a", Payload: original,
	}
	if err := fixture.repo.Enqueue(fixture.ctx, fixture.tc, value); err != nil {
		t.Fatal(err)
	}
	original[0] = 'x'
	stored, err := fixture.repo.Get(fixture.ctx, fixture.tc, value.ID)
	if err != nil {
		t.Fatal(err)
	}
	var expectedPayload, actualPayload any
	if err := json.Unmarshal([]byte(`{"message":"original","nested":{"n":1}}`), &expectedPayload); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(stored.Payload, &actualPayload); err != nil || !reflect.DeepEqual(actualPayload, expectedPayload) {
		t.Fatalf("payload was not copied and round-tripped: %s", stored.Payload)
	}
	if stored.Status != storage.OutboxPending || stored.Attempt != 1 || stored.DedupKey != value.DedupKey {
		t.Fatalf("unexpected durable enqueue state: %+v", stored)
	}
	value.Payload = []byte(`{"message":"original","nested":{"n":1}}`)
	if err := fixture.repo.Enqueue(fixture.ctx, fixture.tc, value); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("duplicate outbox ID error=%v", err)
	}
	duplicateDedup := value
	duplicateDedup.ID = "outbox-dedup-conflict"
	duplicateDedup.Payload = []byte(`{"message":"different"}`)
	if err := fixture.repo.Enqueue(fixture.ctx, fixture.tc, duplicateDedup); !errors.Is(err, storage.ErrDedupConflict) || !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("duplicate dedup key error=%v", err)
	}
	otherTenant := outboxTenantContext("p009d-b")
	other := value
	other.TenantID = otherTenant.TenantID
	other.ID = "outbox-enqueue"
	if err := fixture.repo.Enqueue(fixture.ctx, otherTenant, other); err != nil {
		t.Fatalf("tenant-scoped dedup rejected: %v", err)
	}
	invalid := value
	invalid.ID = "outbox-invalid"
	invalid.AggregateID = ""
	if err := fixture.repo.Enqueue(fixture.ctx, fixture.tc, invalid); !errors.Is(err, storage.ErrInvalidArgument) {
		t.Fatalf("invalid aggregate ID error=%v", err)
	}
	invalid = value
	invalid.ID = "outbox-invalid-json"
	invalid.Payload = []byte("not-json")
	if err := fixture.repo.Enqueue(fixture.ctx, fixture.tc, invalid); !errors.Is(err, storage.ErrInvalidArgument) {
		t.Fatalf("invalid JSON error=%v", err)
	}
}

func TestPostgresOutboxCompetingClaimAndExpiredReclaim(t *testing.T) {
	fixture := newOutboxFixture(t, 2*time.Second)
	const messageCount = 6
	for i := 0; i < messageCount; i++ {
		enqueueOutbox(t, fixture, fmt.Sprintf("outbox-claim-%d", i), fmt.Sprintf("claim-%d", i))
	}
	secondPool := alternatePool(t, fixture)
	secondRepo, err := NewOutboxRepository(secondPool, OutboxRepositoryConfig{LockDuration: 2 * time.Second, MaxBatchSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	type claimResult struct {
		owner string
		items []storage.OutboxMessage
		err   error
	}
	results := make(chan claimResult, 2)
	for _, owner := range []string{"worker-a", "worker-b"} {
		owner := owner
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			items, claimErr := fixture.repo.ClaimBatch(fixture.ctx, fixture.tc, owner, messageCount)
			results <- claimResult{owner: owner, items: items, err: claimErr}
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	claimed := make(map[string]string, messageCount)
	var all []storage.OutboxMessage
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		for _, item := range result.items {
			if previous, exists := claimed[item.ID]; exists {
				t.Fatalf("message claimed twice: %s by %s and %s", item.ID, previous, item.LockedBy)
			}
			claimed[item.ID] = item.LockedBy
			all = append(all, item)
		}
	}
	if len(all) != messageCount {
		t.Fatalf("competing claim count=%d, want %d", len(all), messageCount)
	}
	for id, owner := range claimed {
		if owner != "worker-a" && owner != "worker-b" {
			t.Fatalf("unexpected active owner for %s: %s", id, owner)
		}
		stored, getErr := fixture.repo.Get(fixture.ctx, fixture.tc, id)
		if getErr != nil || stored.Status != storage.OutboxProcessing || stored.LockedBy != owner {
			t.Fatalf("durable claim state for %s: err=%v value=%+v", id, getErr, stored)
		}
	}
	active, err := secondRepo.ClaimBatch(fixture.ctx, fixture.tc, "worker-c", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(active) != 0 {
		t.Fatalf("active lock was reclaimed early: %+v", active)
	}
	expiredID := all[0].ID
	before, err := fixture.repo.Get(fixture.ctx, fixture.tc, expiredID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.pool.Exec(fixture.ctx, `UPDATE outbox_message SET locked_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND outbox_id=$2`, fixture.tc.TenantID, expiredID); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := secondRepo.ClaimBatch(fixture.ctx, fixture.tc, "worker-c", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(reclaimed) != 1 || reclaimed[0].ID != expiredID || reclaimed[0].LockedBy != "worker-c" || reclaimed[0].Attempt != before.Attempt+1 {
		t.Fatalf("expired lock reclaim=%+v before=%+v", reclaimed, before)
	}
}

func TestPostgresOutboxLifecycleFencingRetryAndDLQ(t *testing.T) {
	fixture := newOutboxFixture(t, 2*time.Second)
	value := enqueueOutbox(t, fixture, "outbox-lifecycle", "lifecycle")
	claimed, err := fixture.repo.ClaimBatch(fixture.ctx, fixture.tc, "worker-a", 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("initial claim: err=%v items=%+v", err, claimed)
	}
	if _, err := fixture.pool.Exec(fixture.ctx, `UPDATE outbox_message SET locked_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND outbox_id=$2`, fixture.tc.TenantID, value.ID); err != nil {
		t.Fatal(err)
	}
	if err := fixture.repo.MarkCompleted(fixture.ctx, fixture.tc, "worker-a", value.ID); !errors.Is(err, storage.ErrOutboxLockExpired) {
		t.Fatalf("expired owner completion error=%v", err)
	}
	reclaimed, err := fixture.repo.ClaimBatch(fixture.ctx, fixture.tc, "worker-b", 1)
	if err != nil || len(reclaimed) != 1 {
		t.Fatalf("reclaim for fencing: err=%v items=%+v", err, reclaimed)
	}
	for name, call := range map[string]func() error{
		"complete": func() error { return fixture.repo.MarkCompleted(fixture.ctx, fixture.tc, "worker-a", value.ID) },
		"retry": func() error {
			return fixture.repo.MarkRetry(fixture.ctx, fixture.tc, "worker-a", value.ID, time.Now().UTC().Add(time.Hour), "provider_timeout")
		},
		"dlq": func() error {
			return fixture.repo.MoveToDLQ(fixture.ctx, fixture.tc, "worker-a", value.ID, "provider_permanent")
		},
	} {
		err := call()
		if !errors.Is(err, storage.ErrOutboxLockLost) {
			t.Fatalf("stale %s error=%v", name, err)
		}
	}
	future := time.Now().UTC().Add(time.Hour)
	if err := fixture.repo.MarkRetry(fixture.ctx, fixture.tc, "worker-b", value.ID, future, "provider_timeout"); err != nil {
		t.Fatal(err)
	}
	stored, err := fixture.repo.Get(fixture.ctx, fixture.tc, value.ID)
	if err != nil || stored.Status != storage.OutboxRetry || stored.Attempt != 3 || stored.LastError != "provider_timeout" {
		t.Fatalf("retry state: err=%v value=%+v", err, stored)
	}
	if items, err := fixture.repo.ClaimBatch(fixture.ctx, fixture.tc, "worker-c", 1); err != nil || len(items) != 0 {
		t.Fatalf("future retry claimed early: err=%v items=%+v", err, items)
	}
	if _, err := fixture.pool.Exec(fixture.ctx, `UPDATE outbox_message SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND outbox_id=$2`, fixture.tc.TenantID, value.ID); err != nil {
		t.Fatal(err)
	}
	claimedAgain, err := fixture.repo.ClaimBatch(fixture.ctx, fixture.tc, "worker-c", 1)
	if err != nil || len(claimedAgain) != 1 || claimedAgain[0].Attempt != 3 {
		t.Fatalf("due retry claim: err=%v items=%+v", err, claimedAgain)
	}
	if err := fixture.repo.MarkCompleted(fixture.ctx, fixture.tc, "worker-c", value.ID); err != nil {
		t.Fatal(err)
	}
	if err := fixture.repo.MarkCompleted(fixture.ctx, fixture.tc, "worker-c", value.ID); !errors.Is(err, storage.ErrAlreadyCompleted) {
		t.Fatalf("duplicate completion error=%v", err)
	}

	dlqValue := enqueueOutbox(t, fixture, "outbox-dlq", "dlq")
	if _, err := fixture.repo.ClaimBatch(fixture.ctx, fixture.tc, "worker-d", 1); err != nil {
		t.Fatal(err)
	}
	if err := fixture.repo.MoveToDLQ(fixture.ctx, fixture.tc, "worker-d", dlqValue.ID, "provider_permanent"); err != nil {
		t.Fatal(err)
	}
	dead, err := fixture.repo.Get(fixture.ctx, fixture.tc, dlqValue.ID)
	if err != nil || dead.Status != storage.OutboxDead || dead.LockedBy != "" || dead.LastError != "provider_permanent" {
		t.Fatalf("dead outbox state: err=%v value=%+v", err, dead)
	}
	var deadCount int
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT count(*) FROM dead_letter WHERE tenant_id=$1 AND outbox_id=$2`, fixture.tc.TenantID, dlqValue.ID).Scan(&deadCount); err != nil {
		t.Fatal(err)
	}
	if deadCount != 1 {
		t.Fatalf("dead letter count=%d", deadCount)
	}
	if err := fixture.repo.MoveToDLQ(fixture.ctx, fixture.tc, "worker-d", dlqValue.ID, "provider_permanent"); !errors.Is(err, storage.ErrAlreadyDead) {
		t.Fatalf("duplicate DLQ error=%v", err)
	}
}

func TestPostgresOutboxTransactionsTenantIsolationAndUnavailable(t *testing.T) {
	fixture := newOutboxFixture(t, 2*time.Second)
	failedEnqueue := storage.OutboxMessage{TenantID: fixture.tc.TenantID, ID: "outbox-rollback-enqueue", Kind: "reply", AggregateID: "aggregate-rollback-enqueue", Payload: []byte(`{"rollback":true}`)}
	fixture.repo.beforeCommit = func() error { return errors.New("injected enqueue commit failure") }
	if err := fixture.repo.Enqueue(fixture.ctx, fixture.tc, failedEnqueue); err == nil {
		t.Fatal("enqueue failure unexpectedly succeeded")
	}
	fixture.repo.beforeCommit = nil
	if _, err := fixture.repo.Get(fixture.ctx, fixture.tc, failedEnqueue.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("failed enqueue left a durable row: %v", err)
	}
	completeValue := enqueueOutbox(t, fixture, "outbox-rollback-complete", "rollback-complete")
	if _, err := fixture.repo.ClaimBatch(fixture.ctx, fixture.tc, "worker-a", 1); err != nil {
		t.Fatal(err)
	}
	fixture.repo.beforeCommit = func() error { return errors.New("injected commit failure") }
	if err := fixture.repo.MarkCompleted(fixture.ctx, fixture.tc, "worker-a", completeValue.ID); err == nil {
		t.Fatal("completion failure unexpectedly succeeded")
	}
	fixture.repo.beforeCommit = nil
	stored, err := fixture.repo.Get(fixture.ctx, fixture.tc, completeValue.ID)
	if err != nil || stored.Status != storage.OutboxProcessing || stored.LockedBy != "worker-a" {
		t.Fatalf("completion rollback state: err=%v value=%+v", err, stored)
	}
	if err := fixture.repo.MarkCompleted(fixture.ctx, fixture.tc, "worker-a", completeValue.ID); err != nil {
		t.Fatal(err)
	}

	dlqValue := enqueueOutbox(t, fixture, "outbox-rollback-dlq", "rollback-dlq")
	if _, err := fixture.repo.ClaimBatch(fixture.ctx, fixture.tc, "worker-b", 1); err != nil {
		t.Fatal(err)
	}
	fixture.repo.afterDLQInsert = func() error { return errors.New("injected DLQ insert failure") }
	if err := fixture.repo.MoveToDLQ(fixture.ctx, fixture.tc, "worker-b", dlqValue.ID, "provider_permanent"); err == nil {
		t.Fatal("DLQ failure unexpectedly succeeded")
	}
	fixture.repo.afterDLQInsert = nil
	stored, err = fixture.repo.Get(fixture.ctx, fixture.tc, dlqValue.ID)
	if err != nil || stored.Status != storage.OutboxProcessing {
		t.Fatalf("DLQ rollback outbox state: err=%v value=%+v", err, stored)
	}
	var deadCount int
	if err := fixture.pool.QueryRow(fixture.ctx, `SELECT count(*) FROM dead_letter WHERE tenant_id=$1 AND outbox_id=$2`, fixture.tc.TenantID, dlqValue.ID).Scan(&deadCount); err != nil {
		t.Fatal(err)
	}
	if deadCount != 0 {
		t.Fatalf("DLQ rollback left dead letter rows=%d", deadCount)
	}
	if err := fixture.repo.MoveToDLQ(fixture.ctx, fixture.tc, "worker-b", dlqValue.ID, "provider_permanent"); err != nil {
		t.Fatal(err)
	}

	otherTenant := outboxTenantContext("p009d-other")
	if _, err := fixture.repo.Get(fixture.ctx, otherTenant, completeValue.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("cross-tenant read error=%v", err)
	}
	if err := fixture.repo.MarkCompleted(fixture.ctx, otherTenant, "worker-a", completeValue.ID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("cross-tenant mutation error=%v", err)
	}
	unsafe := enqueueOutbox(t, fixture, "outbox-safe-code", "safe-code")
	if _, err := fixture.repo.ClaimBatch(fixture.ctx, fixture.tc, "worker-c", 1); err != nil {
		t.Fatal(err)
	}
	if err := fixture.repo.MarkRetry(fixture.ctx, fixture.tc, "worker-c", unsafe.ID, time.Now().UTC().Add(time.Minute), "provider timeout"); !errors.Is(err, storage.ErrUnsafeFailureCode) {
		t.Fatalf("unsafe failure code error=%v", err)
	}

	closedPool := alternatePool(t, fixture)
	closedRepo, err := NewOutboxRepository(closedPool, OutboxRepositoryConfig{})
	if err != nil {
		t.Fatal(err)
	}
	closedPool.Close()
	if err := closedRepo.Enqueue(fixture.ctx, fixture.tc, storage.OutboxMessage{TenantID: fixture.tc.TenantID, ID: "outbox-unavailable", Kind: "reply", AggregateID: "aggregate-unavailable"}); !errors.Is(err, storage.ErrBackendUnavailable) {
		t.Fatalf("closed PostgreSQL pool error=%v", err)
	} else if strings.Contains(err.Error(), "postgres://") || strings.Contains(err.Error(), "123456") {
		t.Fatalf("backend error leaked connection details: %v", err)
	}
}

func TestPostgresOutboxContractEquivalentToFake(t *testing.T) {
	fixture := newOutboxFixture(t, 2*time.Second)
	fake := storage.NewFakeRepository()
	implementations := map[string]storage.OutboxRepository{
		"fake":       fake,
		"postgresql": fixture.repo,
	}
	for name, repository := range implementations {
		name, repository := name, repository
		t.Run(name, func(t *testing.T) {
			message := storage.OutboxMessage{
				TenantID: fixture.tc.TenantID, ID: "outbox-contract-" + name,
				Kind: "reply", AggregateID: "aggregate-contract-" + name,
				DedupKey: "contract-dedup-" + name, Payload: []byte(`{"contract":true}`),
			}
			if err := repository.Enqueue(fixture.ctx, fixture.tc, message); err != nil {
				t.Fatal(err)
			}
			duplicate := message
			duplicate.ID += "-duplicate"
			if err := repository.Enqueue(fixture.ctx, fixture.tc, duplicate); !errors.Is(err, storage.ErrDedupConflict) || !errors.Is(err, storage.ErrConflict) {
				t.Fatalf("duplicate dedup error=%v", err)
			}
			claimed, err := repository.ClaimBatch(fixture.ctx, fixture.tc, "contract-owner-a", 1)
			if err != nil || len(claimed) != 1 {
				t.Fatalf("claim: err=%v items=%+v", err, claimed)
			}
			if err := repository.MarkCompleted(fixture.ctx, fixture.tc, "contract-owner-b", message.ID); !errors.Is(err, storage.ErrOutboxLockLost) {
				t.Fatalf("stale owner error=%v", err)
			}
			if err := repository.MarkRetry(fixture.ctx, fixture.tc, "contract-owner-a", message.ID, time.Now().UTC().Add(-time.Minute), "provider_timeout"); err != nil {
				t.Fatal(err)
			}
			ready, err := repository.ClaimBatch(fixture.ctx, fixture.tc, "contract-owner-c", 1)
			if err != nil || len(ready) != 1 || ready[0].Status != storage.OutboxProcessing {
				t.Fatalf("retry claim: err=%v items=%+v", err, ready)
			}
			if err := repository.MarkCompleted(fixture.ctx, fixture.tc, "contract-owner-c", message.ID); err != nil {
				t.Fatal(err)
			}
			if err := repository.MarkCompleted(fixture.ctx, fixture.tc, "contract-owner-c", message.ID); !errors.Is(err, storage.ErrAlreadyCompleted) {
				t.Fatalf("duplicate completion error=%v", err)
			}
			if err := repository.MarkRetry(fixture.ctx, fixture.tc, "contract-owner-c", message.ID, time.Now().UTC(), "provider timeout"); !errors.Is(err, storage.ErrUnsafeFailureCode) {
				t.Fatalf("unsafe code error=%v", err)
			}
		})
	}
}

func TestPostgresOutboxMigrationStateConstraints(t *testing.T) {
	fixture := newOutboxFixture(t, 2*time.Second)
	_, err := fixture.pool.Exec(fixture.ctx, `
INSERT INTO outbox_message (tenant_id, outbox_id, kind, aggregate_id, payload, status, attempt, next_attempt_at)
VALUES ($1, 'outbox-invalid-lock-state', 'reply', 'aggregate-invalid', '{}'::jsonb, 'processing', 1, clock_timestamp())`, fixture.tc.TenantID)
	if err == nil {
		t.Fatal("processing row without lock fields was accepted")
	}
}

func TestPostgresOutboxContextCancellation(t *testing.T) {
	fixture := newOutboxFixture(t, 2*time.Second)
	ctx, cancel := context.WithCancel(fixture.ctx)
	cancel()
	if _, err := fixture.repo.ClaimBatch(ctx, fixture.tc, "worker-a", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled claim error=%v", err)
	}
}

func TestPostgresOutboxDockerRestartRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	lab := testinfra.NewDockerLab(t)
	lab.Start(ctx)
	lab.WaitHealthy(ctx)
	cfg := PostgresConfig{URL: lab.PostgresURL(ctx), MaxConns: 8, MinConns: 1, AllowDestructiveDown: true}
	base, err := NewPool(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("p009d_docker_%d", time.Now().UnixNano())
	if _, err := base.Exec(ctx, "CREATE SCHEMA "+quoteSchema(schema)); err != nil {
		base.Close()
		t.Fatal(err)
	}
	cfg.SearchPath = schema
	pool, err := NewPool(ctx, cfg)
	if err != nil {
		_, _ = base.Exec(context.Background(), "DROP SCHEMA "+quoteSchema(schema)+" CASCADE")
		base.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = base.Exec(cleanupCtx, "DROP SCHEMA "+quoteSchema(schema)+" CASCADE")
		base.Close()
	})
	_, file, _, _ := runtime.Caller(0)
	source := os.DirFS(filepath.Join(filepath.Dir(file), "../../../migrations"))
	migrator, err := NewMigratorWithPool(pool, cfg, source)
	if err != nil {
		t.Fatal(err)
	}
	if err := migrator.Up(ctx); err != nil {
		t.Fatal(err)
	}
	tc := outboxTenantContext("p009d-docker")
	repo, err := NewOutboxRepository(pool, OutboxRepositoryConfig{LockDuration: 2 * time.Second, MaxBatchSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	value := storage.OutboxMessage{TenantID: tc.TenantID, ID: "outbox-docker-restart", Kind: "reply", AggregateID: "aggregate-docker", Payload: []byte(`{"restart":true}`)}
	if err := repo.Enqueue(ctx, tc, value); err != nil {
		t.Fatal(err)
	}
	claimed, err := repo.ClaimBatch(ctx, tc, "docker-old", 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Docker initial claim: err=%v items=%+v", err, claimed)
	}
	lab.StopPostgres(ctx)
	failureCtx, failureCancel := context.WithTimeout(ctx, 3*time.Second)
	err = repo.MarkRetry(failureCtx, tc, "docker-old", value.ID, time.Now().UTC().Add(time.Minute), "backend_unavailable")
	failureCancel()
	if err == nil {
		t.Fatal("repository operation succeeded while Docker PostgreSQL was stopped")
	}
	lab.RestartPostgres(ctx)
	lab.WaitHealthy(ctx)
	recoveredCfg := cfg
	recoveredCfg.URL = lab.PostgresURL(ctx)
	recoveredPool, err := newDockerPool(ctx, recoveredCfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(recoveredPool.Close)
	recoveredRepo, err := NewOutboxRepository(recoveredPool, OutboxRepositoryConfig{LockDuration: 2 * time.Second, MaxBatchSize: 100})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recoveredPool.Exec(ctx, `UPDATE outbox_message SET locked_until=clock_timestamp()-interval '1 second' WHERE tenant_id=$1 AND outbox_id=$2`, tc.TenantID, value.ID); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := recoveredRepo.ClaimBatch(ctx, tc, "docker-new", 1)
	if err != nil || len(reclaimed) != 1 || reclaimed[0].LockedBy != "docker-new" || reclaimed[0].Attempt != claimed[0].Attempt+1 {
		t.Fatalf("Docker recovered claim: err=%v items=%+v initial=%+v", err, reclaimed, claimed)
	}
	if err := recoveredRepo.MarkCompleted(ctx, tc, "docker-old", value.ID); !errors.Is(err, storage.ErrOutboxLockLost) {
		t.Fatalf("Docker stale owner error=%v", err)
	}
	if err := recoveredRepo.MarkCompleted(ctx, tc, "docker-new", value.ID); err != nil {
		t.Fatal(err)
	}
}
