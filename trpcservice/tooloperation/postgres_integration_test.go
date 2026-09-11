package tooloperation

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
)

const toolOperationPostgresDSNEnv = "TEST_POSTGRES_DSN"

func newPostgresIntegrationLedger(t *testing.T) *Postgres {
	t.Helper()
	dsn := os.Getenv(toolOperationPostgresDSNEnv)
	if dsn == "" {
		t.Skip(toolOperationPostgresDSNEnv + " is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL admin pool: %v", err)
	}
	schema := "tooloperation_it_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quotedSchema); err != nil {
		admin.Close()
		t.Fatalf("create PostgreSQL integration schema: %v", err)
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+quotedSchema+" CASCADE")
		admin.Close()
		t.Fatalf("parse PostgreSQL DSN: %v", err)
	}
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = make(map[string]string)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+quotedSchema+" CASCADE")
		admin.Close()
		t.Fatalf("open PostgreSQL schema pool: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := admin.Exec(cleanupCtx, "DROP SCHEMA "+quotedSchema+" CASCADE"); err != nil {
			t.Errorf("drop PostgreSQL integration schema: %v", err)
		}
		admin.Close()
	})
	if _, err := pool.Exec(ctx, PostgreSQLSchemaDDL); err != nil {
		t.Fatalf("install tool operation schema: %v", err)
	}
	ledger, err := NewPostgres(pool)
	if err != nil {
		t.Fatal(err)
	}
	return ledger
}

func TestPostgresIntegrationExactLeaseConcurrencyFencingAndPrivacy(t *testing.T) {
	ledger := newPostgresIntegrationLedger(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now().UTC()
	requests := []ReserveRequest{
		testReserve("tenant-a", "target-operation", "raw-secret-target-payload"),
		testReserve("tenant-a", "other-operation", "raw-secret-other-payload"),
		testReserve("tenant-b", "target-operation", "raw-secret-tenant-b-payload"),
	}
	for _, req := range requests {
		if _, err := ledger.Reserve(ctx, req, now); err != nil {
			t.Fatal(err)
		}
	}
	duplicate, err := ledger.Reserve(ctx, requests[0], now.Add(time.Second))
	if err != nil || !duplicate.Existing {
		t.Fatalf("idempotent Reserve = %+v, error %v", duplicate, err)
	}
	conflict := requests[0]
	conflict.PayloadHash = HashPayload([]byte("different-secret"))
	if _, err := ledger.Reserve(ctx, conflict, now); !errors.Is(err, ErrConflict) {
		t.Fatalf("Reserve conflict error = %v", err)
	}

	type leaseResult struct {
		leased *LeasedOperation
		err    error
	}
	const contenders = 16
	start := make(chan struct{})
	results := make(chan leaseResult, contenders)
	var wg sync.WaitGroup
	for index := 0; index < contenders; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			leased, err := ledger.LeaseOperation(
				ctx, "tenant-a", "target-operation",
				"postgres-owner-"+strings.Repeat("x", index+1), now, time.Minute,
			)
			results <- leaseResult{leased: leased, err: err}
		}(index)
	}
	close(start)
	wg.Wait()
	close(results)
	var winner *LeasedOperation
	for result := range results {
		if result.err == nil {
			if winner != nil {
				t.Fatal("more than one exact-lease contender succeeded")
			}
			winner = result.leased
			continue
		}
		if !errors.Is(result.err, ErrInvalidTransition) {
			t.Fatalf("losing exact lease error = %v", result.err)
		}
	}
	if winner == nil || winner.Record.TenantID != "tenant-a" || winner.Record.OperationKey != "target-operation" {
		t.Fatalf("exact lease winner = %+v", winner)
	}
	other, err := ledger.Get(ctx, "tenant-a", "other-operation")
	if err != nil {
		t.Fatal(err)
	}
	if other.AttemptCount != 0 || !other.LeaseExpiresAt.IsZero() {
		t.Fatalf("exact lease substituted another due operation: %+v", other)
	}
	if _, err := ledger.Get(ctx, "tenant-c", "target-operation"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant Get error = %v", err)
	}

	tenantB, err := ledger.LeaseOperation(
		ctx, "tenant-b", "target-operation", "tenant-b-owner-secret", now, time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if tenantB.Fence.AttemptNo != 1 || tenantB.Record.TenantID != "tenant-b" {
		t.Fatalf("same-key other-tenant lease = %+v", tenantB)
	}
	wrongOwner := tenantB.Fence
	wrongOwner.Owner = "wrong-owner-secret"
	if err := ledger.Renew(ctx, wrongOwner, now, time.Minute); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("wrong-owner Renew error = %v", err)
	}
	if err := ledger.Renew(ctx, tenantB.Fence, now, time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.MarkExecuting(ctx, tenantB.Fence, now); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.MarkExecuting(ctx, tenantB.Fence, now.Add(time.Second)); err != nil {
		t.Fatalf("idempotent MarkExecuting: %v", err)
	}
	finish := FinishRequest{
		Fence: tenantB.Fence, Outcome: StateConfirmed, OutcomeCode: "created",
		ResultHash:      HashPayload([]byte("raw-provider-response-never-stored")),
		ReplayReference: "protected-result/ref-1",
	}
	if _, err := ledger.Finish(ctx, finish, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Finish(ctx, finish, now.Add(3*time.Second)); err != nil {
		t.Fatalf("idempotent Finish: %v", err)
	}
	replay, err := ledger.Reserve(ctx, requests[2], now.Add(4*time.Second))
	if err != nil || !replay.Existing || !replay.Replayed || replay.Confirmation == nil {
		t.Fatalf("confirmed Reserve replay = %+v, error %v", replay, err)
	}
	attempt, err := ledger.GetAttempt(ctx, "tenant-b", "target-operation", 1)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.OwnerHash != ownerHash("tenant-b-owner-secret") ||
		attempt.OwnerHash == "tenant-b-owner-secret" || attempt.Phase != AttemptFinished {
		t.Fatalf("sanitized attempt = %+v", attempt)
	}
	var operationJSON, attemptJSON string
	if err := ledger.pool.QueryRow(ctx, `
		SELECT row_to_json(operation_row)::text
		FROM tool_operations AS operation_row
		WHERE tenant_id = $1 AND operation_key = $2`,
		"tenant-b", "target-operation").Scan(&operationJSON); err != nil {
		t.Fatal(err)
	}
	if err := ledger.pool.QueryRow(ctx, `
		SELECT row_to_json(attempt_row)::text
		FROM tool_operation_attempts AS attempt_row
		WHERE tenant_id = $1 AND operation_key = $2 AND attempt_no = 1`,
		"tenant-b", "target-operation").Scan(&attemptJSON); err != nil {
		t.Fatal(err)
	}
	for _, raw := range []string{
		"raw-secret-tenant-b-payload", "tenant-b-owner-secret",
		"raw-provider-response-never-stored",
	} {
		if strings.Contains(operationJSON, raw) || strings.Contains(attemptJSON, raw) {
			t.Fatalf("PostgreSQL ledger persisted raw value %q", raw)
		}
	}

	if _, err := ledger.pool.Exec(ctx, `
		UPDATE tool_operations
		SET lease_expires_at = clock_timestamp() - interval '1 second'
		WHERE tenant_id = $1 AND operation_key = $2`,
		"tenant-a", "target-operation"); err != nil {
		t.Fatal(err)
	}
	replacement, err := ledger.LeaseOperation(
		ctx, "tenant-a", "target-operation", "replacement-owner", now, time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.Fence.AttemptNo != 2 {
		t.Fatalf("replacement exact lease attempt = %d, want 2", replacement.Fence.AttemptNo)
	}
	if _, err := ledger.MarkExecuting(ctx, winner.Fence, now); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale exact lease fence error = %v", err)
	}
	expiredAttempt, err := ledger.GetAttempt(ctx, "tenant-a", "target-operation", 1)
	if err != nil {
		t.Fatal(err)
	}
	if expiredAttempt.Outcome != StateRetryableNotApplied ||
		expiredAttempt.OutcomeCode != leaseExpiredBeforeExec {
		t.Fatalf("expired exact lease attempt = %+v", expiredAttempt)
	}
}

func TestPostgresIntegrationBatchLeaseDoesNotReclaimOtherTenantExecuting(t *testing.T) {
	ledger := newPostgresIntegrationLedger(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now().UTC()
	if _, err := ledger.Reserve(ctx, testReserve("tenant-a", "batch-due", "payload-a"), now); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Reserve(ctx, testReserve("tenant-b", "executing-expired", "payload-b"), now); err != nil {
		t.Fatal(err)
	}
	executing, err := ledger.LeaseOperation(
		ctx, "tenant-b", "executing-expired", "executing-owner", now, time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.MarkExecuting(ctx, executing.Fence, now); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.pool.Exec(ctx, `
		UPDATE tool_operations
		SET lease_expires_at = clock_timestamp() - interval '1 second'
		WHERE tenant_id = $1 AND operation_key = $2`,
		"tenant-b", "executing-expired"); err != nil {
		t.Fatal(err)
	}
	leased, err := ledger.Lease(ctx, LeaseRequest{
		TenantID: "tenant-a", Owner: "batch-owner", Now: now,
		TTL: time.Minute, Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(leased) != 1 || leased[0].Record.OperationKey != "batch-due" {
		t.Fatalf("tenant-a batch lease = %+v", leased)
	}
	stillExecuting, err := ledger.Get(ctx, "tenant-b", "executing-expired")
	if err != nil {
		t.Fatal(err)
	}
	if stillExecuting.State != StateExecuting {
		t.Fatalf("batch Lease mutated another tenant: %+v", stillExecuting)
	}
	reclaimed, err := ledger.ReclaimExpired(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if reclaimed.Unknown != 1 || reclaimed.Retryable != 0 {
		t.Fatalf("explicit ReclaimExpired = %+v", reclaimed)
	}
	unknown, err := ledger.ListUnknown(ctx, "tenant-b", 10)
	if err != nil || len(unknown) != 1 || unknown[0].OperationKey != "executing-expired" {
		t.Fatalf("tenant-b unknown list = %+v, error %v", unknown, err)
	}
	if listed, err := ledger.ListUnknown(ctx, "tenant-a", 10); err != nil || len(listed) != 0 {
		t.Fatalf("tenant-a unknown list = %+v, error %v", listed, err)
	}
}

func TestPostgresIntegrationResolveUnknownConcurrentIdempotencyAndTenantIsolation(t *testing.T) {
	ledger := newPostgresIntegrationLedger(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now().UTC()
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		if _, err := ledger.Reserve(ctx, testReserve(tenantID, "unknown-operation", tenantID+"-payload"), now); err != nil {
			t.Fatal(err)
		}
		leased, err := ledger.LeaseOperation(
			ctx, tenantID, "unknown-operation", tenantID+"-owner", now, time.Minute,
		)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ledger.MarkExecuting(ctx, leased.Fence, now); err != nil {
			t.Fatal(err)
		}
		if _, err := ledger.pool.Exec(ctx, `
			UPDATE tool_operations
			SET lease_expires_at = clock_timestamp() - interval '1 second'
			WHERE tenant_id = $1 AND operation_key = $2`,
			tenantID, "unknown-operation"); err != nil {
			t.Fatal(err)
		}
	}
	reclaimed, err := ledger.ReclaimExpired(ctx, now, 10)
	if err != nil {
		t.Fatal(err)
	}
	if reclaimed.Unknown != 2 {
		t.Fatalf("unknown reclaim = %+v", reclaimed)
	}
	unknownA, err := ledger.Get(ctx, "tenant-a", "unknown-operation")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.LeaseOperation(
		ctx, "tenant-a", "unknown-operation", "automatic-retry-owner", now, time.Minute,
	); !errors.Is(err, ErrUnknownRequiresResolution) {
		t.Fatalf("unknown exact lease error = %v", err)
	}

	resolutionNow := time.Now().UTC()
	resolution := ResolveRequest{
		ResolutionID: "shared-resolution", TenantID: "tenant-a",
		OperationKey: "unknown-operation", ExpectedVersion: unknownA.StateVersion,
		Action: ResolveConfirm, ActorHash: HashPayload([]byte("operator-a")),
		ReasonCode: "provider_confirmed", ResultHash: HashPayload([]byte("protected-result")),
		ReplayReference: "protected-result/ref-a",
	}
	const contenders = 12
	start := make(chan struct{})
	errs := make(chan error, contenders)
	var wg sync.WaitGroup
	for range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := ledger.ResolveUnknown(ctx, resolution, resolutionNow)
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent idempotent ResolveUnknown: %v", err)
		}
	}
	changed := resolution
	changed.ReasonCode = "different_reason"
	if _, err := ledger.ResolveUnknown(ctx, changed, resolutionNow); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed resolution replay error = %v", err)
	}
	confirmed, err := ledger.Get(ctx, "tenant-a", "unknown-operation")
	if err != nil || confirmed.State != StateConfirmed || confirmed.Confirmation == nil {
		t.Fatalf("resolved tenant-a operation = %+v, error %v", confirmed, err)
	}
	unknownB, err := ledger.Get(ctx, "tenant-b", "unknown-operation")
	if err != nil || unknownB.State != StateUnknown {
		t.Fatalf("tenant-b operation changed with tenant-a resolution: %+v, error %v", unknownB, err)
	}
	if _, err := ledger.ResolveUnknown(ctx, ResolveRequest{
		ResolutionID: "shared-resolution", TenantID: "tenant-b",
		OperationKey: "unknown-operation", ExpectedVersion: unknownB.StateVersion,
		Action: ResolveReject, ActorHash: HashPayload([]byte("operator-b")),
		ReasonCode: "operator_rejected",
	}, resolutionNow); err != nil {
		t.Fatalf("same resolution ID in another tenant: %v", err)
	}
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		if listed, err := ledger.ListUnknown(ctx, tenantID, 10); err != nil || len(listed) != 0 {
			t.Fatalf("remaining unknown for %s = %+v, error %v", tenantID, listed, err)
		}
	}
}

func TestPostgresIntegrationRetryableDueAndFinishIdempotency(t *testing.T) {
	ledger := newPostgresIntegrationLedger(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	now := time.Now().UTC()
	if _, err := ledger.Reserve(ctx, testReserve("tenant-a", "retry-operation", "payload"), now); err != nil {
		t.Fatal(err)
	}
	first, err := ledger.LeaseOperation(
		ctx, "tenant-a", "retry-operation", "retry-owner-1", now, time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	retryAt := now.Add(time.Hour)
	finish := FinishRequest{
		Fence: first.Fence, Outcome: StateRetryableNotApplied,
		OutcomeCode: "proved_not_applied", RetryAt: retryAt,
	}
	if _, err := ledger.Finish(ctx, finish, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Finish(ctx, finish, now.Add(2*time.Second)); err != nil {
		t.Fatalf("idempotent retryable Finish: %v", err)
	}
	if _, err := ledger.LeaseOperation(
		ctx, "tenant-a", "retry-operation", "retry-owner-2", retryAt, time.Minute,
	); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("database-authoritative not-due lease error = %v", err)
	}
	if _, err := ledger.pool.Exec(ctx, `
		UPDATE tool_operations SET next_attempt_at = clock_timestamp()
		WHERE tenant_id = $1 AND operation_key = $2`,
		"tenant-a", "retry-operation"); err != nil {
		t.Fatal(err)
	}
	second, err := ledger.LeaseOperation(
		ctx, "tenant-a", "retry-operation", "retry-owner-2", retryAt, time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if second.Fence.AttemptNo != 2 {
		t.Fatalf("retry attempt = %d, want 2", second.Fence.AttemptNo)
	}
	if _, err := ledger.MarkExecuting(ctx, first.Fence, now); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("finished stale fence error = %v", err)
	}
	attempt, err := ledger.GetAttempt(ctx, "tenant-a", "retry-operation", 1)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Phase != AttemptFinished || attempt.Outcome != StateRetryableNotApplied ||
		attempt.OutcomeCode != "proved_not_applied" {
		t.Fatalf("retryable attempt = %+v", attempt)
	}
}
