package tooloperation

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDeriveOperationKeyStableAndTenantScoped(t *testing.T) {
	first, err := DeriveOperationKey("tenant-a", "turn-7", "call-2", "calendar.create")
	if err != nil {
		t.Fatal(err)
	}
	again, err := DeriveOperationKey("tenant-a", "turn-7", "call-2", "calendar.create")
	if err != nil {
		t.Fatal(err)
	}
	if first != again || !strings.HasPrefix(first, "toolop_") {
		t.Fatalf("stable keys differ: %q %q", first, again)
	}
	otherTenant, err := DeriveOperationKey("tenant-b", "turn-7", "call-2", "calendar.create")
	if err != nil {
		t.Fatal(err)
	}
	otherCall, err := DeriveOperationKey("tenant-a", "turn-7", "call-3", "calendar.create")
	if err != nil {
		t.Fatal(err)
	}
	if first == otherTenant || first == otherCall {
		t.Fatal("tenant and call identity must be part of the operation key")
	}
	for _, raw := range []string{"tenant-a", "turn-7", "call-2", "calendar.create"} {
		if strings.Contains(first, raw) {
			t.Fatalf("operation key leaked raw input %q", raw)
		}
	}
	if _, err := DeriveOperationKey("tenant-a", "turn", "call", "unsafe tool"); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("invalid tool name error = %v", err)
	}
}

func TestMemoryConfirmedReplayAndPayloadConflict(t *testing.T) {
	ctx := context.Background()
	ledger := NewMemory()
	now := time.Date(2026, 8, 31, 10, 0, 0, 0, time.UTC)
	req := testReserve("tenant-a", "operation-1", "fixture-sensitive-value")

	created, err := ledger.Reserve(ctx, req, now)
	if err != nil {
		t.Fatal(err)
	}
	if created.Existing || created.Replayed || created.Record.State != StateReserved {
		t.Fatalf("created = %+v", created)
	}
	existing, err := ledger.Reserve(ctx, req, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !existing.Existing || existing.Replayed {
		t.Fatalf("unconfirmed duplicate = %+v", existing)
	}

	conflict := req
	conflict.PayloadHash = HashPayload([]byte("different-sensitive-payload"))
	if _, err := ledger.Reserve(ctx, conflict, now); !errors.Is(err, ErrConflict) {
		t.Fatalf("payload conflict error = %v", err)
	}
	conflict = req
	conflict.Metadata.OperationClass = "calendar.delete"
	if _, err := ledger.Reserve(ctx, conflict, now); !errors.Is(err, ErrConflict) {
		t.Fatalf("metadata conflict error = %v", err)
	}

	leased := mustLease(t, ledger, "tenant-a", "worker-capability-secret", now, time.Minute, 1)
	if _, err := ledger.MarkExecuting(ctx, leased.Fence, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	finish := FinishRequest{
		Fence: leased.Fence, Outcome: StateConfirmed, OutcomeCode: "created",
		ResultHash:      HashPayload([]byte(`{"event_id":"sensitive-result"}`)),
		ReplayReference: "encrypted-result/ref-1",
	}
	confirmed, err := ledger.Finish(ctx, finish, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if confirmed.State != StateConfirmed || confirmed.Confirmation == nil {
		t.Fatalf("confirmed = %+v", confirmed)
	}
	// A retry after an ambiguous database acknowledgement is idempotent.
	if _, err := ledger.Finish(ctx, finish, now.Add(3*time.Second)); err != nil {
		t.Fatalf("idempotent finish: %v", err)
	}

	replay, err := ledger.Reserve(ctx, req, now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Existing || !replay.Replayed || replay.Confirmation == nil ||
		replay.Confirmation.ReplayReference != "encrypted-result/ref-1" {
		t.Fatalf("confirmed replay = %+v", replay)
	}
	operations, err := ledger.Lease(ctx, LeaseRequest{
		TenantID: "tenant-a", Owner: "other-owner", Now: now.Add(time.Hour), TTL: time.Minute, Limit: 1,
	})
	if err != nil || len(operations) != 0 {
		t.Fatalf("confirmed operation was leased: operations=%+v err=%v", operations, err)
	}

	encoded, err := json.Marshal(struct {
		Record Record
		Fence  Fence
	}{Record: *confirmed, Fence: leased.Fence})
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"fixture-sensitive-value", "fixture-capability-value", "fixture-result-value"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("sanitized ledger value leaked %q: %s", secret, encoded)
		}
	}
}

func TestMemoryAttemptFencingAndUnknownRequiresResolution(t *testing.T) {
	ctx := context.Background()
	ledger := NewMemory()
	now := time.Date(2026, 8, 31, 11, 0, 0, 0, time.UTC)
	for _, tenantID := range []string{"tenant-a", "tenant-b"} {
		if _, err := ledger.Reserve(ctx, testReserve(tenantID, "shared-operation", tenantID+"-payload"), now); err != nil {
			t.Fatal(err)
		}
	}

	first := mustLease(t, ledger, "tenant-a", "owner-1", now, time.Minute, 1)
	if first.Record.TenantID != "tenant-a" {
		t.Fatalf("cross-tenant lease = %+v", first)
	}
	reclaimed, err := ledger.ReclaimExpired(ctx, now.Add(time.Minute), 10)
	if err != nil {
		t.Fatal(err)
	}
	if reclaimed.Retryable != 1 || reclaimed.Unknown != 0 {
		t.Fatalf("reserved reclaim = %+v", reclaimed)
	}
	firstAttempt, err := ledger.GetAttempt(ctx, "tenant-a", "shared-operation", 1)
	if err != nil {
		t.Fatal(err)
	}
	if firstAttempt.Outcome != StateRetryableNotApplied || firstAttempt.OutcomeCode != leaseExpiredBeforeExec {
		t.Fatalf("expired reserved attempt = %+v", firstAttempt)
	}

	second := mustLease(t, ledger, "tenant-a", "owner-2", now.Add(time.Minute), time.Minute, 1)
	if second.Fence.AttemptNo != 2 {
		t.Fatalf("attempt number = %d, want 2", second.Fence.AttemptNo)
	}
	if _, err := ledger.MarkExecuting(ctx, first.Fence, now.Add(time.Minute)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale fence error = %v", err)
	}
	if err := ledger.Renew(ctx, second.Fence, now.Add(90*time.Second), time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.MarkExecuting(ctx, second.Fence, now.Add(100*time.Second)); err != nil {
		t.Fatal(err)
	}

	reclaimed, err = ledger.ReclaimExpired(ctx, now.Add(151*time.Second), 10)
	if err != nil {
		t.Fatal(err)
	}
	if reclaimed.Unknown != 1 || reclaimed.Retryable != 0 {
		t.Fatalf("executing reclaim = %+v", reclaimed)
	}
	unknown, err := ledger.Get(ctx, "tenant-a", "shared-operation")
	if err != nil {
		t.Fatal(err)
	}
	if unknown.State != StateUnknown || unknown.OutcomeCode != leaseExpiredAfterExec {
		t.Fatalf("unknown record = %+v", unknown)
	}
	if operations, err := ledger.Lease(ctx, LeaseRequest{
		TenantID: "tenant-a", Owner: "owner-3", Now: now.Add(time.Hour), TTL: time.Minute, Limit: 10,
	}); err != nil || len(operations) != 0 {
		t.Fatalf("unknown automatically retried: operations=%+v err=%v", operations, err)
	}
	listed, err := ledger.ListUnknown(ctx, "tenant-a", 10)
	if err != nil || len(listed) != 1 || listed[0].OperationKey != "shared-operation" {
		t.Fatalf("unknown list: records=%+v err=%v", listed, err)
	}
	otherTenant, err := ledger.Get(ctx, "tenant-b", "shared-operation")
	if err != nil || otherTenant.State != StateReserved {
		t.Fatalf("tenant-b record changed: record=%+v err=%v", otherTenant, err)
	}

	resolution := ResolveRequest{
		ResolutionID: "resolution-1", TenantID: "tenant-a", OperationKey: "shared-operation",
		ExpectedVersion: unknown.StateVersion, Action: ResolveRetryNotApplied,
		ActorHash: HashPayload([]byte("operator@example.com")), ReasonCode: "provider_proved_absent",
		RetryAt: now.Add(2 * time.Hour),
	}
	resolved, err := ledger.ResolveUnknown(ctx, resolution, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if resolved.State != StateRetryableNotApplied {
		t.Fatalf("resolved = %+v", resolved)
	}
	if _, err := ledger.ResolveUnknown(ctx, resolution, now.Add(time.Hour+time.Second)); err != nil {
		t.Fatalf("idempotent resolution: %v", err)
	}
	changedResolution := resolution
	changedResolution.ReasonCode = "different_reason"
	if _, err := ledger.ResolveUnknown(ctx, changedResolution, now.Add(time.Hour)); !errors.Is(err, ErrConflict) {
		t.Fatalf("resolution conflict error = %v", err)
	}
	if operations, err := ledger.Lease(ctx, LeaseRequest{
		TenantID: "tenant-a", Owner: "owner-4", Now: now.Add(2*time.Hour - time.Second), TTL: time.Minute, Limit: 1,
	}); err != nil || len(operations) != 0 {
		t.Fatalf("operation leased before explicit retry time: operations=%+v err=%v", operations, err)
	}
	third := mustLease(t, ledger, "tenant-a", "owner-4", now.Add(2*time.Hour), time.Minute, 1)
	if third.Fence.AttemptNo != 3 {
		t.Fatalf("resolved retry attempt = %d, want 3", third.Fence.AttemptNo)
	}
}

func TestMemoryOutcomeRules(t *testing.T) {
	ctx := context.Background()
	ledger := NewMemory()
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	if _, err := ledger.Reserve(ctx, testReserve("tenant-a", "retry-operation", "payload"), now); err != nil {
		t.Fatal(err)
	}
	lease := mustLease(t, ledger, "tenant-a", "owner", now, time.Minute, 1)
	if _, err := ledger.Finish(ctx, FinishRequest{
		Fence: lease.Fence, Outcome: StateUnknown, OutcomeCode: "transport_ambiguous",
		ResultHash: HashPayload([]byte("provider-response-hash-is-confirmation-only")),
	}, now.Add(time.Second)); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("non-confirmed result hash error = %v", err)
	}
	if _, err := ledger.Finish(ctx, FinishRequest{
		Fence: lease.Fence, Outcome: StateUnknown, OutcomeCode: "transport_ambiguous",
	}, now.Add(time.Second)); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("unknown before executing error = %v", err)
	}
	retryAt := now.Add(10 * time.Minute)
	if _, err := ledger.Finish(ctx, FinishRequest{
		Fence: lease.Fence, Outcome: StateRetryableNotApplied,
		OutcomeCode: "validation_not_sent", RetryAt: retryAt,
	}, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := mustLeaseAtMost(t, ledger, "tenant-a", "owner-2", retryAt.Add(-time.Nanosecond), time.Minute, 1); len(got) != 0 {
		t.Fatalf("retry leased early: %+v", got)
	}
	second := mustLease(t, ledger, "tenant-a", "owner-2", retryAt, time.Minute, 1)
	if _, err := ledger.Finish(ctx, FinishRequest{
		Fence: second.Fence, Outcome: StatePermanentRejected, OutcomeCode: "policy_rejected",
	}, retryAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := mustLeaseAtMost(t, ledger, "tenant-a", "owner-3", retryAt.Add(time.Hour), time.Minute, 1); len(got) != 0 {
		t.Fatalf("permanent rejection leased: %+v", got)
	}
}

func TestMemoryConcurrentReserveAndLeaseSingleWinner(t *testing.T) {
	ctx := context.Background()
	ledger := NewMemory()
	now := time.Date(2026, 8, 31, 13, 0, 0, 0, time.UTC)
	req := testReserve("tenant-a", "concurrent-operation", "payload")
	const goroutines = 32
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := ledger.Reserve(ctx, req, now)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	winners := make(chan int, goroutines)
	leaseErrs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			leases, err := ledger.Lease(ctx, LeaseRequest{
				TenantID: "tenant-a", Owner: "owner-" + strings.Repeat("x", index+1),
				Now: now, TTL: time.Minute, Limit: 1,
			})
			if err != nil {
				leaseErrs <- err
				return
			}
			winners <- len(leases)
		}(i)
	}
	wg.Wait()
	close(winners)
	close(leaseErrs)
	for err := range leaseErrs {
		t.Fatal(err)
	}
	count := 0
	for winner := range winners {
		count += winner
	}
	if count != 1 {
		t.Fatalf("lease winners = %d, want 1", count)
	}
}

func TestMemoryLeaseOperationExactConcurrentAndTenantScoped(t *testing.T) {
	ctx := context.Background()
	ledger := NewMemory()
	now := time.Date(2026, 8, 31, 14, 0, 0, 0, time.UTC)
	for _, req := range []ReserveRequest{
		testReserve("tenant-a", "target-operation", "target-a"),
		testReserve("tenant-a", "other-operation", "other-a"),
		testReserve("tenant-b", "target-operation", "target-b"),
	} {
		if _, err := ledger.Reserve(ctx, req, now); err != nil {
			t.Fatal(err)
		}
	}

	type leaseResult struct {
		leased *LeasedOperation
		err    error
	}
	const contenders = 32
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
				"exact-owner-"+strings.Repeat("x", index+1), now, time.Minute,
			)
			results <- leaseResult{leased: leased, err: err}
		}(index)
	}
	close(start)
	wg.Wait()
	close(results)

	winners := 0
	for result := range results {
		if result.err == nil {
			winners++
			if result.leased == nil || result.leased.Record.OperationKey != "target-operation" ||
				result.leased.Record.TenantID != "tenant-a" {
				t.Fatalf("exact lease substituted operation: %+v", result.leased)
			}
			continue
		}
		if !errors.Is(result.err, ErrInvalidTransition) {
			t.Fatalf("losing exact lease error = %v", result.err)
		}
	}
	if winners != 1 {
		t.Fatalf("exact lease winners = %d, want 1", winners)
	}

	other, err := ledger.Get(ctx, "tenant-a", "other-operation")
	if err != nil {
		t.Fatal(err)
	}
	if other.AttemptCount != 0 || !other.LeaseExpiresAt.IsZero() {
		t.Fatalf("exact lease touched another due operation: %+v", other)
	}
	otherTenant, err := ledger.LeaseOperation(
		ctx, "tenant-b", "target-operation", "tenant-b-owner", now, time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if otherTenant.Record.TenantID != "tenant-b" || otherTenant.Fence.AttemptNo != 1 {
		t.Fatalf("tenant-scoped exact lease = %+v", otherTenant)
	}
	if _, err := ledger.LeaseOperation(
		ctx, "tenant-c", "target-operation", "missing-owner", now, time.Minute,
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant lookup error = %v", err)
	}
}

func TestMemoryLeaseOperationDueAndExpiredFenceSemantics(t *testing.T) {
	ctx := context.Background()
	ledger := NewMemory()
	now := time.Date(2026, 8, 31, 15, 0, 0, 0, time.UTC)
	if _, err := ledger.Reserve(ctx, testReserve("tenant-a", "due-operation", "payload"), now); err != nil {
		t.Fatal(err)
	}
	first, err := ledger.LeaseOperation(
		ctx, "tenant-a", "due-operation", "owner-1", now, time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	retryAt := now.Add(10 * time.Minute)
	if _, err := ledger.Finish(ctx, FinishRequest{
		Fence: first.Fence, Outcome: StateRetryableNotApplied,
		OutcomeCode: "proved_not_applied", RetryAt: retryAt,
	}, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.Reserve(ctx, testReserve("tenant-a", "other-due", "payload"), now); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.LeaseOperation(
		ctx, "tenant-a", "due-operation", "owner-2", retryAt.Add(-time.Nanosecond), time.Minute,
	); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("lease before due error = %v", err)
	}
	other, err := ledger.Get(ctx, "tenant-a", "other-due")
	if err != nil {
		t.Fatal(err)
	}
	if other.AttemptCount != 0 {
		t.Fatalf("not-due exact lease substituted another operation: %+v", other)
	}
	second, err := ledger.LeaseOperation(
		ctx, "tenant-a", "due-operation", "owner-2", retryAt, time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if second.Fence.AttemptNo != 2 {
		t.Fatalf("retry attempt = %d, want 2", second.Fence.AttemptNo)
	}

	if _, err := ledger.Reserve(ctx, testReserve("tenant-a", "expired-operation", "payload"), now); err != nil {
		t.Fatal(err)
	}
	expired, err := ledger.LeaseOperation(
		ctx, "tenant-a", "expired-operation", "expired-owner-1", now, time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := ledger.LeaseOperation(
		ctx, "tenant-a", "expired-operation", "expired-owner-2", now.Add(time.Minute), time.Minute,
	)
	if err != nil {
		t.Fatal(err)
	}
	if replacement.Fence.AttemptNo != 2 {
		t.Fatalf("replacement attempt = %d, want 2", replacement.Fence.AttemptNo)
	}
	if _, err := ledger.MarkExecuting(ctx, expired.Fence, now.Add(time.Minute)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("expired fence error = %v", err)
	}
	attempt, err := ledger.GetAttempt(ctx, "tenant-a", "expired-operation", 1)
	if err != nil {
		t.Fatal(err)
	}
	if attempt.Phase != AttemptFinished || attempt.Outcome != StateRetryableNotApplied ||
		attempt.OutcomeCode != leaseExpiredBeforeExec {
		t.Fatalf("expired reserved attempt = %+v", attempt)
	}
}

func TestErrorsNeverEchoCallerData(t *testing.T) {
	secret := "fixture-sensitive-value and fixture-bearer"
	_, err := NewMemory().Reserve(context.Background(), ReserveRequest{
		TenantID: "tenant-a", OperationKey: "operation", PayloadHash: HashPayload([]byte(secret)),
		Metadata: Metadata{ToolName: secret},
	}, time.Now())
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("error = %v", err)
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error leaked caller data: %v", err)
	}
}

func testReserve(tenantID, operationID, payload string) ReserveRequest {
	return ReserveRequest{
		TenantID: tenantID, OperationKey: operationID, PayloadHash: HashPayload([]byte(payload)),
		Metadata: Metadata{
			ToolName: "calendar.create", OperationClass: "external_write",
			TargetHash: HashPayload([]byte("private-target")),
		},
	}
}

func mustLease(
	t *testing.T,
	ledger Ledger,
	tenantID, owner string,
	now time.Time,
	ttl time.Duration,
	limit int,
) LeasedOperation {
	t.Helper()
	operations := mustLeaseAtMost(t, ledger, tenantID, owner, now, ttl, limit)
	if len(operations) != 1 {
		t.Fatalf("leases = %d, want 1: %+v", len(operations), operations)
	}
	return operations[0]
}

func mustLeaseAtMost(
	t *testing.T,
	ledger Ledger,
	tenantID, owner string,
	now time.Time,
	ttl time.Duration,
	limit int,
) []LeasedOperation {
	t.Helper()
	operations, err := ledger.Lease(context.Background(), LeaseRequest{
		TenantID: tenantID, Owner: owner, Now: now, TTL: ttl, Limit: limit,
	})
	if err != nil {
		t.Fatal(err)
	}
	return operations
}
