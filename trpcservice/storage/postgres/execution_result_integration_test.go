package postgres

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func executionCommitRecordForLease(tc tenant.TenantContext, sessionID string, lease storage.Lease, jobID, executionID, text string) storage.ExecutionCommitRecord {
	return storage.ExecutionCommitRecord{
		JobID: jobID, ExecutionID: executionID, TenantID: tc.TenantID, SessionID: sessionID,
		OwnerID: lease.OwnerID, Epoch: lease.Epoch, FenceToken: lease.FenceToken,
		ResultJSON: []byte(fmt.Sprintf(`{"text":%q}`, text)), ConfigVersion: tc.ConfigVersion,
	}
}

func TestPostgresExecutionCommitValidAndIdempotent(t *testing.T) {
	store, pool, ctx, tc, sessionID := postgresLeaseFixture(t)
	repository, err := NewExecutionResultRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Acquire(ctx, tc, sessionID, "execution-owner", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	record := executionCommitRecordForLease(tc, sessionID, lease, "job-commit", "execution-commit", "first")
	if err := repository.CommitExecution(ctx, record); err != nil {
		t.Fatal(err)
	}
	got, err := repository.GetExecutionResult(ctx, tc, record.JobID, record.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	if got.OwnerID != lease.OwnerID || got.Epoch != lease.Epoch || got.FenceToken != lease.FenceToken || got.Status != "succeeded" || got.ResultVersion != 1 || got.ConfigVersion != record.ConfigVersion || !equalJSON(got.ResultJSON, record.ResultJSON) {
		t.Fatalf("persisted result=%+v", got)
	}
	if err := repository.CommitExecution(ctx, record); err != nil {
		t.Fatalf("same-token idempotent commit failed: %v", err)
	}
	changed := record
	changed.ResultJSON = []byte(`{"text":"different"}`)
	if err := repository.CommitExecution(ctx, changed); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("changed duplicate error=%v", err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM execution_result WHERE tenant_id=$1 AND execution_id=$2`, tc.TenantID, record.ExecutionID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("result row count=%d", count)
	}
}

func TestPostgresExecutionCommitRejectsValidateThenTakeover(t *testing.T) {
	store, pool, ctx, tc, sessionID := postgresLeaseFixture(t)
	repository, err := NewExecutionResultRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	oldLease, err := store.Acquire(ctx, tc, sessionID, "old-owner", 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	validated := make(chan struct{})
	continueCommit := make(chan struct{})
	oldResult := make(chan error, 1)
	oldRecord := executionCommitRecordForLease(tc, sessionID, oldLease, "job-takeover", "execution-takeover", "old")
	go func() {
		if err := store.Validate(ctx, tc, oldLease); err != nil {
			oldResult <- err
			return
		}
		close(validated)
		<-continueCommit
		oldResult <- repository.CommitExecution(ctx, oldRecord)
	}()
	select {
	case <-validated:
	case <-time.After(time.Second):
		t.Fatal("old owner did not validate")
	}
	time.Sleep(600 * time.Millisecond)
	newLease, err := store.Acquire(ctx, tc, sessionID, "new-owner", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	close(continueCommit)
	if err := <-oldResult; !errors.Is(err, storage.ErrFenceRejected) && !errors.Is(err, storage.ErrEpochRejected) && !errors.Is(err, storage.ErrLeaseLost) {
		t.Fatalf("old owner commit error=%v", err)
	}
	oldResultRecord := oldRecord
	if _, err := repository.GetExecutionResult(ctx, tc, oldResultRecord.JobID, oldResultRecord.ExecutionID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("old result lookup error=%v", err)
	}
	newRecord := executionCommitRecordForLease(tc, sessionID, newLease, oldRecord.JobID, oldRecord.ExecutionID, "new")
	if err := repository.CommitExecution(ctx, newRecord); err != nil {
		t.Fatalf("new owner commit failed: %v", err)
	}
	got, err := repository.GetExecutionResult(ctx, tc, newRecord.JobID, newRecord.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	if got.OwnerID != newLease.OwnerID || !equalJSON(got.ResultJSON, newRecord.ResultJSON) {
		t.Fatalf("new result=%+v", got)
	}
}

func TestPostgresExecutionCommitRejectsLeaseAndIdentityBoundaries(t *testing.T) {
	store, pool, ctx, tc, sessionID := postgresLeaseFixture(t)
	repository, err := NewExecutionResultRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Acquire(ctx, tc, sessionID, "boundary-owner", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	base := executionCommitRecordForLease(tc, sessionID, lease, "job-boundary", "execution-boundary", "boundary")
	cases := []struct {
		name string
		edit func(*storage.ExecutionCommitRecord)
		want []error
	}{
		{"tenant mismatch", func(r *storage.ExecutionCommitRecord) { r.TenantID = "missing-tenant" }, []error{storage.ErrTenantMismatch}},
		{"session mismatch", func(r *storage.ExecutionCommitRecord) { r.SessionID = "missing-session" }, []error{storage.ErrNotFound}},
		{"owner mismatch", func(r *storage.ExecutionCommitRecord) { r.OwnerID = "other-owner" }, []error{storage.ErrFenceRejected}},
		{"epoch mismatch", func(r *storage.ExecutionCommitRecord) { r.Epoch++ }, []error{storage.ErrEpochRejected}},
		{"fence mismatch", func(r *storage.ExecutionCommitRecord) { r.FenceToken++ }, []error{storage.ErrFenceRejected}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			record := base
			tc.edit(&record)
			err := repository.CommitExecution(ctx, record)
			matched := false
			for _, want := range tc.want {
				matched = matched || errors.Is(err, want)
			}
			if !matched {
				t.Fatalf("error=%v", err)
			}
			if _, lookupErr := repository.GetExecutionResult(ctx, tenant.TenantContext{TenantID: base.TenantID}, base.JobID, base.ExecutionID); !errors.Is(lookupErr, storage.ErrNotFound) {
				t.Fatalf("unexpected result after rejected commit: %v", lookupErr)
			}
		})
	}
	if err := store.Release(ctx, tc, lease); err != nil {
		t.Fatal(err)
	}
	shortLease, err := store.Acquire(ctx, tenant.TenantContext{TenantID: tc.TenantID, AgentAppID: tc.AgentAppID, BindingID: tc.BindingID, Channel: tc.Channel}, sessionID, "expiry-owner", 30*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	expired := executionCommitRecordForLease(tc, sessionID, shortLease, "job-expired", "execution-expired", "expired")
	if err := repository.CommitExecution(ctx, expired); !errors.Is(err, storage.ErrLeaseLost) {
		t.Fatalf("expired lease error=%v", err)
	}
}

func TestPostgresExecutionCommitConcurrentOwnersHaveOneDurableWinner(t *testing.T) {
	store, pool, ctx, tc, sessionID := postgresLeaseFixture(t)
	repository, err := NewExecutionResultRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	oldLease, err := store.Acquire(ctx, tc, sessionID, "concurrent-old", 40*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(70 * time.Millisecond)
	newLease, err := store.Acquire(ctx, tc, sessionID, "concurrent-new", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	oldRecord := executionCommitRecordForLease(tc, sessionID, oldLease, "job-concurrent", "execution-concurrent", "old")
	newRecord := executionCommitRecordForLease(tc, sessionID, newLease, oldRecord.JobID, oldRecord.ExecutionID, "new")
	start := make(chan struct{})
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for _, record := range []storage.ExecutionCommitRecord{oldRecord, newRecord} {
		wg.Add(1)
		go func(record storage.ExecutionCommitRecord) {
			defer wg.Done()
			<-start
			results <- repository.CommitExecution(ctx, record)
		}(record)
	}
	close(start)
	wg.Wait()
	close(results)
	var success int
	for err := range results {
		if err == nil {
			success++
			continue
		}
		if !errors.Is(err, storage.ErrFenceRejected) && !errors.Is(err, storage.ErrConflict) && !errors.Is(err, storage.ErrEpochRejected) && !errors.Is(err, storage.ErrLeaseLost) {
			t.Fatalf("unexpected concurrent error=%v", err)
		}
	}
	if success != 1 {
		t.Fatalf("successful concurrent commits=%d", success)
	}
	got, err := repository.GetExecutionResult(ctx, tc, newRecord.JobID, newRecord.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	if got.OwnerID != newLease.OwnerID || !equalJSON(got.ResultJSON, newRecord.ResultJSON) {
		t.Fatalf("durable winner=%+v", got)
	}
}

func TestPostgresExecutionCommitRollsBackAfterInsertFailure(t *testing.T) {
	store, pool, ctx, tc, sessionID := postgresLeaseFixture(t)
	repository, err := NewExecutionResultRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Acquire(ctx, tc, sessionID, "rollback-owner", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	record := executionCommitRecordForLease(tc, sessionID, lease, "job-rollback", "execution-rollback", "rollback")
	pgErr := &pgconn.PgError{Code: "40001", Message: "injected serialization failure"}
	repository.beforeCommit = func() error { return pgErr }
	err = repository.CommitExecution(ctx, record)
	var gotPGErr *pgconn.PgError
	if !errors.As(err, &gotPGErr) || gotPGErr.Code != pgErr.Code {
		t.Fatalf("rollback error chain=%v", err)
	}
	repository.beforeCommit = nil
	if _, err := repository.GetExecutionResult(ctx, tc, record.JobID, record.ExecutionID); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("half-committed result lookup=%v", err)
	}
	if err := repository.CommitExecution(ctx, record); err != nil {
		t.Fatalf("commit after rollback failed: %v", err)
	}
}
