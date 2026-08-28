package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type atomicCompletionFixture struct {
	store       *CoordinationStore
	queue       *queue.PostgresQueue
	repository  *ExecutionResultRepository
	coordinator *AtomicCompletionCoordinator
	ctx         context.Context
	tenant      tenant.TenantContext
	sessionID   string
	job         queue.AgentJob
	delivery    queue.Delivery
	lease       storage.Lease
	request     storage.AtomicCompletionRequest
}

func newAtomicCompletionFixture(t *testing.T, visibility, leaseTTL time.Duration) *atomicCompletionFixture {
	t.Helper()
	store, pool, ctx, tc, sessionID := postgresLeaseFixture(t)
	tc.SessionID = sessionID
	q, err := queue.NewPostgresQueue(pool, queue.PostgresQueueConfig{
		MaxJobAge:              2 * time.Hour,
		MaxVisibilityExtension: 2 * time.Hour,
		PollInterval:           5 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	repository, err := NewExecutionResultRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := NewAtomicCompletionCoordinator(pool)
	if err != nil {
		t.Fatal(err)
	}
	job := atomicCompletionJob(tc, sessionID, fmt.Sprintf("job-completion-%d", time.Now().UnixNano()), fmt.Sprintf("execution-completion-%d", time.Now().UnixNano()))
	if _, err := q.Enqueue(ctx, job); err != nil {
		t.Fatal(err)
	}
	delivery, err := q.Receive(ctx, visibility)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.Acquire(ctx, tc, sessionID, "completion-owner", leaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	fixture := &atomicCompletionFixture{
		store: store, queue: q, repository: repository, coordinator: coordinator,
		ctx: ctx, tenant: tc, sessionID: sessionID, job: job, delivery: delivery, lease: lease,
	}
	fixture.request = atomicCompletionRequest(delivery, lease, `{"text":"atomic"}`)
	t.Cleanup(func() { _ = q.Close() })
	return fixture
}

func atomicCompletionJob(tc tenant.TenantContext, sessionID, jobID, executionID string) queue.AgentJob {
	now := time.Now().UTC()
	tc.SessionID = sessionID
	return queue.AgentJob{
		SchemaVersion: queue.SchemaVersion,
		JobID:         jobID,
		ExecutionID:   executionID,
		Tenant:        queue.TenantContextDTOFromContext(tc),
		Agent:         queue.AgentRefDTO{TenantID: tc.TenantID, AgentAppID: tc.AgentAppID, Version: tc.ConfigVersion},
		Message:       queue.MessageDTO{ID: tc.MessageID, Role: "user", Content: "completion", CreatedAt: now},
		Trace:         queue.TraceContextDTO{TraceID: tc.TraceID, RequestID: tc.RequestID, MessageID: tc.MessageID, ExecutionID: executionID},
		CreatedAt:     now,
		Deadline:      now.Add(time.Minute),
		Attempt:       1,
	}
}

func atomicCompletionRequest(delivery queue.Delivery, lease storage.Lease, resultJSON string) storage.AtomicCompletionRequest {
	return storage.AtomicCompletionRequest{
		Commit: storage.ExecutionCommitRecord{
			JobID: delivery.Job.JobID, ExecutionID: delivery.Job.ExecutionID,
			TenantID: delivery.Job.Tenant.TenantID, SessionID: delivery.Job.Tenant.SessionID,
			OwnerID: lease.OwnerID, Epoch: lease.Epoch, FenceToken: lease.FenceToken,
			ResultJSON: []byte(resultJSON),
		},
		Delivery: storage.DeliveryAckRecord{
			TenantID: delivery.Job.Tenant.TenantID, JobID: delivery.Job.JobID,
			ExecutionID: delivery.Job.ExecutionID, SessionID: delivery.Job.Tenant.SessionID,
			DeliveryID: delivery.DeliveryID,
		},
	}
}

func atomicCompletionRequestWithOutbox(delivery queue.Delivery, lease storage.Lease, resultJSON string) storage.AtomicCompletionRequest {
	request := atomicCompletionRequest(delivery, lease, resultJSON)
	payload, err := json.Marshal(struct {
		SchemaVersion int    `json:"schema_version"`
		Kind          string `json:"kind"`
		TenantID      string `json:"tenant_id"`
		SessionID     string `json:"session_id"`
		JobID         string `json:"job_id"`
		ExecutionID   string `json:"execution_id"`
		RequestID     string `json:"request_id"`
		MessageID     string `json:"message_id"`
		TraceID       string `json:"trace_id"`
		ReplyText     string `json:"reply_text"`
	}{1, "agent.reply", request.Commit.TenantID, request.Commit.SessionID, request.Commit.JobID, request.Commit.ExecutionID, "request", "message", "trace", resultJSON})
	if err != nil {
		panic(err)
	}
	request.Outbox = &storage.OutboxMessage{
		TenantID: request.Commit.TenantID, ID: "reply-" + request.Commit.ExecutionID,
		Kind: "agent.reply", AggregateID: request.Commit.ExecutionID,
		DedupKey: request.Commit.TenantID + "|" + request.Commit.ExecutionID + "|agent.reply", Payload: payload,
	}
	return request
}

func completionOutboxState(t *testing.T, f *atomicCompletionFixture) (status string, attempt int, lockedBy string, payload []byte) {
	t.Helper()
	var locked *string
	if err := f.store.pool.QueryRow(f.ctx, `
SELECT status, attempt, locked_by, payload
FROM outbox_message WHERE tenant_id=$1 AND outbox_id=$2`, f.tenant.TenantID, "reply-"+f.job.ExecutionID).Scan(&status, &attempt, &locked, &payload); err != nil {
		t.Fatal(err)
	}
	if locked != nil {
		lockedBy = *locked
	}
	return
}

func completionQueueStatus(t *testing.T, f *atomicCompletionFixture) (status, deliveryID, lastDeliveryID string) {
	t.Helper()
	if err := f.store.pool.QueryRow(f.ctx, `
SELECT status, COALESCE(delivery_id, ''), COALESCE(last_delivery_id, '')
FROM job_queue WHERE tenant_id = $1 AND job_id = $2`, f.tenant.TenantID, f.job.JobID).Scan(&status, &deliveryID, &lastDeliveryID); err != nil {
		t.Fatal(err)
	}
	return
}

func completionResultCount(t *testing.T, f *atomicCompletionFixture) int {
	t.Helper()
	var count int
	if err := f.store.pool.QueryRow(f.ctx, `SELECT count(*) FROM execution_result WHERE tenant_id=$1 AND job_id=$2`, f.tenant.TenantID, f.job.JobID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func waitForCompletionCondition(t *testing.T, check func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		ok, err := check()
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true before deadline")
		}
		<-ticker.C
	}
}

func waitForQueueDeliveryExpiry(t *testing.T, f *atomicCompletionFixture) {
	t.Helper()
	waitForCompletionCondition(t, func() (bool, error) {
		var active bool
		err := f.store.pool.QueryRow(f.ctx, `
SELECT leased_until > clock_timestamp()
FROM job_queue WHERE tenant_id=$1 AND job_id=$2`, f.tenant.TenantID, f.job.JobID).Scan(&active)
		return !active, err
	})
}

func waitForLeaseExpiry(t *testing.T, f *atomicCompletionFixture) {
	t.Helper()
	waitForCompletionCondition(t, func() (bool, error) {
		var active bool
		err := f.store.pool.QueryRow(f.ctx, `
SELECT leased_until > clock_timestamp()
FROM session_lease WHERE tenant_id=$1 AND session_id=$2`, f.tenant.TenantID, f.sessionID).Scan(&active)
		return !active, err
	})
}

func expireQueueDelivery(t *testing.T, f *atomicCompletionFixture) {
	t.Helper()
	if _, err := f.store.pool.Exec(f.ctx, `
UPDATE job_queue SET leased_until = clock_timestamp() - interval '1 second'
WHERE tenant_id=$1 AND job_id=$2`, f.tenant.TenantID, f.job.JobID); err != nil {
		t.Fatal(err)
	}
}

func installCompletionResultFailure(t *testing.T, f *atomicCompletionFixture) {
	t.Helper()
	_, err := f.store.pool.Exec(f.ctx, `
CREATE OR REPLACE FUNCTION completion_result_failure() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected result write failure'; END; $$;
CREATE TRIGGER completion_result_failure_trigger BEFORE INSERT ON execution_result
FOR EACH ROW EXECUTE FUNCTION completion_result_failure()`)
	if err != nil {
		t.Fatal(err)
	}
}

func installCompletionAckFailure(t *testing.T, f *atomicCompletionFixture) {
	t.Helper()
	_, err := f.store.pool.Exec(f.ctx, `
CREATE OR REPLACE FUNCTION completion_ack_failure() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected ack update failure'; END; $$;
CREATE TRIGGER completion_ack_failure_trigger BEFORE UPDATE ON job_queue
FOR EACH ROW WHEN (NEW.status = 'acked') EXECUTE FUNCTION completion_ack_failure()`)
	if err != nil {
		t.Fatal(err)
	}
}

func installCompletionOutboxFailure(t *testing.T, f *atomicCompletionFixture) {
	t.Helper()
	_, err := f.store.pool.Exec(f.ctx, `
CREATE OR REPLACE FUNCTION completion_outbox_failure() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected outbox write failure'; END; $$;
CREATE TRIGGER completion_outbox_failure_trigger BEFORE INSERT ON outbox_message
FOR EACH ROW WHEN (NEW.kind = 'agent.reply') EXECUTE FUNCTION completion_outbox_failure()`)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPostgresAtomicCompletionCommitsResultAndAckTogether(t *testing.T) {
	f := newAtomicCompletionFixture(t, time.Second, 2*time.Second)
	if err := f.coordinator.CommitResultAndAck(f.ctx, f.request); err != nil {
		t.Fatal(err)
	}
	result, err := f.repository.GetExecutionResult(f.ctx, tenant.TenantContext{TenantID: f.tenant.TenantID}, f.job.JobID, f.job.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	if result.JobID != f.job.JobID || result.ExecutionID != f.job.ExecutionID || result.TenantID != f.tenant.TenantID || result.SessionID != f.sessionID || result.OwnerID != f.lease.OwnerID || result.Epoch != f.lease.Epoch || result.FenceToken != f.lease.FenceToken || !equalJSON(result.ResultJSON, f.request.Commit.ResultJSON) {
		t.Fatalf("unexpected result identity/status: %+v", result)
	}
	status, activeDelivery, lastDelivery := completionQueueStatus(t, f)
	if status != "acked" || activeDelivery != "" || lastDelivery != f.delivery.DeliveryID {
		t.Fatalf("unexpected queue status=%s active=%s last=%s", status, activeDelivery, lastDelivery)
	}
}

func TestPostgresAtomicCompletionWithOutboxCommitsAllFacts(t *testing.T) {
	f := newAtomicCompletionFixture(t, time.Second, 2*time.Second)
	f.request = atomicCompletionRequestWithOutbox(f.delivery, f.lease, `{"text":"atomic reply"}`)
	if err := f.coordinator.CommitResultAndAck(f.ctx, f.request); err != nil {
		t.Fatal(err)
	}
	status, attempt, lockedBy, payload := completionOutboxState(t, f)
	if status != string(storage.OutboxPending) || attempt != 1 || lockedBy != "" || !equalJSON(payload, f.request.Outbox.Payload) {
		t.Fatalf("unexpected reply outbox status=%s attempt=%d locked_by=%q payload=%s", status, attempt, lockedBy, payload)
	}
	queueStatus, activeDelivery, lastDelivery := completionQueueStatus(t, f)
	if queueStatus != "acked" || activeDelivery != "" || lastDelivery != f.delivery.DeliveryID {
		t.Fatalf("unexpected queue status=%s active=%s last=%s", queueStatus, activeDelivery, lastDelivery)
	}
	if err := f.coordinator.CommitResultAndAck(f.ctx, f.request); err != nil {
		t.Fatalf("duplicate three-fact completion=%v", err)
	}
	if completionResultCount(t, f) != 1 {
		t.Fatal("duplicate completion created another execution result")
	}
	var outboxCount int
	if err := f.store.pool.QueryRow(f.ctx, `SELECT count(*) FROM outbox_message WHERE tenant_id=$1 AND aggregate_id=$2`, f.tenant.TenantID, f.job.ExecutionID).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != 1 {
		t.Fatalf("duplicate completion created %d reply outbox rows", outboxCount)
	}
}

func TestPostgresAtomicCompletionOutboxIdentityAndDedupConflicts(t *testing.T) {
	f := newAtomicCompletionFixture(t, time.Second, 2*time.Second)
	f.request = atomicCompletionRequestWithOutbox(f.delivery, f.lease, `{"text":"reply"}`)
	if err := f.coordinator.CommitResultAndAck(f.ctx, f.request); err != nil {
		t.Fatal(err)
	}
	conflicting := atomicCompletionRequestWithOutbox(f.delivery, f.lease, `{"text":"reply"}`)
	conflicting.Outbox.ID = "reply-other-id"
	if err := f.coordinator.CommitResultAndAck(f.ctx, conflicting); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("dedup conflict=%v", err)
	}
	conflicting = atomicCompletionRequestWithOutbox(f.delivery, f.lease, `{"text":"reply"}`)
	conflicting.Outbox.Payload = []byte(`{"schema_version":1,"kind":"agent.reply","tenant_id":"` + f.tenant.TenantID + `","session_id":"` + f.sessionID + `","job_id":"` + f.job.JobID + `","execution_id":"` + f.job.ExecutionID + `","reply_text":"different"}`)
	if err := f.coordinator.CommitResultAndAck(f.ctx, conflicting); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("payload conflict=%v", err)
	}
	conflicting = atomicCompletionRequestWithOutbox(f.delivery, f.lease, `{"text":"reply"}`)
	conflicting.Outbox.AggregateID = "other-execution"
	if err := f.coordinator.CommitResultAndAck(f.ctx, conflicting); !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("aggregate identity conflict=%v", err)
	}
	conflicting = atomicCompletionRequestWithOutbox(f.delivery, f.lease, `{"text":"reply"}`)
	conflicting.Outbox.TenantID = "tenant-other"
	if err := f.coordinator.CommitResultAndAck(f.ctx, conflicting); !errors.Is(err, storage.ErrTenantMismatch) {
		t.Fatalf("tenant identity conflict=%v", err)
	}
}

func TestPostgresAtomicCompletionWithOutboxRollsBackAllFacts(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, *atomicCompletionFixture)
		want  string
	}{
		{name: "result failure", setup: installCompletionResultFailure, want: "injected result write failure"},
		{name: "outbox failure", setup: installCompletionOutboxFailure, want: "injected outbox write failure"},
		{name: "ack failure", setup: installCompletionAckFailure, want: "injected ack update failure"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newAtomicCompletionFixture(t, 2*time.Second, 2*time.Second)
			f.request = atomicCompletionRequestWithOutbox(f.delivery, f.lease, `{"text":"rollback"}`)
			test.setup(t, f)
			err := f.coordinator.CommitResultAndAck(f.ctx, f.request)
			var pgErr *pgconn.PgError
			if err == nil || !errors.As(err, &pgErr) || pgErr.Message != test.want {
				t.Fatalf("failure=%v pg=%v", err, pgErr)
			}
			if completionResultCount(t, f) != 0 {
				t.Fatal("result fact survived failed three-fact transaction")
			}
			var outboxCount int
			if err := f.store.pool.QueryRow(f.ctx, `SELECT count(*) FROM outbox_message WHERE tenant_id=$1 AND outbox_id=$2`, f.tenant.TenantID, f.request.Outbox.ID).Scan(&outboxCount); err != nil {
				t.Fatal(err)
			}
			if outboxCount != 0 {
				t.Fatal("outbox fact survived failed three-fact transaction")
			}
			status, _, _ := completionQueueStatus(t, f)
			if status != "in_flight" {
				t.Fatalf("queue status after rollback=%s", status)
			}
		})
	}
}

func TestPostgresAtomicCompletionDuplicateAndResultOnlyReconciliation(t *testing.T) {
	t.Run("duplicate after lease release", func(t *testing.T) {
		f := newAtomicCompletionFixture(t, time.Second, 2*time.Second)
		if err := f.coordinator.CommitResultAndAck(f.ctx, f.request); err != nil {
			t.Fatal(err)
		}
		if err := f.store.Release(f.ctx, f.tenant, f.lease); err != nil {
			t.Fatal(err)
		}
		if err := f.coordinator.CommitResultAndAck(f.ctx, f.request); err != nil {
			t.Fatalf("duplicate completion=%v", err)
		}
		if count := completionResultCount(t, f); count != 1 {
			t.Fatalf("result rows=%d", count)
		}
	})

	t.Run("existing result repairs active delivery", func(t *testing.T) {
		f := newAtomicCompletionFixture(t, time.Second, 2*time.Second)
		if err := f.repository.CommitExecution(f.ctx, f.request.Commit); err != nil {
			t.Fatal(err)
		}
		if err := f.coordinator.CommitResultAndAck(f.ctx, f.request); err != nil {
			t.Fatalf("result-only reconciliation=%v", err)
		}
		status, _, lastDelivery := completionQueueStatus(t, f)
		if status != "acked" || lastDelivery != f.delivery.DeliveryID {
			t.Fatalf("reconciled queue status=%s last=%s", status, lastDelivery)
		}
	})

	t.Run("ack without result is rejected", func(t *testing.T) {
		f := newAtomicCompletionFixture(t, time.Second, 2*time.Second)
		if err := f.queue.Ack(f.ctx, f.delivery); err != nil {
			t.Fatal(err)
		}
		if err := f.coordinator.CommitResultAndAck(f.ctx, f.request); !errors.Is(err, storage.ErrConflict) {
			t.Fatalf("ack-only error=%v", err)
		}
		if count := completionResultCount(t, f); count != 0 {
			t.Fatalf("ack-only created result rows=%d", count)
		}
	})
}

func TestPostgresAtomicCompletionRejectsLegacyPartialForThreeFactRequest(t *testing.T) {
	f := newAtomicCompletionFixture(t, time.Second, 2*time.Second)
	if err := f.coordinator.CommitResultAndAck(f.ctx, f.request); err != nil {
		t.Fatal(err)
	}
	f.request = atomicCompletionRequestWithOutbox(f.delivery, f.lease, `{"text":"atomic"}`)
	if err := f.coordinator.CommitResultAndAck(f.ctx, f.request); !errors.Is(err, storage.ErrCompletionPartial) || !errors.Is(err, storage.ErrConflict) {
		t.Fatalf("legacy result+ack without outbox error=%v", err)
	}
	if completionResultCount(t, f) != 1 {
		t.Fatal("partial compatibility check changed the existing result")
	}
	status, _, _ := completionQueueStatus(t, f)
	if status != "acked" {
		t.Fatalf("partial compatibility queue status=%s", status)
	}
}

func TestPostgresAtomicCompletionIndependentPoolsHaveOneDurableCompletion(t *testing.T) {
	f := newAtomicCompletionFixture(t, time.Second, 2*time.Second)
	f.request = atomicCompletionRequestWithOutbox(f.delivery, f.lease, `{"text":"competing"}`)
	schema := f.store.pool.Config().ConnConfig.RuntimeParams["search_path"]
	secondPool, err := NewPool(f.ctx, PostgresConfig{URL: os.Getenv("TEST_DATABASE_URL"), SearchPath: schema, MaxConns: 2, MinConns: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer secondPool.Close()
	secondCoordinator, err := NewAtomicCompletionCoordinator(secondPool)
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	go func() {
		<-start
		errs <- f.coordinator.CommitResultAndAck(f.ctx, f.request)
	}()
	go func() {
		<-start
		errs <- secondCoordinator.CommitResultAndAck(f.ctx, f.request)
	}()
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatalf("competing completion=%v", err)
		}
	}
	if completionResultCount(t, f) != 1 {
		t.Fatal("competing pools created duplicate execution results")
	}
	var outboxCount int
	if err := f.store.pool.QueryRow(f.ctx, `SELECT count(*) FROM outbox_message WHERE tenant_id=$1 AND aggregate_id=$2`, f.tenant.TenantID, f.job.ExecutionID).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != 1 {
		t.Fatalf("competing pools created %d reply outbox rows", outboxCount)
	}
}

func TestPostgresAtomicCompletionUnknownOutcomeReconcilesThreeFacts(t *testing.T) {
	f := newAtomicCompletionFixture(t, time.Second, 2*time.Second)
	f.request = atomicCompletionRequestWithOutbox(f.delivery, f.lease, `{"text":"unknown"}`)
	f.coordinator.beforeCommit = func(conn *pgxpool.Conn) {
		_ = conn.Conn().PgConn().Close(context.Background())
	}
	err := f.coordinator.CommitResultAndAck(f.ctx, f.request)
	var outcomeErr *CompletionOutcomeError
	if err == nil || !errors.Is(err, storage.ErrCompletionOutcomeUnknown) || !errors.As(err, &outcomeErr) {
		t.Fatalf("unknown three-fact error=%v typed=%v", err, outcomeErr)
	}
	if outcomeErr.Outcome != CompletionAllCommitted && outcomeErr.Outcome != CompletionNeitherCommitted {
		t.Fatalf("unexpected three-fact reconciliation outcome=%s", outcomeErr.Outcome)
	}
	if outcomeErr.Outcome == CompletionAllCommitted {
		f.coordinator.beforeCommit = nil
		if err := f.coordinator.CommitResultAndAck(f.ctx, f.request); err != nil {
			t.Fatalf("idempotent retry after all committed=%v", err)
		}
	}
}

func TestPostgresAtomicCompletionRejectsFencingAndDeliveryBoundaries(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*atomicCompletionFixture)
		want  []error
	}{
		{name: "owner mismatch", setup: func(f *atomicCompletionFixture) { f.request.Commit.OwnerID = "other-owner" }, want: []error{storage.ErrFenceRejected}},
		{name: "epoch mismatch", setup: func(f *atomicCompletionFixture) { f.request.Commit.Epoch++ }, want: []error{storage.ErrEpochRejected}},
		{name: "fence token mismatch", setup: func(f *atomicCompletionFixture) { f.request.Commit.FenceToken++ }, want: []error{storage.ErrFenceRejected}},
		{name: "delivery id mismatch", setup: func(f *atomicCompletionFixture) { f.request.Delivery.DeliveryID = "old-delivery" }, want: []error{storage.ErrDeliveryFinished}},
		{name: "job mismatch", setup: func(f *atomicCompletionFixture) { f.request.Commit.JobID = "other-job" }, want: []error{storage.ErrInvalidDelivery}},
		{name: "execution mismatch", setup: func(f *atomicCompletionFixture) { f.request.Commit.ExecutionID = "other-execution" }, want: []error{storage.ErrInvalidDelivery}},
		{name: "tenant mismatch", setup: func(f *atomicCompletionFixture) { f.request.Commit.TenantID = "other-tenant" }, want: []error{storage.ErrTenantMismatch}},
		{name: "session mismatch", setup: func(f *atomicCompletionFixture) { f.request.Commit.SessionID = "other-session" }, want: []error{storage.ErrInvalidDelivery}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newAtomicCompletionFixture(t, time.Second, 2*time.Second)
			test.setup(f)
			err := f.coordinator.CommitResultAndAck(f.ctx, f.request)
			matched := false
			for _, want := range test.want {
				matched = matched || errors.Is(err, want)
			}
			if !matched {
				t.Fatalf("error=%v", err)
			}
			if count := completionResultCount(t, f); count != 0 {
				t.Fatalf("rejected completion result rows=%d", count)
			}
			status, _, _ := completionQueueStatus(t, f)
			if status != "in_flight" {
				t.Fatalf("rejected completion queue status=%s", status)
			}
		})
	}

	t.Run("expired delivery", func(t *testing.T) {
		f := newAtomicCompletionFixture(t, 35*time.Millisecond, 2*time.Second)
		waitForQueueDeliveryExpiry(t, f)
		if err := f.coordinator.CommitResultAndAck(f.ctx, f.request); !errors.Is(err, storage.ErrDeliveryExpired) {
			t.Fatalf("expired delivery error=%v", err)
		}
		if count := completionResultCount(t, f); count != 0 {
			t.Fatalf("expired delivery result rows=%d", count)
		}
	})

	t.Run("discarded delivery", func(t *testing.T) {
		f := newAtomicCompletionFixture(t, time.Second, 2*time.Second)
		if err := f.queue.Nack(f.ctx, f.delivery, queue.NackOptions{Reason: "discard"}); err != nil {
			t.Fatal(err)
		}
		if err := f.coordinator.CommitResultAndAck(f.ctx, f.request); !errors.Is(err, storage.ErrDeliveryFinished) {
			t.Fatalf("discarded delivery error=%v", err)
		}
		if count := completionResultCount(t, f); count != 0 {
			t.Fatalf("discarded delivery result rows=%d", count)
		}
	})

	t.Run("old delivery token after reclaim", func(t *testing.T) {
		f := newAtomicCompletionFixture(t, 35*time.Millisecond, 2*time.Second)
		waitForQueueDeliveryExpiry(t, f)
		newDelivery, err := f.queue.Receive(f.ctx, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if newDelivery.DeliveryID == f.delivery.DeliveryID {
			t.Fatalf("reclaimed delivery reused token=%s", newDelivery.DeliveryID)
		}
		if err := f.coordinator.CommitResultAndAck(f.ctx, f.request); !errors.Is(err, storage.ErrDeliveryFinished) {
			t.Fatalf("old delivery token error=%v", err)
		}
		if count := completionResultCount(t, f); count != 0 {
			t.Fatalf("old token created result rows=%d", count)
		}
		newRequest := atomicCompletionRequest(newDelivery, f.lease, `{"text":"reclaimed"}`)
		if err := f.coordinator.CommitResultAndAck(f.ctx, newRequest); err != nil {
			t.Fatalf("new delivery completion=%v", err)
		}
	})
}

func TestPostgresAtomicCompletionSQLFailuresRollbackBothResources(t *testing.T) {
	t.Run("result write failure", func(t *testing.T) {
		f := newAtomicCompletionFixture(t, 2*time.Second, 2*time.Second)
		installCompletionResultFailure(t, f)
		err := f.coordinator.CommitResultAndAck(f.ctx, f.request)
		var pgErr *pgconn.PgError
		if err == nil || !errors.As(err, &pgErr) || pgErr.Message != "injected result write failure" {
			t.Fatalf("result failure=%v pg=%v", err, pgErr)
		}
		if count := completionResultCount(t, f); count != 0 {
			t.Fatalf("result failure rows=%d", count)
		}
		status, _, _ := completionQueueStatus(t, f)
		if status != "in_flight" {
			t.Fatalf("result failure queue status=%s", status)
		}
		expireQueueDelivery(t, f)
		second, err := f.queue.Receive(f.ctx, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if second.DeliveryID == f.delivery.DeliveryID {
			t.Fatalf("recovery reused delivery token=%s", second.DeliveryID)
		}
	})

	t.Run("ack update failure", func(t *testing.T) {
		f := newAtomicCompletionFixture(t, 2*time.Second, 2*time.Second)
		installCompletionAckFailure(t, f)
		err := f.coordinator.CommitResultAndAck(f.ctx, f.request)
		var pgErr *pgconn.PgError
		if err == nil || !errors.As(err, &pgErr) || pgErr.Message != "injected ack update failure" {
			t.Fatalf("ack failure=%v pg=%v", err, pgErr)
		}
		if count := completionResultCount(t, f); count != 0 {
			t.Fatalf("ack failure left result rows=%d", count)
		}
		status, _, _ := completionQueueStatus(t, f)
		if status != "in_flight" {
			t.Fatalf("ack failure queue status=%s", status)
		}
		expireQueueDelivery(t, f)
		second, err := f.queue.Receive(f.ctx, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if second.DeliveryID == f.delivery.DeliveryID {
			t.Fatalf("recovery reused delivery token=%s", second.DeliveryID)
		}
	})
}

func TestPostgresAtomicCompletionTakeoverAndConcurrentWinner(t *testing.T) {
	t.Run("takeover before transaction", func(t *testing.T) {
		f := newAtomicCompletionFixture(t, time.Second, 35*time.Millisecond)
		waitForLeaseExpiry(t, f)
		newLease, err := f.store.Acquire(f.ctx, f.tenant, f.sessionID, "new-completion-owner", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		oldErr := f.coordinator.CommitResultAndAck(f.ctx, f.request)
		if !errors.Is(oldErr, storage.ErrFenceRejected) && !errors.Is(oldErr, storage.ErrEpochRejected) && !errors.Is(oldErr, storage.ErrLeaseLost) {
			t.Fatalf("old completion error=%v", oldErr)
		}
		newRequest := atomicCompletionRequest(f.delivery, newLease, `{"text":"new-owner"}`)
		if err := f.coordinator.CommitResultAndAck(f.ctx, newRequest); err != nil {
			t.Fatal(err)
		}
		result, err := f.repository.GetExecutionResult(f.ctx, tenant.TenantContext{TenantID: f.tenant.TenantID}, f.job.JobID, f.job.ExecutionID)
		if err != nil {
			t.Fatal(err)
		}
		if result.OwnerID != newLease.OwnerID || !equalJSON(result.ResultJSON, newRequest.Commit.ResultJSON) {
			t.Fatalf("takeover result=%+v", result)
		}
	})

	t.Run("concurrent old and new owner", func(t *testing.T) {
		f := newAtomicCompletionFixture(t, time.Second, 35*time.Millisecond)
		waitForLeaseExpiry(t, f)
		newLease, err := f.store.Acquire(f.ctx, f.tenant, f.sessionID, "new-concurrent-owner", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		newRequest := atomicCompletionRequest(f.delivery, newLease, `{"text":"new-winner"}`)
		start := make(chan struct{})
		results := make(chan error, 2)
		var wg sync.WaitGroup
		for _, request := range []storage.AtomicCompletionRequest{f.request, newRequest} {
			wg.Add(1)
			go func(request storage.AtomicCompletionRequest) {
				defer wg.Done()
				<-start
				results <- f.coordinator.CommitResultAndAck(f.ctx, request)
			}(request)
		}
		close(start)
		wg.Wait()
		close(results)
		successes := 0
		for err := range results {
			if err == nil {
				successes++
				continue
			}
			if !errors.Is(err, storage.ErrFenceRejected) && !errors.Is(err, storage.ErrEpochRejected) && !errors.Is(err, storage.ErrLeaseLost) && !errors.Is(err, storage.ErrDeliveryFinished) && !errors.Is(err, storage.ErrConflict) {
				t.Fatalf("concurrent completion error=%v", err)
			}
		}
		if successes != 1 {
			t.Fatalf("concurrent completion successes=%d", successes)
		}
		result, err := f.repository.GetExecutionResult(f.ctx, tenant.TenantContext{TenantID: f.tenant.TenantID}, f.job.JobID, f.job.ExecutionID)
		if err != nil {
			t.Fatal(err)
		}
		if result.OwnerID != newLease.OwnerID || !equalJSON(result.ResultJSON, newRequest.Commit.ResultJSON) || completionResultCount(t, f) != 1 {
			t.Fatalf("concurrent winner=%+v rows=%d", result, completionResultCount(t, f))
		}
		status, _, lastDelivery := completionQueueStatus(t, f)
		if status != "acked" || lastDelivery != f.delivery.DeliveryID {
			t.Fatalf("concurrent queue status=%s last=%s", status, lastDelivery)
		}
	})
}

func TestPostgresAtomicCompletionUnknownCommitOutcomeIsReported(t *testing.T) {
	f := newAtomicCompletionFixture(t, time.Second, 2*time.Second)
	f.coordinator.beforeCommit = func(conn *pgxpool.Conn) {
		_ = conn.Conn().PgConn().Close(context.Background())
	}
	err := f.coordinator.CommitResultAndAck(f.ctx, f.request)
	var outcomeErr *CompletionOutcomeError
	if err == nil || !errors.Is(err, storage.ErrCompletionOutcomeUnknown) || !errors.As(err, &outcomeErr) {
		t.Fatalf("commit outcome error=%v typed=%v", err, outcomeErr)
	}
	if outcomeErr.Outcome == CompletionOutcomeUnknown {
		t.Fatalf("commit outcome was not reconciled: %v", outcomeErr)
	}
	status, _, _ := completionQueueStatus(t, f)
	if status == "discarded" {
		t.Fatalf("unknown commit outcome discarded delivery")
	}
	if outcomeErr.Outcome == CompletionBothCommitted {
		f.coordinator.beforeCommit = nil
		if err := f.coordinator.CommitResultAndAck(f.ctx, f.request); err != nil {
			t.Fatalf("idempotent retry after both committed=%v", err)
		}
	}
}
