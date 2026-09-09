package worker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/persistence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/sessionfence"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type retryingPersistenceExecutor struct {
	mu           sync.Mutex
	agentCalls   int
	persistCalls int
	sqlWrites    int
	committed    string
}

func (*retryingPersistenceExecutor) Ready(context.Context) error { return nil }
func (*retryingPersistenceExecutor) Execute(context.Context, message.ExecutionTask) (message.OutboundMessage, error) {
	return message.OutboundMessage{}, nil
}
func (e *retryingPersistenceExecutor) ExecuteFenced(_ context.Context, task message.ExecutionTask, fence sessionfence.Fence) (message.OutboundMessage, sessionfence.TurnCommit, error) {
	e.mu.Lock()
	e.agentCalls++
	e.mu.Unlock()
	return message.OutboundMessage{RequestID: task.RequestID, TraceID: task.TraceID, SessionID: task.SessionID, Text: "answer"}, sessionfence.TurnCommit{
		SessionCoord: fence.SessionCoord, SessionSeq: fence.SessionSeq, AppName: tenant.AppName(task.TenantID, task.AgentAppID),
		UserID: task.RunnerUserID, SessionID: task.SessionID, FinalState: map[string][]byte{},
	}, nil
}
func (*retryingPersistenceExecutor) PersistenceRoute(_ context.Context, task message.ExecutionTask) (persistence.Route, error) {
	return persistence.Route{TenantID: task.TenantID, AgentAppID: task.AgentAppID, Fingerprint: persistence.BackendFingerprint{
		SchemaVersion: persistence.FingerprintSchemaVersion, Kind: tenant.StorageKindPostgres, StorageProfileID: "pg",
		DatabaseIdentity: "db.example:5432/app", Namespace: "public.tenant_",
	}}, nil
}
func (e *retryingPersistenceExecutor) Persist(_ context.Context, envelope persistence.Envelope) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.persistCalls++
	if e.persistCalls == 1 {
		e.sqlWrites++
		e.committed = envelope.EnvelopeDigest
		return persistence.ErrBackendUnavailable
	}
	if e.committed != envelope.EnvelopeDigest {
		return persistence.ErrCommitDigestConflict
	}
	return nil
}

func TestPersistenceRetryDoesNotExecuteAgentTwice(t *testing.T) {
	store, server, _ := newStrongWorkerStore(t, time.Second)
	if err := store.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	exec := &retryingPersistenceExecutor{}
	service, err := New(store, exec, "sql-worker")
	if err != nil {
		t.Fatal(err)
	}
	task := workerTask("sql-retry")
	if _, _, err := store.Submit(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	delivery, err := store.ReadTask(context.Background(), "sql-worker", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	service.process(context.Background(), delivery)
	snapshot, err := store.Snapshot(context.Background(), task.InboxID())
	if err != nil || snapshot.State != messaging.StatePersistRetryWait || snapshot.Attempt != task.Attempt || snapshot.PersistAttempt != 2 {
		t.Fatalf("first persistence attempt snapshot = (%#v,%v)", snapshot, err)
	}
	if _, err := store.ReadReply(context.Background(), "gateway-before-sql", time.Millisecond); err == nil {
		t.Fatal("SQL task replied before Redis finalize")
	}
	server.SetTime(time.Now().Add(time.Minute))
	if promoted, err := store.PromotePersistenceRetries(context.Background(), 10); err != nil || promoted != 1 {
		t.Fatalf("promote persistence retry = (%d,%v)", promoted, err)
	}
	replayed, err := store.ReadTask(context.Background(), "sql-worker", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	service.process(context.Background(), replayed)
	final, err := store.Snapshot(context.Background(), task.InboxID())
	if err != nil || final.State != messaging.StateSucceeded {
		t.Fatalf("final snapshot = (%#v,%v)", final, err)
	}
	exec.mu.Lock()
	defer exec.mu.Unlock()
	if exec.agentCalls != 1 || exec.persistCalls != 2 || exec.sqlWrites != 1 {
		t.Fatalf("calls agent=%d persist=%d SQL writes=%d", exec.agentCalls, exec.persistCalls, exec.sqlWrites)
	}
}

func TestSQLFailuresUseStableCodesBeforeAndAfterEnvelope(t *testing.T) {
	if code, retryable := classify(persistence.ErrBackendUnavailable); code != "sql_unavailable" || !retryable {
		t.Fatalf("pre-Agent SQL unavailable classification = (%q,%v)", code, retryable)
	}
	if code, retryable := classify(persistence.ErrSchemaIncompatible); code != "sql_schema_incompatible" || retryable {
		t.Fatalf("pre-Agent SQL schema classification = (%q,%v)", code, retryable)
	}
	for input, want := range map[error]string{
		persistence.ErrCommitDigestConflict:    "sql_commit_digest_conflict",
		persistence.ErrSessionSequenceConflict: "sql_session_sequence_conflict",
		persistence.ErrInvalidEnvelope:         "persistence_envelope_invalid",
	} {
		if code, retryable := classifyPersistence(fmt.Errorf("wrapped: %w", input)); code != want || retryable {
			t.Fatalf("persistence classification for %v = (%q,%v)", input, code, retryable)
		}
	}
}
