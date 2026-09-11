package tool

import (
	"context"
	"errors"
	"testing"
	"time"
)

func toolExecutionRequest(callID string) ExecutionRequest {
	return ExecutionRequest{
		TenantID: "tenant-a", RequestID: "request-1", ToolCallID: callID,
		ToolName: "refund", Arguments: []byte(`{"order_id":"42"}`), TraceID: "trace-1", LeaseTTL: time.Minute,
	}
}

func TestMemoryExecutionLedgerReplaysCompletedIntentAcrossDifferentToolCallIDs(t *testing.T) {
	ledger := NewMemoryExecutionLedger()
	first, err := ledger.Begin(context.Background(), toolExecutionRequest("call-a"))
	if err != nil || !first.Created {
		t.Fatalf("first Begin() = %+v, %v", first, err)
	}
	if err := ledger.Complete(context.Background(), "tenant-a", first.IdempotencyKey, map[string]any{"ok": true}); err != nil {
		t.Fatal(err)
	}
	replay, err := ledger.Begin(context.Background(), toolExecutionRequest("call-b"))
	if err != nil {
		t.Fatal(err)
	}
	if replay.Created || replay.Status != ExecutionCompleted {
		t.Fatalf("replay = %+v, want completed reuse", replay)
	}
	result, ok := replay.Result.(map[string]any)
	if !ok || result["ok"] != true {
		t.Fatalf("replayed result = %#v", replay.Result)
	}
}

func TestMemoryExecutionLedgerTurnsExpiredRunningIntoOutcomeUnknown(t *testing.T) {
	ledger := NewMemoryExecutionLedger()
	now := time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)
	ledger.now = func() time.Time { return now }
	first, err := ledger.Begin(context.Background(), toolExecutionRequest("call-a"))
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	decision, err := ledger.Begin(context.Background(), toolExecutionRequest("call-b"))
	if err != nil {
		t.Fatal(err)
	}
	if decision.Status != ExecutionOutcomeUnknown || decision.Created {
		t.Fatalf("expired decision = %+v, want terminal outcome_unknown", decision)
	}
	if err := ledger.Complete(context.Background(), "tenant-a", first.IdempotencyKey, "late"); err == nil {
		t.Fatal("late Complete() error = nil, want rejected terminal unknown")
	}
}

func TestMemoryExecutionLedgerValidatesRequestsAndContext(t *testing.T) {
	ledger := NewMemoryExecutionLedger()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ledger.Begin(canceled, toolExecutionRequest("call-a")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Begin(canceled) error = %v", err)
	}

	request := toolExecutionRequest("call-a")
	request.LeaseTTL = 0
	if _, err := ledger.Begin(context.Background(), request); err == nil {
		t.Fatal("Begin() accepted non-positive lease TTL")
	}

	first, err := ledger.Begin(context.Background(), toolExecutionRequest("call-a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.Complete(canceled, "tenant-a", first.IdempotencyKey, "result"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Complete(canceled) error = %v", err)
	}
}

func TestMemoryExecutionLedgerReusesRunningIntentWithoutCreatingDuplicate(t *testing.T) {
	ledger := NewMemoryExecutionLedger()
	first, err := ledger.Begin(context.Background(), toolExecutionRequest("call-a"))
	if err != nil || !first.Created {
		t.Fatalf("first Begin() = %#v, %v", first, err)
	}
	replay, err := ledger.Begin(context.Background(), toolExecutionRequest("call-b"))
	if err != nil {
		t.Fatal(err)
	}
	if replay.Created || replay.Status != ExecutionRunning || replay.IdempotencyKey != first.IdempotencyKey {
		t.Fatalf("running replay = %#v", replay)
	}
}

func TestMemoryExecutionLedgerRestartsFailedIntent(t *testing.T) {
	ledger := NewMemoryExecutionLedger()
	first, err := ledger.Begin(context.Background(), toolExecutionRequest("call-a"))
	if err != nil {
		t.Fatal(err)
	}
	if err := ledger.Fail(context.Background(), "tenant-a", first.IdempotencyKey, "remote_rejected"); err != nil {
		t.Fatal(err)
	}
	restarted, err := ledger.Begin(context.Background(), toolExecutionRequest("call-b"))
	if err != nil {
		t.Fatal(err)
	}
	if !restarted.Created || restarted.Status != ExecutionRunning || restarted.IdempotencyKey != first.IdempotencyKey || restarted.Result != nil {
		t.Fatalf("restarted decision = %#v", restarted)
	}
	if err := ledger.Complete(context.Background(), "tenant-a", restarted.IdempotencyKey, "ok"); err != nil {
		t.Fatal(err)
	}
	if err := ledger.Fail(context.Background(), "tenant-a", restarted.IdempotencyKey, "late"); !errors.Is(err, ErrToolOutcomeUnknown) {
		t.Fatalf("Fail(terminal) error = %v", err)
	}
}
