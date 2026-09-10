package tool

import (
	"context"
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
