package gateway

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTerminalFailureDoesNotOverwriteSuccessOrAnotherWorker(t *testing.T) {
	ctx := context.Background()
	journal := NewMemoryJournal()
	defer journal.Close()
	_, err := journal.Accept(ctx, testInboundRequest(t, "terminal-test", "hello"))
	if err != nil {
		t.Fatal(err)
	}
	task := journal.Tasks()[0]
	_ = journal.MarkRunRunning(ctx, task.RequestID, "new-owner")
	if _, err := journal.TerminalFailRun(ctx, task, RunResult{WorkerID: "stale-owner", Reply: "failure"}); !errors.Is(err, ErrRunSuperseded) {
		t.Fatalf("stale terminal: %v", err)
	}
	if err := journal.FailRun(ctx, task.RequestID, "error", nil, "stale-owner"); !errors.Is(err, ErrRunSuperseded) {
		t.Fatalf("stale failure: %v", err)
	}
	if err := journal.CompleteRun(ctx, task, RunResult{WorkerID: "stale-owner", Reply: "stale result"}); !errors.Is(err, ErrRunSuperseded) {
		t.Fatalf("stale completion: %v", err)
	}
	if err := journal.CompleteRun(ctx, task, RunResult{WorkerID: "new-owner", Reply: "success"}); err != nil {
		t.Fatal(err)
	}
	if err := journal.CompleteRun(ctx, task, RunResult{WorkerID: "recovery-owner", Reply: "success"}); err != nil {
		t.Fatalf("completed replay must still repair bookkeeping: %v", err)
	}
	if dead, err := journal.TerminalFailRun(ctx, task, RunResult{WorkerID: "new-owner", Reply: "failure"}); err != nil || dead {
		t.Fatalf("success replaced: %t %v", dead, err)
	}
	items, err := journal.ClaimOutbound(ctx, "sender", 10, time.Minute)
	if err != nil || len(items) != 1 || items[0].Text != "success" {
		t.Fatalf("items=%+v %v", items, err)
	}
}
