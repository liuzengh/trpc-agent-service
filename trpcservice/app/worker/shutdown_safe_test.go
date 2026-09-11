package worker

import (
	"context"
	"testing"
	"time"
)

// TestShutdownSafeSurvivesCancellation pins the fix for what the node-failure
// drill exposed: a rolling restart (SIGTERM) cancels the consumer context while
// turns are in flight, and every accounting write after that point used the
// cancelled context and silently failed. The turns ended up with a durable reply
// in the outbox but no ledger row, no usage rows and an uncommitted idempotency
// marker ("usage metering skipped ... context canceled" in the logs).
//
// A write about work that is already durable must outlive the shutdown that
// interrupted it, while still carrying the turn's context values (tenant,
// policy, trace) and staying bounded.
func TestShutdownSafeSurvivesCancellation(t *testing.T) {
	type ctxKey struct{}
	parent, cancel := context.WithCancel(context.WithValue(context.Background(), ctxKey{}, "t-demo"))
	cancel() // the shutdown already happened

	ctx, stop := shutdownSafe(parent)
	defer stop()

	if err := ctx.Err(); err != nil {
		t.Errorf("detached context is already done (%v), the write would be skipped", err)
	}
	if got := ctx.Value(ctxKey{}); got != "t-demo" {
		t.Errorf("context value = %v, want the parent's value kept", got)
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("detached context has no deadline: a dead database would hold the process open")
	}
	if left := time.Until(deadline); left <= 0 || left > shutdownWriteTimeout {
		t.Errorf("deadline in %v, want it within (0, %v]", left, shutdownWriteTimeout)
	}
}
