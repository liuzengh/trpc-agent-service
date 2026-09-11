package queue

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/cyl6/trpc-agent-service/trpcservice/delivery"
	"github.com/cyl6/trpc-agent-service/trpcservice/worker"
)

type outcomeProcessor struct {
	outcome         delivery.Outcome
	unsafeTaskRetry bool
	calls           atomic.Int32
}

func (p *outcomeProcessor) Process(context.Context, worker.Task) (worker.Result, error) {
	p.calls.Add(1)
	failure := (&delivery.Result{
		Outcome: p.outcome, ErrorType: "test_failure",
	}).Failure()
	if typed, ok := failure.(*delivery.FailureError); ok && p.unsafeTaskRetry {
		typed.RetryWholeTask = false
	}
	return worker.Result{}, fmt.Errorf("send reply: %w", failure)
}

func TestInProcessRetriesOnlyExplicitlySafeDeliveryFailure(t *testing.T) {
	for _, test := range []struct {
		name    string
		outcome delivery.Outcome
		unsafe  bool
		calls   int32
	}{
		{name: "retryable not sent", outcome: delivery.RetryableNotSent, calls: 3},
		{name: "later part retry is not whole-task safe", outcome: delivery.RetryableNotSent, unsafe: true, calls: 1},
		{name: "unknown", outcome: delivery.Unknown, calls: 1},
		{name: "permanent", outcome: delivery.PermanentRejected, calls: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			processor := &outcomeProcessor{outcome: test.outcome, unsafeTaskRetry: test.unsafe}
			q := NewInProcess(processor, 1, 1, nil)
			if err := q.Submit(context.Background(), worker.Task{}); err != nil {
				t.Fatalf("submit: %v", err)
			}
			if err := q.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			if got := processor.calls.Load(); got != test.calls {
				t.Fatalf("calls = %d, want %d", got, test.calls)
			}
		})
	}
}
