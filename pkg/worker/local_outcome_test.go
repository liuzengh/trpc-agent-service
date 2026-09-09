package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/governance"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestLocalClientMarksFailuresAfterRunnerEntryAsUnknown(t *testing.T) {
	for _, test := range []struct {
		name   string
		runner *scriptedRunner
	}{
		{name: "partial terminal stream", runner: &scriptedRunner{events: []*event.Event{
			responseEvent("partial result"), event.NewErrorEvent("invocation", "agent", "tool_error", "test-only failure"),
		}}},
		{name: "incomplete stream", runner: &scriptedRunner{events: []*event.Event{responseEvent("partial result")}}},
		{name: "synchronous cancellation", runner: &scriptedRunner{runErr: context.Canceled}},
		{name: "nested preflight failure", runner: &scriptedRunner{runErr: ErrExecutionPreflight}},
		{name: "nested approval without invocation capability", runner: &scriptedRunner{runErr: &governance.ApprovalRequiredError{Challenge: governance.ApprovalChallenge{ExpiresAt: time.Now().Add(time.Minute)}}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			value := newBudgetProcessWorker(t, test.runner, &recordingBudgetController{})
			response, err := NewLocalClient(value).ProcessMessage(context.Background(), &Request{
				UserID: "alice", SessionID: "session-1", IdempotencyKey: "inbox:1", Content: "hello",
			})
			if response != nil || !errors.Is(err, ErrWorkerExecutionOutcomeUnknown) || test.runner.runs != 1 {
				t.Fatalf("post-Runner failure is retryable to Consumer: response=%v error=%v runs=%d", response, err, test.runner.runs)
			}
		})
	}
}

func TestLocalClientPreservesPreflightAndApprovalRetryPolicy(t *testing.T) {
	t.Run("transient preflight", func(t *testing.T) {
		r := &scriptedRunner{}
		value := newBudgetProcessWorker(t, r, &recordingBudgetController{checkErr: errors.New("temporary budget failure")})
		_, err := NewLocalClient(value).ProcessMessage(context.Background(), &Request{UserID: "alice", SessionID: "session-1", Content: "hello"})
		if !errors.Is(err, ErrExecutionPreflight) || errors.Is(err, ErrWorkerExecutionOutcomeUnknown) || r.runs != 0 {
			t.Fatalf("preflight retry policy changed: error=%v runs=%d", err, r.runs)
		}
	})
	t.Run("approval pause", func(t *testing.T) {
		r := &scriptedRunner{runErr: &governance.ApprovalRequiredError{Challenge: governance.ApprovalChallenge{ExpiresAt: time.Now().Add(time.Minute)}}}
		value := newBudgetProcessWorker(t, r, &recordingBudgetController{})
		value.runner = approvalCapabilityRunner{scriptedRunner: r}
		_, err := NewLocalClient(value).ProcessMessage(context.Background(), &Request{UserID: "alice", SessionID: "session-1", Content: "hello"})
		if _, paused := AsApprovalPause(err); !paused || errors.Is(err, ErrWorkerExecutionOutcomeUnknown) || r.runs != 1 {
			t.Fatalf("approval waiting policy changed: error=%v runs=%d", err, r.runs)
		}
	})
}

type approvalCapabilityRunner struct{ *scriptedRunner }

func (r approvalCapabilityRunner) Run(ctx context.Context, userID, sessionID string, message model.Message, options ...agent.RunOption) (<-chan *event.Event, error) {
	state, ok := governance.ApprovalCapabilityFromContext(ctx)
	if !ok {
		return nil, errors.New("missing invocation approval capability")
	}
	state.SetChallenge(r.runErr.(*governance.ApprovalRequiredError).Challenge)
	return r.scriptedRunner.Run(ctx, userID, sessionID, message, options...)
}

func TestAsApprovalPauseRejectsUnknownExecutionWithNestedApproval(t *testing.T) {
	err := errors.Join(ErrWorkerExecutionOutcomeUnknown, &governance.ApprovalRequiredError{Challenge: governance.ApprovalChallenge{ExpiresAt: time.Now().Add(time.Minute)}})
	if _, paused := AsApprovalPause(err); paused {
		t.Fatal("nested approval converted an unknown execution outcome into a resumable pause")
	}
}
