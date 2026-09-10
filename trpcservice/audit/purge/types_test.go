package purge

import "testing"

func TestTransitionOKCoversStateMachine(t *testing.T) {
	allowed := []struct{ from, to State }{
		{StatePlanned, StateApproved},
		{StateApproved, StateExecuting},
		{StateExecuting, StateCompleted},
		{StateExecuting, StateFailed},
		{StateExecuting, StateQuarantined},
		{StateFailed, StateExecuting},
		{StateFailed, StateQuarantined},
	}
	for _, pair := range allowed {
		if !TransitionOK(pair.from, pair.to) {
			t.Fatalf("transition %s -> %s should be allowed", pair.from, pair.to)
		}
	}
	denied := []struct{ from, to State }{
		{StatePlanned, StateExecuting},
		{StatePlanned, StateCompleted},
		{StateApproved, StateCompleted},
		{StateCompleted, StateExecuting},
		{StateQuarantined, StateExecuting},
		{StateExecuting, StateApproved},
	}
	for _, pair := range denied {
		if TransitionOK(pair.from, pair.to) {
			t.Fatalf("transition %s -> %s should be denied", pair.from, pair.to)
		}
	}
	// Same-state idempotent updates are allowed only for non-terminal states.
	for _, state := range []State{StatePlanned, StateApproved, StateExecuting, StateFailed} {
		if !TransitionOK(state, state) {
			t.Fatalf("same-state %s should be allowed", state)
		}
	}
	for _, state := range []State{StateCompleted, StateQuarantined} {
		if TransitionOK(state, state) {
			t.Fatalf("same-state %s should be denied", state)
		}
	}
}
