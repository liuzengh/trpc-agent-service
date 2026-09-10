package runtime

import (
	"context"
	"errors"
	"testing"
)

func TestExecutionBudgetCountsAndBoundsParallelTools(t *testing.T) {
	ctx := WithExecutionBudget(context.Background(), ExecutionBudget{MaxLLMCalls: 1})
	if err := ConsumeLLMCall(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ConsumeLLMCall(ctx); !errors.Is(err, ErrExecutionBudgetExceeded) || ExecutionBudgetReason(err) != "max_llm_calls" {
		t.Fatalf("llm limit err=%v", err)
	}
	if violation := ExecutionBudgetViolation(ctx); !errors.Is(violation, ErrExecutionBudgetExceeded) || ExecutionBudgetReason(violation) != "max_llm_calls" {
		t.Fatalf("first breach not retained: %v", violation)
	}

	ctx = WithExecutionBudget(context.Background(), ExecutionBudget{MaxToolCalls: 2, MaxParallelTools: 1})
	release, err := BeginToolCall(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := BeginToolCall(ctx); !errors.Is(err, ErrExecutionBudgetExceeded) || ExecutionBudgetReason(err) != "max_parallel_tools" {
		t.Fatalf("parallel limit err=%v", err)
	}
	release()
	release, err = BeginToolCall(ctx)
	if err != nil {
		t.Fatalf("released parallel slot was not reusable: %v", err)
	}
	release()
}
