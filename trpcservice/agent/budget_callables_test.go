package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

// budgetProbeTool is a minimal CallableTool standing in for any concrete tool a
// Resolver can return — a local function tool or a tenant-scoped MCP tool alike.
type budgetProbeTool struct{}

func (budgetProbeTool) Declaration() *tool.Declaration {
	return &tool.Declaration{Name: "probe", Description: "budget probe"}
}

func (budgetProbeTool) Call(context.Context, []byte) (any, error) { return "ok", nil }

// TestBudgetCallablesEnforcesMaxToolCallsForAnyTool documents the invariant that
// the factory wraps every resolved tool — including an MCP tool — in the same
// per-run circuit breaker, so MaxToolCalls is enforced uniformly rather than
// only for hand-written function tools.
func TestBudgetCallablesEnforcesMaxToolCallsForAnyTool(t *testing.T) {
	ctx := runtime.WithExecutionBudget(context.Background(), runtime.ExecutionBudget{MaxToolCalls: 1})
	wrapped := budgetCallables([]tool.Tool{budgetProbeTool{}})
	if len(wrapped) != 1 {
		t.Fatalf("wrapped=%d", len(wrapped))
	}
	callable, ok := wrapped[0].(tool.CallableTool)
	if !ok {
		t.Fatalf("wrapped tool is not callable: %T", wrapped[0])
	}
	if _, err := callable.Call(ctx, nil); err != nil {
		t.Fatalf("first call should consume the budget: %v", err)
	}
	if _, err := callable.Call(ctx, nil); !errors.Is(err, runtime.ErrExecutionBudgetExceeded) {
		t.Fatalf("second call should exceed MaxToolCalls, got %v", err)
	}
}
