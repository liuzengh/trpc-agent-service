package governance

import (
	"context"
	"strings"
	"testing"

	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestToolBudgetCountsAndStructuredFeedback(t *testing.T) {
	ctx, feedback := WithFeedback(context.Background())
	var decisions []ToolDecision
	policy := ToolPolicy{AllowedTools: []string{"echo"}, MaxToolCalls: 1}
	options := agentcore.NewRunOptions(RunOptions(policy, "alice", nil, func(_ context.Context, d ToolDecision) error { decisions = append(decisions, d); return nil })...)
	for _, name := range []string{"not_allowed", "echo", "echo"} {
		if _, err := options.ToolPermissionPolicy.CheckToolPermission(ctx, &agenttool.PermissionRequest{ToolName: name}); err != nil {
			t.Fatal(err)
		}
	}
	if decisions[0].CallsUsed != 0 || decisions[1].CallsUsed != 1 || decisions[2].CallsUsed != 2 || decisions[2].CallLimit != 1 || decisions[2].Code != CodeToolBudgetExceeded {
		t.Fatal(decisions)
	}
	if reply := feedback.Reply("模型说等待恢复后重试", "req-fixture"); !strings.Contains(reply, "req-fixture") || !strings.Contains(reply, CodeToolBudgetExceeded) || strings.Contains(reply, "模型说") {
		t.Fatal(reply)
	}
	// The policy and counter are new for every Run, not a replenishing quota.
	next := agentcore.NewRunOptions(RunOptions(policy, "alice", nil)...)
	d, err := next.ToolPermissionPolicy.CheckToolPermission(context.Background(), &agenttool.PermissionRequest{ToolName: "echo"})
	if err != nil || d.Action != agenttool.PermissionActionAllow {
		t.Fatal(d, err)
	}
}

func TestFeedbackDoesNotInventUnknownOutcome(t *testing.T) {
	ctx, feedback := WithFeedback(context.Background())
	RecordToolFailure(ctx, "tool", "sandbox_execution_failed")
	if got := feedback.Reply("original", "req"); got != "original" {
		t.Fatal("unknown execution changed into known failure", got)
	}
	RecordToolFailure(ctx, "skill_run", CodeSandboxUnavailable)
	if got := feedback.Reply("original", "req"); !strings.Contains(got, CodeSandboxUnavailable) {
		t.Fatal(got)
	}
}
