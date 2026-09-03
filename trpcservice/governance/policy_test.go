package governance

import (
	"context"
	"encoding/json"
	"testing"

	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestToolPermissionPolicy(t *testing.T) {
	policy, err := ParseToolPolicy(json.RawMessage(`{
        "allowed_tools":["echo","dangerous_demo"],
        "dangerous_tools":["dangerous_demo"],
        "max_tool_calls":2,
        "max_run_duration":"30s"
    }`))
	if err != nil {
		t.Fatalf("parse policy: %v", err)
	}
	runOptions := agentcore.NewRunOptions(RunOptions(policy, "alice", nil)...)
	decision, err := runOptions.ToolPermissionPolicy.CheckToolPermission(
		context.Background(),
		&agenttool.PermissionRequest{ToolName: "dangerous_demo"},
	)
	if err != nil || decision.Action != agenttool.PermissionActionAsk {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
	decision, _ = runOptions.ToolPermissionPolicy.CheckToolPermission(
		context.Background(),
		&agenttool.PermissionRequest{ToolName: "unknown"},
	)
	if decision.Action != agenttool.PermissionActionDeny {
		t.Fatalf("unknown decision=%+v", decision)
	}
}

func TestApprovedDangerousTool(t *testing.T) {
	policy := ToolPolicy{
		AllowedTools: []string{"dangerous_demo"}, DangerousTools: []string{"dangerous_demo"},
	}
	runOptions := agentcore.NewRunOptions(RunOptions(policy, "alice", []string{"dangerous_demo"})...)
	decision, _ := runOptions.ToolPermissionPolicy.CheckToolPermission(
		context.Background(), &agenttool.PermissionRequest{ToolName: "dangerous_demo"},
	)
	if decision.Action != agenttool.PermissionActionAllow {
		t.Fatalf("decision=%+v", decision)
	}
}
