package governance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

func TestToolPermissionDecisionRecorder(t *testing.T) {
	policy := ToolPolicy{AllowedTools: []string{"echo"}}
	var recorded ToolDecision
	runOptions := agentcore.NewRunOptions(RunOptions(
		policy,
		"alice",
		nil,
		func(_ context.Context, decision ToolDecision) error {
			recorded = decision
			return nil
		},
	)...)
	decision, err := runOptions.ToolPermissionPolicy.CheckToolPermission(
		context.Background(),
		&agenttool.PermissionRequest{ToolName: "echo", ToolCallID: "call-1"},
	)
	if err != nil || decision.Action != agenttool.PermissionActionAllow {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
	if recorded.ToolName != "echo" || recorded.ToolCallID != "call-1" ||
		recorded.Action != "allow" {
		t.Fatalf("recorded=%+v", recorded)
	}
}

func TestDangerousApprovalIsBoundToArgumentsHash(t *testing.T) {
	policy := ToolPolicy{
		AllowedTools: []string{"dangerous"}, DangerousTools: []string{"dangerous"},
	}
	approvedArguments := []byte(`{"target":"record-a"}`)
	digest := sha256.Sum256(approvedArguments)
	approved := []ApprovedToolCall{{
		ToolName: "dangerous", ArgumentsHash: hex.EncodeToString(digest[:]),
	}}
	runOptions := agentcore.NewRunOptions(RunOptionsWithApprovals(
		policy, "alice", nil, approved,
	)...)
	decision, err := runOptions.ToolPermissionPolicy.CheckToolPermission(
		context.Background(), &agenttool.PermissionRequest{
			ToolName: "dangerous", Arguments: approvedArguments,
		},
	)
	if err != nil || decision.Action != agenttool.PermissionActionAllow {
		t.Fatalf("approved decision=%+v err=%v", decision, err)
	}
	decision, err = runOptions.ToolPermissionPolicy.CheckToolPermission(
		context.Background(), &agenttool.PermissionRequest{
			ToolName: "dangerous", Arguments: []byte(`{"target":"record-b"}`),
		},
	)
	if err != nil || decision.Action != agenttool.PermissionActionAsk {
		t.Fatalf("changed arguments decision=%+v err=%v", decision, err)
	}
}
