package governance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"sync"
	"testing"

	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

func TestToolAccessFiltersVisibilityAndDeniesEvenApprovedCalls(t *testing.T) {
	policy := ToolPolicy{AllowedTools: []string{"echo", "write"}, DangerousTools: []string{"write"}, ToolAllowedUsers: map[string][]string{"write": {"alice"}}, DirectOnlyTools: []string{"write"}}
	args := []byte(`{"value":"test"}`)
	digest := sha256.Sum256(args)
	for _, tc := range []struct {
		user, audience string
		approve        bool
		want           agenttool.PermissionAction
		visible        bool
	}{
		{"alice", "direct", false, agenttool.PermissionActionAsk, true},
		{"alice", "direct", true, agenttool.PermissionActionAllow, true},
		{"bob", "direct", true, agenttool.PermissionActionDeny, false},
		{"alice", "group", true, agenttool.PermissionActionDeny, false},
		{"alice", "", true, agenttool.PermissionActionDeny, false},
		{"", "direct", true, agenttool.PermissionActionDeny, false},
	} {
		var approved []ApprovedToolCall
		if tc.approve {
			approved = []ApprovedToolCall{{ToolName: "write", ArgumentsHash: hex.EncodeToString(digest[:])}}
		}
		run := agentcore.NewRunOptions(RunOptionsForCaller(policy, Caller{UserID: tc.user, ChatType: tc.audience}, nil, approved)...)
		tool := function.NewFunctionTool(func(context.Context, struct{}) (string, error) { return "unused", nil }, function.WithName("write"))
		if run.ToolFilter(context.Background(), tool) != tc.visible {
			t.Fatal("restricted tool visibility differs")
		}
		decision, err := run.ToolPermissionPolicy.CheckToolPermission(context.Background(), &agenttool.PermissionRequest{ToolName: "write", Arguments: args})
		if err != nil || decision.Action != tc.want {
			t.Fatalf("user=%q audience=%q action=%s err=%v", tc.user, tc.audience, decision.Action, err)
		}
	}
	legacy := agentcore.NewRunOptions(RunOptionsWithApprovals(policy, "alice", []string{"write"}, nil)...)
	decision, _ := legacy.ToolPermissionPolicy.CheckToolPermission(context.Background(), &agenttool.PermissionRequest{ToolName: "write", Arguments: args})
	if decision.Action != agenttool.PermissionActionDeny {
		t.Fatal("legacy caller invented a direct audience")
	}
}

func TestToolAccessCannotMutateSharedPolicyOrUseWhitespaceBypass(t *testing.T) {
	policy := ToolPolicy{AllowedTools: []string{" echo ", " write "}, ToolAllowedUsers: map[string][]string{"write": {"alice"}}, DirectOnlyTools: []string{"write"}}
	before, _ := json.Marshal(policy)
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got := scopeToolAccess(policy, Caller{UserID: "bob", ChatType: "direct"})
			if !reflect.DeepEqual(got, []string{"echo"}) {
				t.Error("user restriction bypassed")
			}
			got[0] = "changed"
		}()
	}
	wg.Wait()
	after, _ := json.Marshal(policy)
	if string(before) != string(after) {
		t.Fatal("shared revision policy mutated")
	}
	policy.ToolAllowedUsers["write"] = nil
	if got := scopeToolAccess(policy, Caller{UserID: "alice", ChatType: "direct"}); !reflect.DeepEqual(got, []string{"echo"}) {
		t.Fatal("empty explicit allowlist must deny everyone")
	}
}

func TestToolAccessRejectsInvalidPolicy(t *testing.T) {
	for _, raw := range []string{
		`{"tool_allowed_users":{"write":[""]}}`,
		`{"tool_allowed_users":{"write":["alice","alice"]}}`,
		`{"tool_allowed_users":{"write":[" alice"]}}`,
		`{"tool_allowed_users":{"write":["user\nforged"]}}`,
		`{"tool_allowed_users":{"bad tool":["alice"]}}`,
		`{"direct_only_tools":["write","write"]}`,
	} {
		if _, err := ParseToolPolicy(json.RawMessage(raw)); err == nil {
			t.Fatal("invalid tool access policy accepted")
		}
	}
}
