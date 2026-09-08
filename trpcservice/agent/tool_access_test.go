package agent

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestCompilerAppliesVerifiedUserAndAudienceToToolAccess(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.Revisions[0].ToolPolicy = json.RawMessage(`{"allowed_tools":["dangerous_demo"],"dangerous_tools":["dangerous_demo"],"tool_allowed_users":{"dangerous_demo":["alice"]},"direct_only_tools":["dangerous_demo"]}`)
	repo := controlplane.NewMemoryRepository(data)
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(repo)
	compiler, err := NewRevisionCompiler(repo, NewTutorialModel(), false)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		user, audience string
		want           agenttool.PermissionAction
	}{
		{"alice", "direct", agenttool.PermissionActionAllow}, {"bob", "direct", agenttool.PermissionActionDeny}, {"alice", "group", agenttool.PermissionActionDeny}, {"alice", "", agenttool.PermissionActionDeny},
	} {
		options, err := compiler.RunPolicyOptions(context.Background(), ChatInput{Scope: runtimecontext.TutorialScope(), UserID: tc.user, ChatType: tc.audience, ApprovedTools: []string{"dangerous_demo"}})
		if err != nil {
			t.Fatal(err)
		}
		decision, err := agentcore.NewRunOptions(options...).ToolPermissionPolicy.CheckToolPermission(context.Background(), &agenttool.PermissionRequest{ToolName: "dangerous_demo"})
		if err != nil || decision.Action != tc.want {
			t.Fatal("compiler caller scope did not reach permission policy", err)
		}
	}
}
