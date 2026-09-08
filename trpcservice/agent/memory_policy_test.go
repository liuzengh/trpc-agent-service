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

func TestCompilerMemoryPermissionUsesTrustedChatAudience(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.Revisions[0].MemoryConfig = json.RawMessage(`{"direct_only":true}`)
	data.Revisions[0].ToolPolicy = json.RawMessage(`{"allowed_tools":["memory_add","memory_load","echo"]}`)
	repo := controlplane.NewMemoryRepository(data)
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(repo)
	compiler, err := NewRevisionCompiler(repo, NewTutorialModel(), false)
	if err != nil {
		t.Fatal(err)
	}
	for _, audience := range []string{"direct", "group", ""} {
		input := ChatInput{Scope: runtimecontext.TutorialScope(), UserID: "user", ChatType: audience, ApprovedTools: []string{"memory_load"}}
		opts, err := compiler.RunPolicyOptions(context.Background(), input)
		if err != nil {
			t.Fatal(err)
		}
		policy := agentcore.NewRunOptions(opts...).ToolPermissionPolicy
		for _, name := range []string{"memory_add", "memory_load", "echo"} {
			decision, err := policy.CheckToolPermission(context.Background(), &agenttool.PermissionRequest{ToolName: name})
			if err != nil {
				t.Fatal(err)
			}
			want := agenttool.PermissionActionAllow
			if audience != "direct" && name != "echo" {
				want = agenttool.PermissionActionDeny
			}
			if decision.Action != want {
				t.Fatalf("audience=%q tool=%s action=%s", audience, name, decision.Action)
			}
		}
	}
}
