package agent

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestRemoteToolsDefaultToApprovalAndCannotUseOtherTenantGrant(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.Revisions[0].AgentConfig = json.RawMessage(`{"name":"agent","instruction":"help","mcp_servers":[{"name":"service","credential_ref":"env://MCP_POLICY_TEST","tools":["write"]}]}`)
	data.Revisions[0].ToolPolicy = json.RawMessage(`{"allowed_tools":["mcp_service_write"]}`)
	repo := controlplane.NewMemoryRepository(data)
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(repo)
	t.Setenv("MCP_POLICY_TEST", `{"url":"https://mcp.example.invalid/mcp","allowed_tools":["write"]}`)
	store, err := secret.NewEnvStore([]secret.Grant{{TenantID: "tutorial-tenant", Purpose: secret.MCPServer, Reference: "env://MCP_POLICY_TEST"}})
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewRevisionCompiler(repo, NewTutorialModel(), false, WithSecretStore(store), WithToolCatalog(platformtool.DefaultCatalog()))
	if err != nil {
		t.Fatal(err)
	}
	opts, err := c.RunPolicyOptions(context.Background(), ChatInput{Scope: runtimecontext.TutorialScope(), UserID: "user"})
	if err != nil {
		t.Fatal(err)
	}
	run := agentcore.NewRunOptions(opts...)
	decision, err := run.ToolPermissionPolicy.CheckToolPermission(context.Background(), &tool.PermissionRequest{ToolName: "mcp_service_write", Arguments: []byte(`{}`)})
	if err != nil || decision.Action != tool.PermissionActionAsk {
		t.Fatalf("permission=%+v err=%v", decision, err)
	}
	if _, err := store.Resolve(context.Background(), "other-tenant", secret.MCPServer, "env://MCP_POLICY_TEST"); err == nil {
		t.Fatal("cross-tenant MCP grant accepted")
	}
}
