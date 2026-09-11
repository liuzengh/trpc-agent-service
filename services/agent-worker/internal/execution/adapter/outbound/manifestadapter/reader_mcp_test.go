package manifestadapter

import (
	"context"
	protocol "github.com/liuzengh/trpc-agent-service/api/schemas/deployment/v1"
	"testing"
)

func TestReaderMCPFixedSelectionAndCredential(t *testing.T) {
	for _, auth := range []string{"none", "bearer"} {
		t.Run(auth, func(t *testing.T) {
			reader, route, _, _ := readerDataInput(t, func(c map[string]any) {
				n := readerDataNode(c)
				n["tool_resources"] = []string{"search"}
				n["callable_entries"] = []string{"tools/search"}
				c["resolved_requirements"].(map[string]any)["tools"] = map[string]any{"search": "search"}
				resources := c["resources"].(map[string]any)
				resources["models"].(map[string]any)["primary"].(map[string]any)["capabilities"] = []string{"chat", "tool_call"}
				r := map[string]any{"kind": "mcp_streamable_http", "adapter_version": "mcp-web-search-v1", "server_url": "https://mcp.example.test/mcp", "toolset_name": "selected", "tool_name": "lookup", "auth": map[string]any{"kind": auth}, "capability": "calculator.add"}
				if auth == "bearer" {
					r["auth"].(map[string]any)["credential"] = map[string]any{"credential_id": "crd_22222222222222222222222222222222", "purpose": "bearer_token", "audience_digest": protocol.CredentialAudienceDigest("mcp_streamable_http", "https://mcp.example.test/mcp", auth)}
				}
				resources["tools"] = map[string]any{"search": r}
				e := c["execution"].(map[string]any)
				e["allowed_endpoint_hosts"] = append(e["allowed_endpoint_hosts"].([]any), "mcp.example.test")
			})
			p, err := reader.Resolve(context.Background(), route)
			if err != nil {
				t.Fatal(err)
			}
			if len(p.Tools) != 1 {
				t.Fatal(p.Tools)
			}
			x := p.Tools[0]
			if x.Resource != "search" || x.ToolName != "lookup" || x.Capability != "calculator.add" || x.AuthKind != auth {
				t.Fatal(x)
			}
			count := 2
			if auth == "bearer" {
				count++
			}
			if len(p.Uses()) != count {
				t.Fatal(p.Uses())
			}
			p.Tools[0].ServerURL = "https://changed.example.test"
			again, err := reader.Resolve(context.Background(), route)
			if err != nil || again.Tools[0].ServerURL != x.ServerURL {
				t.Fatal("mutable plan aliases publication", err)
			}
		})
	}
}
