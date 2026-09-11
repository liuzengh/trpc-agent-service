package assembly

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
	tmcp "trpc.group/trpc-go/trpc-mcp-go"
)

type stubTool struct {
	name string
}

func (t stubTool) Declaration() *agenttool.Declaration {
	return &agenttool.Declaration{Name: t.name, Description: t.name}
}

func (t stubTool) Call(context.Context, []byte) (any, error) {
	return map[string]string{"ok": "true"}, nil
}

func TestToolRegistryAppliesTenantAllowList(t *testing.T) {
	registry, err := NewToolRegistry(map[string]agenttool.CallableTool{
		"demo.echo": stubTool{name: "demo.echo"},
	})
	if err != nil {
		t.Fatalf("NewToolRegistry() error = %v", err)
	}

	surface, err := registry.Surface(context.Background(), config.TenantConfig{
		TenantID: "acme", AppCode: "support",
		Tools: config.ToolPolicy{Allowed: []string{"demo.echo"}},
	})
	if err != nil {
		t.Fatalf("Surface() error = %v", err)
	}
	tools := surface.Tools
	if len(tools) != 2 || tools[0].Declaration().Name != "platform.present_card" || tools[1].Declaration().Name != "demo.echo" {
		t.Fatalf("Surface().Tools = %#v, want implicit present_card plus demo.echo", tools)
	}

	surface, err = registry.Surface(context.Background(), config.TenantConfig{TenantID: "acme", AppCode: "support"})
	if err != nil || len(surface.Tools) != 1 || surface.Tools[0].Declaration().Name != "platform.present_card" || len(surface.ToolSets) != 0 {
		t.Fatalf("empty allow-list surface/error = %#v/%v, want implicit present_card/nil", surface, err)
	}

	_, err = registry.Surface(context.Background(), config.TenantConfig{
		TenantID: "acme", AppCode: "support",
		Tools: config.ToolPolicy{Allowed: []string{"unknown.tool"}},
	})
	if err == nil {
		t.Fatal("unknown allowed tool error = nil")
	}
}

func TestToolRegistryRejectsDuplicateModelFacingToolName(t *testing.T) {
	registry, err := NewToolRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = registry.Surface(context.Background(), config.TenantConfig{
		TenantID: "acme", AppCode: "support",
		Tools: config.ToolPolicy{
			Allowed: []string{"duckduckgo_search"},
			HTTP: []config.HTTPToolConfig{{
				Name: "duckduckgo_search", URL: "https://tools.example.com/search",
			}},
		},
	})
	if err == nil {
		t.Fatal("Surface() error = nil, want duplicate model-facing name rejected")
	}
}

func TestToolRegistryCatalogUsesRuntimeDeclarations(t *testing.T) {
	registry, err := NewToolRegistry(map[string]agenttool.CallableTool{
		"ignored-map-key": stubTool{name: "zeta.tool"},
		"also-ignored":    stubTool{name: "alpha.tool"},
	})
	if err != nil {
		t.Fatalf("NewToolRegistry() error = %v", err)
	}
	if got := registry.Names(); len(got) != 4 || got[0] != "alpha.tool" || got[1] != "duckduckgo_search" || got[2] != "platform.present_card" || got[3] != "zeta.tool" {
		t.Fatalf("Names() = %v, want stable declaration names plus built-in search/card tools", got)
	}
	selectable := registry.SelectableNames()
	if len(selectable) != 3 || selectable[0] != "alpha.tool" || selectable[1] != "duckduckgo_search" || selectable[2] != "zeta.tool" {
		t.Fatalf("SelectableNames() = %v, want tenant-grantable tools only", selectable)
	}
	catalog := registry.Catalog()
	if len(catalog) != 3 || catalog[0].Name != "alpha.tool" || catalog[0].Description != "alpha.tool" {
		t.Fatalf("Catalog() = %#v, want tenant-selectable runtime declaration metadata", catalog)
	}
}

func TestToolRegistryAlwaysExposesPresentCardWithoutTenantGrant(t *testing.T) {
	registry, err := NewToolRegistry(nil)
	if err != nil {
		t.Fatal(err)
	}
	surface, err := registry.Surface(context.Background(), config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(surface.Tools) != 1 || surface.Tools[0].Declaration().Name != "platform.present_card" {
		t.Fatalf("Surface().Tools = %#v", surface.Tools)
	}
}

func TestToolRegistryHTTPFunctionToolUsesConfiguredSchemaAndSecretResolver(t *testing.T) {
	var sawAuth atomic.Bool
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") == "Bearer resolved-token" {
			sawAuth.Store(true)
		}
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if body["order_id"] != "42" {
			t.Fatalf("request body = %#v", body)
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"status": "ok"})
	}))
	defer server.Close()

	secrets, err := credential.NewEnvironmentSecretResolver(func(name string) string {
		if name == "TOOL_TOKEN" {
			return "resolved-token"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewToolRegistry(nil, WithToolSecretResolver(secrets), WithToolHTTPClient(server.Client()))
	if err != nil {
		t.Fatal(err)
	}
	surface, err := registry.Surface(context.Background(), config.TenantConfig{
		TenantID: "acme", AppCode: "support",
		Tools: config.ToolPolicy{
			Allowed: []string{"query_order"},
			HTTP: []config.HTTPToolConfig{{
				Name: "query_order", Description: "查询订单", URL: server.URL, CredentialRef: "env:TOOL_TOKEN",
				InputSchema: &agenttool.Schema{Type: "object", Required: []string{"order_id"}, Properties: map[string]*agenttool.Schema{"order_id": {Type: "string"}}},
			}},
		},
	})
	if err != nil {
		t.Fatalf("Surface() error = %v", err)
	}
	if len(surface.Tools) != 2 || surface.Tools[0].Declaration().Name != "platform.present_card" || surface.Tools[1].Declaration().Name != "query_order" {
		t.Fatalf("surface tools = %#v", surface.Tools)
	}
	callable, ok := surface.Tools[1].(agenttool.CallableTool)
	if !ok {
		t.Fatal("HTTP Function Tool is not callable")
	}
	result, err := callable.Call(context.Background(), []byte(`{"order_id":"42"}`))
	if err != nil {
		t.Fatalf("HTTP Function Tool Call() error = %v", err)
	}
	if result == nil || !sawAuth.Load() {
		t.Fatalf("result/auth = %#v/%v", result, sawAuth.Load())
	}
}

func TestToolRegistryMCPUsesNativeToolSetAndFiltersRemoteTools(t *testing.T) {
	mcpServer := tmcp.NewServer(
		"fixture", "1.0.0",
		tmcp.WithServerPath("/mcp"),
		tmcp.WithStatelessMode(true),
		tmcp.WithPostSSEEnabled(false),
		tmcp.WithGetSSEEnabled(false),
	)
	for _, name := range []string{"find_customer", "delete_customer"} {
		toolName := name
		mcpServer.RegisterTool(tmcp.NewTool(toolName, tmcp.WithDescription(toolName)), func(context.Context, *tmcp.CallToolRequest) (*tmcp.CallToolResult, error) {
			return &tmcp.CallToolResult{Content: []tmcp.Content{tmcp.NewTextContent(toolName + " ok")}}, nil
		})
	}
	var sawAuth atomic.Bool
	handler := mcpServer.HTTPHandler()
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") == "Bearer resolved-token" {
			sawAuth.Store(true)
		}
		handler.ServeHTTP(writer, request)
	}))
	defer server.Close()
	secrets, err := credential.NewEnvironmentSecretResolver(func(name string) string {
		if name == "TOOL_TOKEN" {
			return "resolved-token"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	registry, err := NewToolRegistry(nil, WithToolSecretResolver(secrets), WithToolHTTPClient(server.Client()))
	if err != nil {
		t.Fatal(err)
	}
	surface, err := registry.Surface(context.Background(), config.TenantConfig{
		TenantID: "acme", AppCode: "support",
		Tools: config.ToolPolicy{
			Allowed: []string{"crm_find_customer"},
			MCP: []config.MCPToolConfig{{
				Name: "crm", Transport: "streamable", URL: server.URL + "/mcp", CredentialRef: "env:TOOL_TOKEN",
			}},
		},
	})
	if err != nil {
		t.Fatalf("Surface() error = %v", err)
	}
	defer func() {
		for _, set := range surface.ToolSets {
			_ = set.Close()
		}
	}()
	if len(surface.ToolSets) != 1 {
		t.Fatalf("ToolSets() = %d, want 1", len(surface.ToolSets))
	}
	remote := surface.ToolSets[0].Tools(context.Background())
	if len(remote) != 1 || remote[0].Declaration().Name != "find_customer" {
		t.Fatalf("MCP remote tools = %#v, want find_customer only", remote)
	}
	callable, ok := remote[0].(agenttool.CallableTool)
	if !ok {
		t.Fatal("MCP tool is not callable")
	}
	if _, err := callable.Call(context.Background(), []byte(`{}`)); err != nil {
		t.Fatalf("MCP Call() error = %v", err)
	}
	if !sawAuth.Load() {
		t.Fatal("MCP requests did not resolve/inject configured credential")
	}
}
