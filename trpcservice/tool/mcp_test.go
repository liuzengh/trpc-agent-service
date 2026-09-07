package tool

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/session"
	coretool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestMCPToolScopePaginationGrantsAndSchemaPin(t *testing.T) {
	var calls atomic.Int32
	var changed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer credential-canary" {
			t.Error("missing tenant credential")
		}
		if r.Method == http.MethodDelete {
			w.WriteHeader(204)
			return
		}
		var e struct {
			ID     any    `json:"id"`
			Method string `json:"method"`
			Params struct {
				Cursor string `json:"cursor"`
				Name   string `json:"name"`
			} `json:"params"`
		}
		if json.NewDecoder(r.Body).Decode(&e) != nil {
			t.Error("invalid RPC")
		}
		if e.ID == nil {
			w.WriteHeader(202)
			return
		}
		var result any
		switch e.Method {
		case "initialize":
			result = map[string]any{"protocolVersion": "2025-03-26", "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]any{"name": "fixture", "version": "1"}}
		case "tools/list":
			if e.Params.Cursor == "" {
				result = map[string]any{"tools": []any{}, "nextCursor": "page2"}
			} else {
				schema := map[string]any{"type": "object", "properties": map[string]any{"q": map[string]any{"type": "string"}}}
				if changed.Load() {
					schema["required"] = []string{"new-field"}
				}
				result = map[string]any{"tools": []any{map[string]any{"name": "echo", "description": "safe echo", "inputSchema": schema}}}
			}
		case "tools/call":
			if e.Params.Name != "echo" {
				t.Error("unapproved tool")
			}
			calls.Add(1)
			result = map[string]any{"content": []any{map[string]any{"type": "text", "text": "credential-canary response"}}}
		default:
			t.Error("unexpected method")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": e.ID, "result": result})
	}))
	defer server.Close()
	raw, _ := json.Marshal(MCPCredential{URL: server.URL + "/mcp", BearerToken: "credential-canary", AllowedTools: []string{"echo"}, ReadOnlyTools: []string{"echo"}})
	store := secret.StaticStore{"fixture": string(raw)}
	scope := runtimecontext.TutorialScope()
	spec := []MCPServerSpec{{Name: "test", CredentialRef: "fixture", Tools: []string{"echo"}}}
	tools, err := BuildMCPTools(context.Background(), store, scope, spec, []string{"mcp_test_echo"})
	if err != nil || len(tools) != 1 {
		t.Fatalf("tools=%d err=%v", len(tools), err)
	}
	call := tools[0].(coretool.CallableTool)
	if _, err := call.Call(context.Background(), []byte(`{"q":"hi"}`)); err == nil {
		t.Fatal("unscoped tool call accepted")
	}
	inv := &agentcore.Invocation{Session: session.NewSession(scope.StorageScope, "user", "session"), RunOptions: agentcore.RunOptions{AppName: scope.StorageScope}}
	ctx := agentcore.NewInvocationContext(context.Background(), inv)
	out, err := call.Call(ctx, []byte(`{"q":"hi"}`))
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(out)
	if strings.Contains(string(encoded), "credential-canary") {
		t.Fatal("credential echoed into model result")
	}
	changed.Store(true)
	if _, err := call.Call(ctx, []byte(`{"q":"hi"}`)); err == nil {
		t.Fatal("changed remote schema accepted")
	}
	if calls.Load() != 1 {
		t.Fatal("unapproved external calls")
	}
	dangerous, err := MCPDangerousTools(ctx, store, scope.TenantID, spec)
	if err != nil || len(dangerous) != 0 {
		t.Fatal("deployment read-only grant ignored")
	}
	var cfg MCPCredential
	_ = json.Unmarshal(raw, &cfg)
	cfg.ReadOnlyTools = nil
	raw, _ = json.Marshal(cfg)
	store["fixture"] = string(raw)
	dangerous, err = MCPDangerousTools(ctx, store, scope.TenantID, spec)
	if err != nil || len(dangerous) != 1 {
		t.Fatal("remote tools must default to approval")
	}
	if _, err := call.Call(ctx, []byte(`{}`)); err == nil {
		t.Fatal("revoked read-only grant remained usable")
	}
}
func TestMCPConfigRejectsCommandsAndDuplicateTools(t *testing.T) {
	for _, raw := range []string{`{"mcp_servers":[{"name":"server","credential_ref":"x","tools":[]}]}`, `{"mcp_servers":[{"name":"server","credential_ref":"x","tools":["echo","echo"]}]}`} {
		if _, err := ParseMCPServers([]byte(raw)); err == nil {
			t.Fatal("invalid MCP spec accepted")
		}
	}
	if _, err := loadMCPCredential(context.Background(), secret.StaticStore{"x": `{"url":"file:///private","allowed_tools":["echo"]}`}, "tenant", "x"); err == nil {
		t.Fatal("non-HTTP transport accepted")
	}
}
