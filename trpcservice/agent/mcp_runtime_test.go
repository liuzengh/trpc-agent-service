package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/toolexec"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type mcpCallingModel struct{}

func (mcpCallingModel) Info() model.Info { return model.Info{Name: "mcp-test"} }
func (mcpCallingModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	message := model.NewAssistantMessage("remote tool completed")
	finish := "stop"
	if request.Messages[len(request.Messages)-1].Role == model.RoleUser {
		message = model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{ID: "remote-call", Type: "function", Function: model.FunctionDefinitionParam{Name: "mcp_test_echo", Arguments: []byte(`{"q":"hello"}`)}}}}
		finish = "tool_calls"
	}
	out := make(chan *model.Response, 1)
	out <- &model.Response{Done: true, Choices: []model.Choice{{Message: message, FinishReason: &finish}}}
	close(out)
	return out, nil
}
func TestRunnerUsesGovernedMCPToolAndWritesExecutionAudit(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.WriteHeader(204)
			return
		}
		var req struct {
			ID     any    `json:"id"`
			Method string `json:"method"`
		}
		if json.NewDecoder(r.Body).Decode(&req) != nil {
			t.Error("bad RPC")
		}
		if req.ID == nil {
			w.WriteHeader(202)
			return
		}
		var result any
		switch req.Method {
		case "initialize":
			result = map[string]any{"protocolVersion": "2025-03-26", "capabilities": map[string]any{"tools": map[string]any{}}, "serverInfo": map[string]any{"name": "fixture", "version": "1"}}
		case "tools/list":
			result = map[string]any{"tools": []any{map[string]any{"name": "echo", "inputSchema": map[string]any{"type": "object"}}}}
		case "tools/call":
			calls.Add(1)
			result = map[string]any{"content": []any{map[string]any{"type": "text", "text": "hello"}}}
		default:
			t.Error("unexpected RPC")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
	}))
	defer server.Close()
	data := controlplane.DefaultBootstrapData()
	data.Revisions[0].AgentConfig = json.RawMessage(`{"name":"agent","instruction":"use tool","mcp_servers":[{"name":"test","credential_ref":"mcp","tools":["echo"]}]}`)
	data.Revisions[0].ToolPolicy = json.RawMessage(`{"allowed_tools":["mcp_test_echo"]}`)
	data.Revisions[0].Checksum = controlplane.RevisionChecksum(data.Revisions[0])
	repo := controlplane.NewMemoryRepository(data)
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(repo)
	credential, _ := json.Marshal(platformtool.MCPCredential{URL: server.URL + "/mcp", AllowedTools: []string{"echo"}, ReadOnlyTools: []string{"echo"}})
	journal := toolexec.NewMemoryJournal()
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(journal)
	writer := audit.NewMemoryWriter()
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(writer)
	selected := mcpCallingModel{}
	c, err := NewRevisionCompiler(repo, selected, false, WithToolCatalog(platformtool.DefaultCatalog()), WithSecretStore(secret.StaticStore{"mcp": string(credential)}), WithToolExecutionJournal(journal), WithAuditWriter(writer))
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntimeWithCompilerServices(selected, c, inmemory.NewSessionService(), coordination.NewLocalCoordinator(), idempotency.NewLocalStore(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(runtime)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	input := ChatInput{Scope: runtimecontext.TutorialScope(), UserID: "user", SessionID: "session", MessageID: "mcp", RequestID: "mcp", Text: "call echo"}
	if _, err := runtime.ChatWithScope(ctx, input); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("remote calls=%d", calls.Load())
	}
	rows, err := journal.ListByRequest(ctx, input.Scope.TenantID, "mcp")
	if err != nil || len(rows) != 1 || rows[0].Status != toolexec.StatusSucceeded {
		t.Fatalf("execution journal=%+v err=%v", rows, err)
	}
	found := false
	for _, e := range writer.Events() {
		if e.Decision == "tool_succeeded" && e.ToolName == "mcp_test_echo" {
			found = true
		}
	}
	if !found {
		t.Fatal("missing MCP execution audit")
	}
}
