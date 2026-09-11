package agent

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/docsmcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/toolexec"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type docsCallingModel struct{}

func (docsCallingModel) Info() model.Info { return model.Info{Name: "synthetic-docs-model"} }
func (docsCallingModel) GenerateContent(ctx context.Context, req *model.Request) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	last := req.Messages[len(req.Messages)-1]
	message, finish := model.NewAssistantMessage(last.Content), "stop"
	if last.Role == model.RoleUser {
		message = model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{ID: "docs-call", Type: "function", Function: model.FunctionDefinitionParam{Name: "mcp_docs_search_project_docs", Arguments: []byte(`{"query":"Redis Session","limit":1}`)}}}}
		finish = "tool_calls"
	}
	out := make(chan *model.Response, 1)
	out <- &model.Response{Done: true, Choices: []model.Choice{{Message: message, FinishReason: &finish}}}
	close(out)
	return out, nil
}
func TestRunnerCallsRealDocsMCPProtocolAndRecordsTool(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "docs"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "docs", "architecture.md"), []byte("# Synthetic architecture\nRedis Session is shared across workers.\n"), 0600); err != nil {
		t.Fatal(err)
	}
	index, err := docsmcp.LoadIndex(root)
	if err != nil {
		t.Fatal(err)
	}
	token := "synthetic-docs-auth-0123456789abcdef"
	handler, err := docsmcp.NewHandler(index, token)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	data := controlplane.DefaultBootstrapData()
	data.Revisions[0].AgentConfig = json.RawMessage(`{"name":"docs-agent","instruction":"Use documentation","mcp_servers":[{"name":"docs","credential_ref":"docs","tools":["search_project_docs"]}]}`)
	data.Revisions[0].ToolPolicy = json.RawMessage(`{"allowed_tools":["mcp_docs_search_project_docs"]}`)
	data.Revisions[0].Checksum = controlplane.RevisionChecksum(data.Revisions[0])
	repo := controlplane.NewMemoryRepository(data)
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(repo)
	raw, _ := json.Marshal(platformtool.MCPCredential{URL: server.URL + "/mcp", BearerToken: token, AllowedTools: []string{docsmcp.ToolName}, ReadOnlyTools: []string{docsmcp.ToolName}})
	writer := audit.NewMemoryWriter()
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(writer)
	journal := toolexec.NewMemoryJournal()
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(journal)
	selected := docsCallingModel{}
	compiler, err := NewRevisionCompiler(repo, selected, false, WithToolCatalog(platformtool.DefaultCatalog()), WithSecretStore(secret.StaticStore{"docs": string(raw)}), WithAuditWriter(writer), WithToolExecutionJournal(journal))
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntimeWithCompilerServices(selected, compiler, inmemory.NewSessionService(), coordination.NewLocalCoordinator(), idempotency.NewLocalStore(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(runtime)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result, err := runtime.ChatWithScope(ctx, ChatInput{Scope: runtimecontext.TutorialScope(), ChatType: "direct", UserID: "test-user", SessionID: "test-session", RequestID: "docs-request", MessageID: "docs-message", Text: "Search docs"})
	if err != nil || !strings.Contains(result.Reply, "Redis Session") || !strings.Contains(result.Reply, "docs/architecture.md") {
		t.Fatalf("result=%q err=%v", result.Reply, err)
	}
	executions, err := journal.ListByRequest(ctx, "tutorial-tenant", "docs-request")
	if err != nil || len(executions) != 1 || executions[0].Status != toolexec.StatusSucceeded {
		t.Fatal("missing successful governed MCP execution", err)
	}
	found := false
	for _, e := range writer.Events() {
		if e.ToolName == "mcp_docs_search_project_docs" && e.Decision == "tool_succeeded" {
			found = true
		}
	}
	if !found {
		t.Fatal("missing MCP success audit")
	}
}
