package agent

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/toolexec"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type knowledgeCallingModel struct{}

func (knowledgeCallingModel) Info() model.Info { return model.Info{Name: "synthetic-knowledge-model"} }
func (knowledgeCallingModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	last := request.Messages[len(request.Messages)-1]
	message, finish := model.NewAssistantMessage(last.Content), "stop"
	if last.Role == model.RoleUser {
		message = model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{ID: "knowledge-call", Type: "function", Function: model.FunctionDefinitionParam{Name: "knowledge_search", Arguments: []byte(`{"query":"how long can I borrow the test document?"}`)}}}}
		finish = "tool_calls"
	}
	ch := make(chan *model.Response, 1)
	ch <- &model.Response{Done: true, Choices: []model.Choice{{Message: message, FinishReason: &finish}}}
	close(ch)
	return ch, nil
}

func TestRunnerKnowledgeToolUsesRemoteEmbeddingAndJournal(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/embeddings" || r.Header.Get("Authorization") != "Bearer test-embedding-key" {
			t.Error("embedding request missing scope credential")
		}
		_, _ = io.WriteString(w, `{"data":[{"index":0,"embedding":[1,0,0]}],"usage":{"prompt_tokens":5,"total_tokens":5}}`)
	}))
	defer server.Close()
	data := controlplane.DefaultBootstrapData()
	data.BackendBindings = append(data.BackendBindings, controlplane.BackendBinding{ID: "knowledge", TenantID: "tutorial-tenant", AppID: "tutorial-app", ResourceType: "knowledge", BackendType: "inmemory", MigrationState: "active", Version: 1, Config: json.RawMessage(`{"dimensions":3}`)})
	data.Revisions[0].KnowledgeConfig, _ = json.Marshal(map[string]any{"enabled": true, "embedding": map[string]any{"provider": "openai", "model": "test-model", "base_url": server.URL + "/v1", "dimensions": 3, "secret_ref": "test://embedding"}})
	data.Revisions[0].Checksum = controlplane.RevisionChecksum(data.Revisions[0])
	repo := controlplane.NewMemoryRepository(data)
	defer repo.Close()
	secrets := secret.StaticStore{"test://embedding": "test-embedding-key"}
	router, _ := storage.NewKnowledgeRouter(repo, secrets)
	defer router.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := router.UpsertDocument(ctx, runtimecontext.TutorialScope(), data.Revisions[0], storage.KnowledgeDocument{ID: "synthetic-borrowing", Content: "The test document may be borrowed for seventeen days."}); err != nil {
		t.Fatal(err)
	}
	writer := audit.NewMemoryWriter()
	defer writer.Close()
	journal := toolexec.NewMemoryJournal()
	defer journal.Close()
	selected := knowledgeCallingModel{}
	compiler, err := NewRevisionCompiler(repo, selected, false, WithToolCatalog(platformtool.DefaultCatalog()), WithSecretStore(secrets), WithKnowledgeProvider(router), WithAuditWriter(writer), WithToolExecutionJournal(journal))
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntimeWithCompilerServices(selected, compiler, inmemory.NewSessionService(), coordination.NewLocalCoordinator(), idempotency.NewLocalStore(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	result, err := runtime.ChatWithScope(ctx, ChatInput{Scope: runtimecontext.TutorialScope(), ChatType: "direct", UserID: "synthetic-user", SessionID: "knowledge-test", RequestID: "knowledge-request", MessageID: "knowledge-message", Text: "Consult the knowledge base"})
	if err != nil || !strings.Contains(result.Reply, "seventeen days") || !strings.Contains(result.Reply, "synthetic-borrowing") {
		t.Fatal("Runner did not return scoped Knowledge evidence", err)
	}
	executions, err := journal.ListByRequest(ctx, "tutorial-tenant", "knowledge-request")
	if err != nil || len(executions) != 1 || executions[0].ToolName != "knowledge_search" || executions[0].Status != toolexec.StatusSucceeded {
		t.Fatal("Knowledge tool bypassed journal", err)
	}
	found := false
	for _, event := range writer.Events() {
		if event.ToolName == "knowledge_search" && event.Decision == "tool_succeeded" {
			found = true
		}
	}
	if !found {
		t.Fatal("Knowledge tool success audit missing")
	}
}
