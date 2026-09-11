package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type memoryCallingModel struct{}

func (memoryCallingModel) Info() model.Info { return model.Info{Name: "synthetic-memory-model"} }
func (memoryCallingModel) GenerateContent(ctx context.Context, req *model.Request) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	last := req.Messages[len(req.Messages)-1]
	message, finish := model.NewAssistantMessage(last.Content), "stop"
	if last.Role == model.RoleUser {
		name, args := "memory_load", []byte(`{"limit":10}`)
		if last.Content == "remember" {
			name = "memory_add"
			args = []byte(`{"memory":"synthetic-long-term-fact","topics":["synthetic-test"]}`)
		}
		message = model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{ID: "memory-call", Type: "function", Function: model.FunctionDefinitionParam{Name: name, Arguments: args}}}}
		finish = "tool_calls"
	}
	out := make(chan *model.Response, 1)
	out <- &model.Response{Done: true, Choices: []model.Choice{{Message: message, FinishReason: &finish}}}
	close(out)
	return out, nil
}

// A fresh Runner has empty Session history; the only source of the fact is
// PostgreSQL Memory. No real model, user facts or IM endpoint is involved.
func TestPersistentMemoryRunnerPostgresIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_URL")
	if dsn == "" {
		t.Skip("isolated PostgreSQL not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	data := controlplane.DefaultBootstrapData()
	data.Revisions[0].MemoryConfig = json.RawMessage(`{"direct_only":true}`)
	data.Revisions[0].ToolPolicy = json.RawMessage(`{"allowed_tools":["memory_add","memory_load"]}`)
	data.Revisions[0].Checksum = controlplane.RevisionChecksum(data.Revisions[0])
	for i, b := range data.BackendBindings {
		if b.ResourceType == "memory" {
			data.BackendBindings[i].BackendType = "postgres"
			data.BackendBindings[i].SecretRef = "secret://test-memory"
			data.BackendBindings[i].Config = json.RawMessage(`{"table_name":"memory_runner_integration"}`)
		}
	}
	repo := controlplane.NewMemoryRepository(data)
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(repo)
	journal := toolexec.NewMemoryJournal()
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(journal)
	writer := audit.NewMemoryWriter()
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(writer)
	selected := memoryCallingModel{}
	build := func() (*Runtime, *storage.MemoryRouter) {
		memories, err := storage.NewMemoryRouter(repo, secret.StaticStore{"secret://test-memory": dsn})
		if err != nil {
			t.Fatal(err)
		}
		compiler, err := NewRevisionCompiler(repo, selected, false, WithToolCatalog(platformtool.DefaultCatalog()), WithToolExecutionJournal(journal), WithAuditWriter(writer))
		if err != nil {
			t.Fatal(err)
		}
		runtime, err := NewRuntimeWithCompilerServices(selected, compiler, inmemory.NewSessionService(), coordination.NewLocalCoordinator(), idempotency.NewLocalStore(), false, runner.WithMemoryService(memories))
		if err != nil {
			t.Fatal(err)
		}
		return runtime, memories
	}
	user := fmt.Sprintf("synthetic-user-%x", time.Now().UnixNano())
	first, firstMemory := build()
	input := ChatInput{Scope: runtimecontext.TutorialScope(), ChatType: "direct", UserID: user, SessionID: "before-restart", MessageID: "remember", RequestID: "remember", Text: "remember"}
	if _, err := first.ChatWithScope(ctx, input); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := firstMemory.Close(); err != nil {
		t.Fatal(err)
	}
	second, secondMemory := build()
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(second)
	defer func(closer interface{ Close() error }) { _ = closer.Close() }(secondMemory)
	input.SessionID = "fresh-session-after-restart"
	input.MessageID = "load"
	input.RequestID = "load"
	input.Text = "load"
	result, err := second.ChatWithScope(ctx, input)
	if err != nil || !strings.Contains(result.Reply, "synthetic-long-term-fact") {
		t.Fatalf("persistent Memory not loaded: %v reply=%q", err, result.Reply)
	}
	for _, id := range []string{"remember", "load"} {
		rows, err := journal.ListByRequest(ctx, input.Scope.TenantID, id)
		if err != nil || len(rows) != 1 || rows[0].Status != toolexec.StatusSucceeded {
			t.Fatalf("request=%s tool journal=%+v err=%v", id, rows, err)
		}
	}
	input.UserID = user + "-other"
	input.SessionID = "other-user"
	input.RequestID = "other-user"
	input.MessageID = "other-user"
	result, err = second.ChatWithScope(ctx, input)
	if err != nil || strings.Contains(result.Reply, "synthetic-long-term-fact") {
		t.Fatal("cross-user memory disclosure", err)
	}
	input.UserID = user
	input.ChatType = "group"
	input.SessionID = "group"
	input.RequestID = "group"
	input.MessageID = "group"
	result, _ = second.ChatWithScope(ctx, input)
	if strings.Contains(result.Reply, "synthetic-long-term-fact") {
		t.Fatal("private memory disclosed in group")
	}
	rows, err := journal.ListByRequest(ctx, input.Scope.TenantID, "group")
	if err != nil || len(rows) != 0 {
		t.Fatal("group memory tool was executed", err)
	}
}
