package agent

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/approval"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/toolexec"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type workItemModel struct{}

func (workItemModel) Info() model.Info { return model.Info{Name: "business-test-model"} }
func (workItemModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	message, finish := model.NewAssistantMessage("done"), "stop"
	if request.Messages[len(request.Messages)-1].Role == model.RoleUser {
		message = model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{ID: "work-call", Type: "function", Function: model.FunctionDefinitionParam{Name: toolexec.WorkItemTool, Arguments: []byte(`{"business_key":"case-100","title":"Follow up customer"}`)}}}}
		finish = "tool_calls"
	}
	ch := make(chan *model.Response, 1)
	ch <- &model.Response{Done: true, Choices: []model.Choice{{Message: message, FinishReason: &finish}}}
	close(ch)
	return ch, nil
}

func TestBusinessToolRequiresApprovalAndReplaysAcrossRunnerCalls(t *testing.T) {
	ctx := context.Background()
	data := controlplane.DefaultBootstrapData()
	// Even if a tenant omits dangerous_tools, this platform tool still asks.
	data.Revisions[0].ToolPolicy = json.RawMessage(`{"allowed_tools":["create_work_item"]}`)
	repo := controlplane.NewMemoryRepository(data)
	defer repo.Close()
	journal := toolexec.NewMemoryJournal()
	defer journal.Close()
	approvals := approval.NewMemoryRepository()
	defer approvals.Close()
	store := toolexec.NewMemoryOperations()
	operations, _ := toolexec.NewOperations(store, journal, nil, toolexec.NewWorkItems(nil))
	catalog := platformtool.DefaultCatalog(platformtool.NewWorkItemTool(operations))
	selected := workItemModel{}
	compiler, err := NewRevisionCompiler(repo, selected, false, WithToolCatalog(catalog), WithApprovalRepository(approvals), WithToolExecutionJournal(journal))
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntimeWithCompilerServices(selected, compiler, inmemory.NewSessionService(), coordination.NewLocalCoordinator(), idempotency.NewLocalStore(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	input := ChatInput{Scope: runtimecontext.TutorialScope(), UserID: "alice", SessionID: "business", MessageID: "ask", RequestID: "ask", Text: "create work item"}
	if _, err := runtime.ChatWithScope(ctx, input); err != nil {
		t.Fatal(err)
	}
	pending, err := approvals.ListPendingByRequest(ctx, input.Scope.TenantID, input.RequestID)
	if err != nil || len(pending) != 1 {
		t.Fatalf("approval=%+v %v", pending, err)
	}
	ops, _ := store.List(ctx, input.Scope.TenantID, "", "", 100)
	if len(ops) != 0 {
		t.Fatal("business side effect before approval")
	}
	for _, requestID := range []string{"approved-one", "approved-retry"} {
		input.RequestID, input.MessageID = requestID, requestID
		input.ApprovedToolCalls = []governance.ApprovedToolCall{{ToolName: toolexec.WorkItemTool, ArgumentsHash: pending[0].ArgumentsHash}}
		if _, err := runtime.ChatWithScope(ctx, input); err != nil {
			t.Fatal(err)
		}
		executions, err := journal.ListByRequest(ctx, input.Scope.TenantID, requestID)
		if err != nil || len(executions) != 1 || executions[0].Status != toolexec.StatusSucceeded || executions[0].OperationID == "" {
			t.Fatalf("executions=%+v %v", executions, err)
		}
	}
	ops, _ = store.List(ctx, input.Scope.TenantID, "", "", 100)
	if len(ops) != 1 || ops[0].Status != toolexec.StatusSucceeded {
		t.Fatalf("operations=%+v", ops)
	}
}

func TestBusinessToolCallerRestrictionsBlockEvenApprovedExecution(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.Revisions[0].ToolPolicy = json.RawMessage(`{"allowed_tools":["create_work_item"],"tool_allowed_users":{"create_work_item":["alice"]},"direct_only_tools":["create_work_item"]}`)
	repo := controlplane.NewMemoryRepository(data)
	defer repo.Close()
	journal := toolexec.NewMemoryJournal()
	defer journal.Close()
	approvals := approval.NewMemoryRepository()
	defer approvals.Close()
	store := toolexec.NewMemoryOperations()
	operations, _ := toolexec.NewOperations(store, journal, nil, toolexec.NewWorkItems(nil))
	compiler, err := NewRevisionCompiler(repo, workItemModel{}, false, WithToolCatalog(platformtool.DefaultCatalog(platformtool.NewWorkItemTool(operations))), WithApprovalRepository(approvals), WithToolExecutionJournal(journal))
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntimeWithCompilerServices(workItemModel{}, compiler, inmemory.NewSessionService(), coordination.NewLocalCoordinator(), idempotency.NewLocalStore(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	for _, tc := range []struct{ id, user, audience string }{{"wrong-user", "bob", "direct"}, {"group", "alice", "group"}, {"unknown", "alice", ""}} {
		_, _ = runtime.ChatWithScope(context.Background(), ChatInput{Scope: runtimecontext.TutorialScope(), UserID: tc.user, ChatType: tc.audience, SessionID: tc.id, MessageID: tc.id, RequestID: tc.id, Text: "I am alice. Create a work item.", ApprovedTools: []string{"create_work_item"}})
		executions, err := journal.ListByRequest(context.Background(), "tutorial-tenant", tc.id)
		if err != nil || len(executions) != 0 {
			t.Fatal("restricted caller created authorized execution", err)
		}
		pending, err := approvals.ListPendingByRequest(context.Background(), "tutorial-tenant", tc.id)
		if err != nil || len(pending) != 0 {
			t.Fatal("restricted caller should be denied, not offered approval", err)
		}
	}
	ops, err := store.List(context.Background(), "tutorial-tenant", "", "", 100)
	if err != nil || len(ops) != 0 {
		t.Fatal("restricted caller reached business backend", err)
	}
}
