package agent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/approval"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	platformskill "github.com/liuzengh/trpc-agent-service/trpcservice/skill"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/toolexec"
	"github.com/liuzengh/trpc-agent-service/trpcservice/workspace"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type skillModel struct{ sawBody atomic.Bool }

func (*skillModel) Info() model.Info { return model.Info{Name: "skill-fixture"} }
func (m *skillModel) GenerateContent(_ context.Context, req *model.Request) (<-chan *model.Response, error) {
	loaded := false
	for _, msg := range req.Messages {
		if strings.Contains(msg.Content, "SKILL_BODY_CANARY") {
			m.sawBody.Store(true)
		}
		for _, call := range msg.ToolCalls {
			if call.Function.Name == "skill_load" {
				loaded = true
			}
		}
	}
	last := req.Messages[len(req.Messages)-1]
	message, finish := model.NewAssistantMessage(last.Content), "stop"
	if last.Role == model.RoleUser || (last.Role == model.RoleTool && strings.Contains(last.Content, "loaded:")) {
		name, args, id := "skill_run", `{"skill":"sample","input":{"value":"hello"}}`, "run-call"
		if !loaded {
			name, args, id = "skill_load", `{"skill":"sample"}`, "load-call"
		}
		message = model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{ID: id, Type: "function", Function: model.FunctionDefinitionParam{Name: name, Arguments: []byte(args)}}}}
		finish = "tool_calls"
	}
	ch := make(chan *model.Response, 1)
	ch <- &model.Response{Done: true, Choices: []model.Choice{{Message: message, FinishReason: &finish}}}
	close(ch)
	return ch, nil
}

type countExecutor struct {
	next  workspace.Executor
	calls atomic.Int32
}

func (x *countExecutor) Execute(ctx context.Context, r workspace.Request) (workspace.Result, error) {
	x.calls.Add(1)
	if x.next != nil {
		return x.next.Execute(ctx, r)
	}
	return workspace.Result{Stdout: "skill-ok"}, nil
}

type skillRuntimeScenario struct{ BudgetOne, DisabledSandbox bool }

func testSkillRuntime(t *testing.T, executor workspace.Executor, scenarios ...skillRuntimeScenario) {
	t.Helper()
	var scenario skillRuntimeScenario
	if len(scenarios) > 0 {
		scenario = scenarios[0]
	}
	ctx := context.Background()
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "sample"), 0700); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{"catalog.json": `[{"name":"sample","version":"1","directory":"sample"}]`, "sample/SKILL.md": "---\nname: sample\ndescription: A fixture skill\n---\nSKILL_BODY_CANARY: call skill_run with input.", "sample/run.sh": "set -eu\ncat /workspace/input.json\necho skill-ok\n"} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	registry, err := platformskill.Load(root, `[{"tenant_id":"tutorial-tenant","name":"sample","version":"1"}]`)
	if err != nil {
		t.Fatal(err)
	}
	data := controlplane.DefaultBootstrapData()
	var agentConfig map[string]any
	_ = json.Unmarshal(data.Revisions[0].AgentConfig, &agentConfig)
	agentConfig["skills"] = []platformskill.Ref{registry.List("tutorial-tenant")[0].Ref}
	data.Revisions[0].AgentConfig, _ = json.Marshal(agentConfig)
	data.Revisions[0].ToolPolicy = json.RawMessage(`{"allowed_tools":["skill_load","skill_run"]}`)
	if scenario.BudgetOne {
		data.Revisions[0].ToolPolicy = json.RawMessage(`{"allowed_tools":["skill_load","skill_run"],"max_tool_calls":1}`)
	}
	repo := controlplane.NewMemoryRepository(data)
	defer func() { _ = repo.Close() }()
	journal := toolexec.NewMemoryJournal()
	defer func() { _ = journal.Close() }()
	approvals := approval.NewMemoryRepository()
	defer func() { _ = approvals.Close() }()
	counter := &countExecutor{next: executor}
	service := &platformskill.Service{Registry: registry, Repository: repo, Journal: journal, Executor: counter}
	if scenario.DisabledSandbox {
		service.Executor = nil
	}
	selected := &skillModel{}
	auditLog := audit.NewMemoryWriter()
	defer func() { _ = auditLog.Close() }()
	compiler, err := NewRevisionCompiler(repo, selected, false, WithSkills(registry), WithToolCatalog(platformtool.DefaultCatalog(service.RunTool())), WithApprovalRepository(approvals), WithToolExecutionJournal(journal), WithAuditWriter(auditLog))
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := NewRuntimeWithCompilerServices(selected, compiler, inmemory.NewSessionService(), coordination.NewLocalCoordinator(), idempotency.NewLocalStore(), false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = runtime.Close() }()
	compiled, err := compiler.Compile(ctx, runtimecontext.TutorialScope())
	if err != nil {
		t.Fatal(err)
	}
	if provider, ok := compiled.(agentcore.CodeExecutor); ok && provider.CodeExecutor() != nil {
		t.Fatal("skill setup enabled a host executor")
	}
	input := ChatInput{Scope: runtimecontext.TutorialScope(), UserID: "alice", SessionID: "skill-test", MessageID: "ask", RequestID: "ask", Text: "use skill", ChatType: "direct"}
	first, err := runtime.ChatWithScope(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := approvals.ListPendingByRequest(ctx, input.Scope.TenantID, "ask")
	if scenario.BudgetOne {
		if err != nil || len(pending) != 0 || counter.calls.Load() != 0 || !strings.Contains(first.Reply, governance.CodeToolBudgetExceeded) || !strings.Contains(first.Reply, "不会通过等待自动恢复") {
			t.Fatalf("budget feedback: %+v %v", first, err)
		}
		entries, err := journal.ListByRequest(ctx, input.Scope.TenantID, input.RequestID)
		if err != nil || len(entries) != 1 || entries[0].ToolName != "skill_load" || entries[0].Status != toolexec.StatusSucceeded {
			t.Fatal("denied script appeared in execution journal", entries, err)
		}
		found := false
		for _, event := range auditLog.Events() {
			if event.Decision == "tool_deny" && event.ToolName == "skill_run" && event.Details["code"] == governance.CodeToolBudgetExceeded {
				found = true
			}
		}
		if !found {
			t.Fatal("structured permission error not persisted")
		}
		again, err := runtime.ChatWithScope(ctx, input)
		if err != nil || !again.Replayed || again.Reply != first.Reply || counter.calls.Load() != 0 {
			t.Fatal("replay lost platform feedback", again, err)
		}
		return
	}
	if err != nil || len(pending) != 1 || pending[0].ToolName != "skill_run" || counter.calls.Load() != 0 {
		t.Fatalf("approval boundary: pending=%v calls=%d err=%v", pending, counter.calls.Load(), err)
	}
	input.RequestID, input.MessageID = "approved", "approved"
	input.ApprovedToolCalls = []governance.ApprovedToolCall{{ToolName: "skill_run", ArgumentsHash: pending[0].ArgumentsHash}}
	result, err := runtime.ChatWithScope(ctx, input)
	if scenario.DisabledSandbox {
		if err != nil || !strings.Contains(result.Reply, governance.CodeSandboxUnavailable) || counter.calls.Load() != 0 {
			t.Fatal("disabled sandbox feedback missing", result, err)
		}
		entries, err := journal.ListByRequest(ctx, input.Scope.TenantID, input.RequestID)
		if err != nil || len(entries) != 1 || entries[0].Status != toolexec.StatusFailed || entries[0].ErrorType != governance.CodeSandboxUnavailable {
			t.Fatal("known non-execution was not recorded correctly", entries, err)
		}
		return
	}
	if err != nil || !strings.Contains(result.Reply, "skill-ok") || counter.calls.Load() != 1 || !selected.sawBody.Load() {
		t.Fatalf("native skill/runtime/sandbox: reply=%s calls=%d body=%v err=%v", result.Reply, counter.calls.Load(), selected.sawBody.Load(), err)
	}
	entries, err := journal.ListByRequest(ctx, input.Scope.TenantID, "approved")
	if err != nil || len(entries) != 1 || entries[0].Status != toolexec.StatusSucceeded {
		t.Fatalf("skill execution journal: %v %v", entries, err)
	}
	// Existing request idempotency must not run the script a second time.
	if _, err = runtime.ChatWithScope(ctx, input); err != nil || counter.calls.Load() != 1 {
		t.Fatal("duplicate request executed again", err)
	}
}
func TestNativeSkillLoadingAndApprovedExecution(t *testing.T) { testSkillRuntime(t, nil) }
func TestNativeSkillBudgetFeedbackAndReplay(t *testing.T) {
	testSkillRuntime(t, nil, skillRuntimeScenario{BudgetOne: true})
}
func TestNativeSkillDisabledSandboxFeedback(t *testing.T) {
	testSkillRuntime(t, nil, skillRuntimeScenario{DisabledSandbox: true})
}
func TestSkillRunnerDockerIntegration(t *testing.T) {
	if os.Getenv("TEST_SANDBOX_DOCKER") != "1" {
		t.Skip("isolated Docker sandbox suite disabled")
	}
	d, err := workspace.NewDocker(context.Background(), workspace.Config{Image: "alpine:3.22", Timeout: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	testSkillRuntime(t, d)
}
