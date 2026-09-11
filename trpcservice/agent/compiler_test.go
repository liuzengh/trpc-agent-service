package agent

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/approval"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	agentcore "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestRevisionCompilerCompilesAndCachesAgent(t *testing.T) {
	repository := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	t.Cleanup(func() { _ = repository.Close() })
	compiler, err := NewRevisionCompiler(repository, NewTutorialModel(), false)
	if err != nil {
		t.Fatalf("new compiler: %v", err)
	}
	scope := runtimecontext.TutorialScope()
	first, err := compiler.Compile(context.Background(), scope)
	if err != nil {
		t.Fatalf("compile first: %v", err)
	}
	second, err := compiler.Compile(context.Background(), scope)
	if err != nil {
		t.Fatalf("compile second: %v", err)
	}
	if first != second {
		t.Fatal("same revision did not return cached Agent")
	}
	if first.Info().Name != "tutorial-agent" {
		t.Fatalf("Agent name = %q", first.Info().Name)
	}
}

func TestRevisionCompilerConcurrentCache(t *testing.T) {
	repository := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	t.Cleanup(func() { _ = repository.Close() })
	compiler, err := NewRevisionCompiler(repository, NewTutorialModel(), false)
	if err != nil {
		t.Fatalf("new compiler: %v", err)
	}
	const count = 16
	results := make(chan any, count)
	var group sync.WaitGroup
	for i := 0; i < count; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			compiled, compileErr := compiler.Compile(
				context.Background(),
				runtimecontext.TutorialScope(),
			)
			if compileErr != nil {
				results <- compileErr
				return
			}
			results <- compiled
		}()
	}
	group.Wait()
	close(results)
	var expected any
	for result := range results {
		if compileErr, ok := result.(error); ok {
			t.Fatalf("compile error: %v", compileErr)
		}
		if expected == nil {
			expected = result
			continue
		}
		if expected != result {
			t.Fatal("concurrent compilation returned different Agent instances")
		}
	}
}

func TestRevisionCompilerRejectsScopeMismatch(t *testing.T) {
	repository := controlplane.NewMemoryRepository(controlplane.DefaultBootstrapData())
	t.Cleanup(func() { _ = repository.Close() })
	compiler, err := NewRevisionCompiler(repository, NewTutorialModel(), false)
	if err != nil {
		t.Fatalf("new compiler: %v", err)
	}
	scope, err := runtimecontext.NewScope(
		"tutorial-tenant",
		"other-app",
		"tutorial-revision-1",
		"http",
		"tutorial-http-binding",
	)
	if err != nil {
		t.Fatalf("new scope: %v", err)
	}
	if _, err := compiler.Compile(context.Background(), scope); err == nil ||
		!strings.Contains(err.Error(), "scope mismatch") {
		t.Fatalf("compile mismatch error = %v", err)
	}
}

func TestRevisionCompilerRejectsUnknownAgentTypeAndFields(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.Revisions[0].AgentType = "unknown"
	repository := controlplane.NewMemoryRepository(data)
	compiler, err := NewRevisionCompiler(repository, NewTutorialModel(), false)
	if err != nil {
		t.Fatalf("new compiler: %v", err)
	}
	if _, err := compiler.Compile(context.Background(), runtimecontext.TutorialScope()); err == nil ||
		!strings.Contains(err.Error(), "unsupported Agent type") {
		t.Fatalf("unknown type error = %v", err)
	}
	_ = repository.Close()

	data = controlplane.DefaultBootstrapData()
	data.Revisions[0].AgentConfig = json.RawMessage(`{
        "name":"agent","instruction":"hello","unknown":true
    }`)
	repository = controlplane.NewMemoryRepository(data)
	t.Cleanup(func() { _ = repository.Close() })
	compiler, err = NewRevisionCompiler(repository, NewTutorialModel(), false)
	if err != nil {
		t.Fatalf("new compiler: %v", err)
	}
	if _, err := compiler.Compile(context.Background(), runtimecontext.TutorialScope()); err == nil ||
		!strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field error = %v", err)
	}
}

func TestRevisionCompilerBuildsRevisionModelFromSecretEnvironment(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.Revisions[0].ModelConfig = json.RawMessage(`{
        "source":"revision",
        "provider":"openai",
        "name":"tenant-model",
        "base_url":"https://model.example/v1",
        "api_key_env":"TENANT_MODEL_TEST_KEY"
    }`)
	t.Setenv("TENANT_MODEL_TEST_KEY", "test-secret")
	repository := controlplane.NewMemoryRepository(data)
	t.Cleanup(func() { _ = repository.Close() })
	store, _ := secret.NewEnvStore([]secret.Grant{{TenantID: "tutorial-tenant", Purpose: secret.Model, Reference: "env://TENANT_MODEL_TEST_KEY"}})
	compiler, err := NewRevisionCompiler(repository, NewTutorialModel(), false, WithSecretStore(store))
	if err != nil {
		t.Fatalf("new compiler: %v", err)
	}
	compiled, err := compiler.Compile(context.Background(), runtimecontext.TutorialScope())
	if err != nil {
		t.Fatalf("compile revision model: %v", err)
	}
	if compiled.Info().Name != "tutorial-agent" {
		t.Fatalf("compiled Agent name = %q", compiled.Info().Name)
	}
}

func TestRuntimeUsesCompiledRevisionAgent(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.Revisions[0].AgentConfig = json.RawMessage(`{
        "name":"tenant-specific-agent",
        "description":"compiled from revision",
        "instruction":"Answer using this tenant revision."
    }`)
	repository := controlplane.NewMemoryRepository(data)
	t.Cleanup(func() { _ = repository.Close() })
	selectedModel := NewTutorialModel()
	compiler, err := NewRevisionCompiler(repository, selectedModel, false)
	if err != nil {
		t.Fatalf("new compiler: %v", err)
	}
	sessionService := inmemory.NewSessionService()
	coordinator := coordination.NewLocalCoordinator()
	idempotencyStore := idempotency.NewLocalStore()
	runtime, err := NewRuntimeWithCompilerServices(
		selectedModel,
		compiler,
		sessionService,
		coordinator,
		idempotencyStore,
		false,
	)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	result, err := runtime.ChatWithScope(context.Background(), ChatInput{
		Scope:     runtimecontext.TutorialScope(),
		MessageID: "compiled-agent-message",
		UserID:    "alice",
		SessionID: "compiled-agent-session",
		Text:      "hello",
	})
	if err != nil {
		t.Fatalf("chat with compiled Agent: %v", err)
	}
	if result.AgentName != "tenant-specific-agent" {
		t.Fatalf("Agent name = %q", result.AgentName)
	}
}

func TestRevisionCompilerAddsOnlyRevisionTools(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.Revisions[0].ToolPolicy = json.RawMessage(`{
        "allowed_tools":["echo"],
        "dangerous_tools":[],
        "max_tool_calls":1
    }`)
	repository := controlplane.NewMemoryRepository(data)
	t.Cleanup(func() { _ = repository.Close() })
	compiler, err := NewRevisionCompiler(
		repository,
		NewTutorialModel(),
		false,
		WithToolCatalog(platformtool.DefaultCatalog()),
	)
	if err != nil {
		t.Fatalf("new compiler: %v", err)
	}
	compiled, err := compiler.Compile(context.Background(), runtimecontext.TutorialScope())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	provider, ok := compiled.(interface{ Tools() []agenttool.Tool })
	if !ok {
		t.Fatalf("compiled Agent %T does not expose tools", compiled)
	}
	tools := provider.Tools()
	if len(tools) != 1 || tools[0].Declaration().Name != "echo" {
		t.Fatalf("tools=%+v", tools)
	}
}

func TestRevisionCompilerAuditsToolPermissionDecision(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.Revisions[0].ToolPolicy = json.RawMessage(`{"allowed_tools":["echo"]}`)
	repository := controlplane.NewMemoryRepository(data)
	auditWriter := audit.NewMemoryWriter()
	t.Cleanup(func() {
		_ = auditWriter.Close()
		_ = repository.Close()
	})
	compiler, err := NewRevisionCompiler(
		repository,
		NewTutorialModel(),
		false,
		WithToolCatalog(platformtool.DefaultCatalog()),
		WithAuditWriter(auditWriter),
	)
	if err != nil {
		t.Fatalf("new compiler: %v", err)
	}
	input := ChatInput{
		Scope: runtimecontext.TutorialScope(), MessageID: "message-1",
		RequestID: "request-1", UserID: "alice", SessionID: "session-1",
	}
	options, err := compiler.RunPolicyOptions(context.Background(), input)
	if err != nil {
		t.Fatalf("run policy options: %v", err)
	}
	runOptions := agentcore.NewRunOptions(options...)
	decision, err := runOptions.ToolPermissionPolicy.CheckToolPermission(
		context.Background(),
		&agenttool.PermissionRequest{ToolName: "echo", ToolCallID: "call-1"},
	)
	if err != nil || decision.Action != agenttool.PermissionActionAllow {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
	events := auditWriter.Events()
	if len(events) != 1 || events[0].ToolName != "echo" ||
		events[0].Decision != "tool_allow" || events[0].RequestID != "request-1" {
		t.Fatalf("events=%+v", events)
	}
}

func TestRevisionCompilerLoadsUsagePricing(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.Revisions[0].ModelConfig = json.RawMessage(`{
        "source":"startup_env",
        "prompt_cost_per_million":2.5,
        "completion_cost_per_million":10
    }`)
	repository := controlplane.NewMemoryRepository(data)
	t.Cleanup(func() { _ = repository.Close() })
	compiler, err := NewRevisionCompiler(repository, NewTutorialModel(), false)
	if err != nil {
		t.Fatalf("new compiler: %v", err)
	}
	pricing, err := compiler.UsagePricing(context.Background(), runtimecontext.TutorialScope())
	if err != nil {
		t.Fatalf("load pricing: %v", err)
	}
	if cost := pricing.Cost(1_000_000, 500_000); cost != 7.5 {
		t.Fatalf("cost=%v", cost)
	}
}

func TestRevisionCompilerCreatesDurableApproval(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.Revisions[0].ToolPolicy = json.RawMessage(`{
        "allowed_tools":["dangerous_demo"],
        "dangerous_tools":["dangerous_demo"]
    }`)
	repository := controlplane.NewMemoryRepository(data)
	approvals := approval.NewMemoryRepository()
	t.Cleanup(func() {
		_ = approvals.Close()
		_ = repository.Close()
	})
	compiler, err := NewRevisionCompiler(
		repository, NewTutorialModel(), false,
		WithToolCatalog(platformtool.DefaultCatalog()),
		WithApprovalRepository(approvals),
	)
	if err != nil {
		t.Fatalf("new compiler: %v", err)
	}
	input := ChatInput{
		Scope: runtimecontext.TutorialScope(), MessageID: "message-approval",
		RequestID: "request-approval", UserID: "alice", SessionID: "session-approval",
		Text: "perform dangerous action", ReplyTarget: "alice",
	}
	options, err := compiler.RunPolicyOptions(context.Background(), input)
	if err != nil {
		t.Fatalf("run policy options: %v", err)
	}
	runOptions := agentcore.NewRunOptions(options...)
	decision, err := runOptions.ToolPermissionPolicy.CheckToolPermission(
		context.Background(), &agenttool.PermissionRequest{
			ToolName: "dangerous_demo", ToolCallID: "call-approval", Arguments: []byte(`{"ok":true}`),
		},
	)
	if err != nil || decision.Action != agenttool.PermissionActionAsk {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
	pending, err := approvals.ListPendingByRequest(
		context.Background(), "tutorial-tenant", "request-approval",
	)
	if err != nil || len(pending) != 1 || pending[0].ToolName != "dangerous_demo" {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
}

func TestRevisionCompilerAllowsFrameworkKnowledgeTool(t *testing.T) {
	data := controlplane.DefaultBootstrapData()
	data.Revisions[0].KnowledgeConfig = json.RawMessage(`{"enabled":true}`)
	repository := controlplane.NewMemoryRepository(data)
	t.Cleanup(func() { _ = repository.Close() })
	compiler, err := NewRevisionCompiler(repository, NewTutorialModel(), false)
	if err != nil {
		t.Fatalf("new compiler: %v", err)
	}
	options, err := compiler.RunPolicyOptions(context.Background(), ChatInput{
		Scope: runtimecontext.TutorialScope(), UserID: "alice",
	})
	if err != nil {
		t.Fatalf("run options: %v", err)
	}
	runOptions := agentcore.NewRunOptions(options...)
	decision, err := runOptions.ToolPermissionPolicy.CheckToolPermission(
		context.Background(), &agenttool.PermissionRequest{ToolName: "knowledge_search"},
	)
	if err != nil || decision.Action != agenttool.PermissionActionAllow {
		t.Fatalf("decision=%+v err=%v", decision, err)
	}
}
