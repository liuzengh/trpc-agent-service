package agent

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/coordination"
	"github.com/liuzengh/trpc-agent-service/trpcservice/idempotency"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
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
	compiler, err := NewRevisionCompiler(repository, NewTutorialModel(), false)
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
