package platform

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestRunnerIsIdempotent(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if err := store.SaveTenant(ctx, Tenant{ID: "t1", Agent: AgentConfig{Name: "a1"}}); err != nil {
		t.Fatal(err)
	}
	runner := Runner{Store: store, Responder: EchoResponder{}}
	in := Message{ID: "m1", TenantID: "t1", BindingID: "binding-1", Channel: "web", UserID: "u1", SessionID: "s1", Content: "hello"}
	_, first, err := runner.Run(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	_, second, err := runner.Run(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if first == "" || second != "" {
		t.Fatalf("expected first reply and empty duplicate reply, got %q and %q", first, second)
	}
	messages, err := store.Messages(ctx, "t1", "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected one request and one response, got %d messages", len(messages))
	}
}

type cancelingResponder struct{ cancel context.CancelFunc }

func (r cancelingResponder) Respond(_ context.Context, _ Tenant, _ []Message, _ string) (string, error) {
	r.cancel()
	return "should not commit", nil
}

func TestRunnerDoesNotCommitAssistantAfterResponderCancellation(t *testing.T) {
	store := NewMemoryStore()
	if err := store.SaveTenant(context.Background(), Tenant{ID: "t1", Agent: AgentConfig{Name: "a1"}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	_, _, err := (Runner{Store: store, Responder: cancelingResponder{cancel: cancel}}).Run(ctx, Message{ID: "m1", TenantID: "t1", BindingID: "b1", Channel: "web", SessionID: "s1", Content: "hello"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled run, got %v", err)
	}
	messages, err := store.Messages(context.Background(), "t1", "s1")
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].Role != "user" {
		t.Fatalf("assistant message was committed after cancellation: %+v", messages)
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if len(store.events[key("t1", "s1")]) != 0 {
		t.Fatal("assistant event was committed after cancellation")
	}
}

func TestMemoryStoreCommitAssistantIsAtomic(t *testing.T) {
	store := NewMemoryStore()
	message := Message{ID: "reply", TenantID: "t1", SessionID: "s1", Role: "assistant", Content: "ok"}
	event := Event{ID: "reply", TenantID: "t1", SessionID: "s1", Type: "assistant.completed", Payload: "ok"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.CommitAssistant(ctx, message, event); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	if len(store.messages[key("t1", "s1")]) != 0 || len(store.events[key("t1", "s1")]) != 0 {
		t.Fatal("canceled commit left partial state")
	}
}

func TestTenantIsolation(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if err := store.SaveTenant(ctx, Tenant{ID: "known"}); err != nil {
		t.Fatal(err)
	}
	_, _, err := (Runner{Store: store, Responder: EchoResponder{}}).Run(ctx, Message{ID: "m", TenantID: "unknown", BindingID: "binding-1", Channel: "web", SessionID: "s", Content: "x"})
	if err == nil {
		t.Fatal("expected unknown tenant to be rejected")
	}
	if err := store.SaveTenant(ctx, Tenant{ID: "other"}); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim(ctx, "other", "web", "binding-1", "m1")
	if err != nil || !claimed {
		t.Fatalf("same external message ID must be isolated by tenant: claimed=%v err=%v", claimed, err)
	}
}

func TestClaimHasOneConcurrentWinner(t *testing.T) {
	store := NewMemoryStore()
	var winners atomic.Int32
	const contenders = 100
	done := make(chan struct{}, contenders)
	for i := 0; i < contenders; i++ {
		go func() {
			claimed, err := store.Claim(context.Background(), "tenant-a", "web", "binding-a", "message-a")
			if err == nil && claimed {
				winners.Add(1)
			}
			done <- struct{}{}
		}()
	}
	for i := 0; i < contenders; i++ {
		<-done
	}
	if got := winners.Load(); got != 1 {
		t.Fatalf("expected one claim winner, got %d", got)
	}
}

func TestClaimHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewMemoryStore().Claim(ctx, "tenant-a", "web", "binding-a", "message-a"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled context, got %v", err)
	}
}

type stubFactory struct {
	runtime stubRuntime
	spec    agent.AgentSpec
}

func (f *stubFactory) Build(_ context.Context, _ tenant.TenantContext, spec agent.AgentSpec) (agent.AgentRuntime, error) {
	f.spec = spec
	return f.runtime, nil
}

type stubRuntime struct{}

func (stubRuntime) Run(_ context.Context, input agent.AgentInput) (agent.AgentResult, error) {
	return agent.AgentResult{Text: "runtime:" + input.Input.Content}, nil
}

func platformTenantContext() tenant.TenantContext {
	return tenant.TenantContext{
		TenantID: "tenant-a", AgentAppID: "agent-a", BindingID: "binding-a", Channel: "web",
		InternalUser: "user-a", SessionID: "session-a", RequestID: "request-a", MessageID: "message-a", TraceID: "trace-a",
		ConfigVersion: 2, BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "none"},
	}
}

func TestRuntimeResponderUsesVerifiedTenantContext(t *testing.T) {
	factory := &stubFactory{}
	responder := RuntimeResponder{Factory: factory}
	platformT := Tenant{ID: "tenant-a", Agent: AgentConfig{Name: "assistant", Model: "configured", SystemPrompt: "system"}}
	ctx := tenant.WithContext(context.Background(), platformTenantContext())
	out, err := responder.Respond(ctx, platformT, []Message{{ID: "h1", Role: "user", Content: "history"}}, "hello")
	if err != nil {
		t.Fatal(err)
	}
	if out != "runtime:hello" {
		t.Fatalf("unexpected runtime output %q", out)
	}
	if factory.spec.TenantID != "tenant-a" || factory.spec.AgentAppID != "agent-a" || factory.spec.Version != 2 {
		t.Fatalf("runtime spec crossed tenant boundary: %+v", factory.spec)
	}
}

func TestRuntimeResponderRejectsMissingOrMismatchedContext(t *testing.T) {
	responder := RuntimeResponder{Factory: &stubFactory{}}
	platformT := Tenant{ID: "tenant-a", Agent: AgentConfig{Name: "assistant", Model: "configured"}}
	if _, err := responder.Respond(context.Background(), platformT, nil, "hello"); err == nil {
		t.Fatal("expected missing tenant context to be rejected")
	}
	ctx := tenant.WithContext(context.Background(), platformTenantContext())
	other := platformT
	other.ID = "tenant-b"
	if _, err := responder.Respond(ctx, other, nil, "hello"); err == nil {
		t.Fatal("expected tenant mismatch to be rejected")
	}
}
