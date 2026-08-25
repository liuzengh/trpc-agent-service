package agent

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	frameworkevent "trpc.group/trpc-go/trpc-agent-go/event"
)

type fakeProvider struct {
	mu        sync.Mutex
	response  ProviderResponse
	err       error
	started   chan struct{}
	cancelled chan struct{}
	requests  []ProviderRequest
}

func (p *fakeProvider) Complete(ctx context.Context, request ProviderRequest) (ProviderResponse, error) {
	p.mu.Lock()
	p.requests = append(p.requests, ProviderRequest{Messages: cloneMessages(request.Messages)})
	p.mu.Unlock()
	if p.started != nil {
		select {
		case p.started <- struct{}{}:
		default:
		}
	}
	if p.response.Text == "__block__" {
		<-ctx.Done()
		if p.cancelled != nil {
			closeOnce(p.cancelled)
		}
		return ProviderResponse{}, ctx.Err()
	}
	if p.err != nil {
		return ProviderResponse{}, p.err
	}
	return p.response, nil
}

func staticProviderFactory(provider Provider) ProviderFactory {
	return ProviderFactoryFunc(func(context.Context, tenant.TenantContext, AgentSpec) (Provider, error) {
		return provider, nil
	})
}

func closeOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func validContext() tenant.TenantContext {
	return tenant.TenantContext{
		TenantID: "tenant-a", AgentAppID: "agent-a", BindingID: "binding-a", Channel: "web",
		InternalUser: "user-a", SessionID: "session-a", RequestID: "request-a", MessageID: "message-a", TraceID: "trace-a",
		ConfigVersion: 3, BackendPolicy: tenant.BackendPolicy{Session: "memory", Memory: "memory", Vector: "none", Object: "none"},
	}
}

func validSpec() AgentSpec {
	return AgentSpec{TenantID: "tenant-a", AgentAppID: "agent-a", Version: 3, Name: "assistant", ModelProvider: "fake", SystemPrompt: "be concise"}
}

func validInput() AgentInput {
	return AgentInput{TenantContext: validContext(), Agent: validSpec(), History: []Message{{ID: "h1", Role: "user", Content: "previous"}}, Input: Message{ID: "message-a", Role: "user", Content: "hello"}}
}

func TestFactoryValidatesTenantBoundary(t *testing.T) {
	provider := &fakeProvider{response: ProviderResponse{Text: "ok"}}
	factory, err := NewFactory(RuntimeDependencies{ProviderFactory: staticProviderFactory(provider)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := factory.Build(context.Background(), validContext(), validSpec()); err != nil {
		t.Fatalf("expected valid runtime, got %v", err)
	}
	bad := validContext()
	bad.TenantID = "tenant-b"
	if _, err := factory.Build(context.Background(), bad, validSpec()); !errors.Is(err, ErrTenantMismatch) {
		t.Fatalf("expected tenant mismatch, got %v", err)
	}
	bad = validContext()
	bad.Permissions = []string{"agent.run"}
	before := append([]string(nil), bad.Permissions...)
	runtimeValue, err := factory.Build(context.Background(), bad, validSpec())
	if err != nil {
		t.Fatal(err)
	}
	bad.Permissions[0] = "changed"
	input := validInput()
	input.TenantContext.Permissions = []string{"agent.run"}
	if _, err := runtimeValue.Run(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, []string{"agent.run"}) {
		t.Fatal("test setup unexpectedly changed")
	}
}

func TestRuntimeUsesFrameworkRunnerAndMapsResult(t *testing.T) {
	provider := &fakeProvider{response: ProviderResponse{Text: "answer", InputTokens: 4, OutputTokens: 2, FinishType: "stop"}}
	factory, err := NewFactory(RuntimeDependencies{ProviderFactory: staticProviderFactory(provider), DrainTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	runtimeValue, err := factory.Build(context.Background(), validContext(), validSpec())
	if err != nil {
		t.Fatal(err)
	}
	input := validInput()
	result, err := runtimeValue.Run(context.Background(), input)
	if err != nil {
		t.Fatalf("run failed: %v", err)
	}
	if result.Text != "answer" {
		t.Fatalf("expected answer, got %q", result.Text)
	}
	if result.Usage.InputTokens != 4 || result.Usage.OutputTokens != 2 {
		t.Fatalf("unexpected usage: %+v", result.Usage)
	}
	provider.mu.Lock()
	defer provider.mu.Unlock()
	if len(provider.requests) != 1 || len(provider.requests[0].Messages) != 3 {
		t.Fatalf("unexpected provider requests: %+v", provider.requests)
	}
	if provider.requests[0].Messages[0].Role != "system" || provider.requests[0].Messages[0].Content != "be concise" || provider.requests[0].Messages[1].Content != "previous" || provider.requests[0].Messages[2].Content != "hello" {
		t.Fatalf("history was not mapped: %+v", provider.requests[0].Messages)
	}
}

func TestRuntimeCancellationPropagatesToProvider(t *testing.T) {
	provider := &fakeProvider{response: ProviderResponse{Text: "__block__"}, started: make(chan struct{}, 1), cancelled: make(chan struct{})}
	factory, err := NewFactory(RuntimeDependencies{ProviderFactory: staticProviderFactory(provider), DrainTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	runtimeValue, err := factory.Build(context.Background(), validContext(), validSpec())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	resultCh := make(chan error, 1)
	go func() {
		_, runErr := runtimeValue.Run(ctx, validInput())
		resultCh <- runErr
	}()
	select {
	case <-provider.started:
	case <-time.After(time.Second):
		t.Fatal("provider did not start")
	}
	cancel()
	select {
	case err := <-resultCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runtime did not return after cancellation")
	}
	select {
	case <-provider.cancelled:
	case <-time.After(time.Second):
		t.Fatal("provider did not receive cancellation")
	}
}

func TestDrainEventsTimesOutWithoutClosingChannel(t *testing.T) {
	events := make(chan *frameworkevent.Event)
	started := time.Now()
	_, err := drainEvents(context.Background(), events, 20*time.Millisecond)
	if !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("expected drain timeout, got %v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("drain timeout was not bounded")
	}
}

func TestDrainEventsKeepsOrderedEvents(t *testing.T) {
	events := make(chan *frameworkevent.Event, 2)
	events <- &frameworkevent.Event{Author: "user"}
	events <- &frameworkevent.Event{Author: "assistant"}
	close(events)
	result, err := drainEvents(context.Background(), events, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Events) != 2 || result.Events[0].Sequence != 1 || result.Events[1].Sequence != 2 {
		t.Fatalf("unexpected event order: %+v", result.Events)
	}
}
