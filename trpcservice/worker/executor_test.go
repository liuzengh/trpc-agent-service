package worker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/Violet2314/trpc-agent-service/trpcservice/storage"
	"github.com/Violet2314/trpc-agent-service/trpcservice/tenant"
)

func TestExecutorFiltersToolsRedactsAndRecordsUsage(t *testing.T) {
	capturedModel := &captureModel{
		responseText: "customer account-123 uses api_key=super-secret",
		totalTokens:  17,
	}
	models := &fakeModelFactory{model: capturedModel}
	sessionService := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = sessionService.Close() })
	memoryService := inmemory.NewMemoryService()
	t.Cleanup(func() { _ = memoryService.Close() })
	backends := &fakeBackendFactory{
		session: sessionService,
		memory:  storage.MemoryBackend{Service: memoryService},
	}
	governor := &recordingGovernor{}
	executor, err := NewExecutor(
		backends,
		models,
		&fakeToolProvider{tools: []agenttool.Tool{
			fakeTool{name: "allowed_tool"},
			fakeTool{name: "denied_tool"},
		}},
		governor,
		NewPolicyRedactor(),
	)
	if err != nil {
		t.Fatalf("NewExecutor() error = %v", err)
	}
	snapshot := workerSnapshot()
	snapshot.App.Tools = []string{"allowed_tool"}
	snapshot.Tenant.Policy.RedactPatterns = []string{`account-[0-9]+`}
	messages := []storage.UserEvent{
		{ID: 1, SenderID: "user-1", Text: "first"},
		{ID: 2, SenderID: "user-1", Text: "second"},
	}

	stream, err := executor.Execute(
		context.Background(), snapshot, "tenant-a:webui:user-1", messages,
	)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	events := collectEvents(stream)

	request := capturedModel.lastRequest()
	if request == nil {
		t.Fatal("model did not receive a request")
	}
	if len(request.Tools) != 1 || request.Tools["allowed_tool"] == nil {
		t.Fatalf("model tools = %v, want only allowed_tool", toolNames(request.Tools))
	}
	var userContents []string
	for _, message := range request.Messages {
		if message.Role == model.RoleUser {
			userContents = append(userContents, message.Content)
		}
	}
	if len(userContents) < 2 ||
		userContents[len(userContents)-2] != "first" ||
		userContents[len(userContents)-1] != "second" {
		t.Fatalf("user messages = %v, want ordered batch", userContents)
	}
	if !hasEvent(events, "text_delta", "customer "+redactedValue+" uses "+redactedValue) {
		t.Fatalf("projected events = %#v, want redacted text", events)
	}
	if !hasUsage(events, 17) {
		t.Fatalf("projected events = %#v, want usage event", events)
	}
	if !hasType(events, "done") {
		t.Fatalf("projected events = %#v, want done", events)
	}
	if governor.recordedTokens != 17 {
		t.Fatalf("recorded tokens = %d, want 17", governor.recordedTokens)
	}
}

func TestExecutorPolicyDenialsDoNotCallModel(t *testing.T) {
	tests := []struct {
		name    string
		err     error
		message string
	}{
		{name: "budget", err: ErrBudgetDenied, message: "budget"},
		{name: "permission", err: ErrPermissionDenied, message: "not allowed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			models := &fakeModelFactory{model: &captureModel{}}
			executor, err := NewExecutor(
				&fakeBackendFactory{},
				models,
				&fakeToolProvider{},
				&recordingGovernor{authorizeErr: test.err},
				nil,
			)
			if err != nil {
				t.Fatalf("NewExecutor() error = %v", err)
			}
			stream, err := executor.Execute(
				context.Background(),
				workerSnapshot(),
				"tenant-a:webui:user-1",
				[]storage.UserEvent{{SenderID: "user-1", Text: "hello"}},
			)
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			events := collectEvents(stream)
			if models.calls != 0 {
				t.Fatalf("model factory calls = %d, want 0", models.calls)
			}
			if !hasErrorContaining(events, test.message) || !hasType(events, "done") {
				t.Fatalf("denial events = %#v", events)
			}
		})
	}
}

func TestExecutorResolvesConfirmationBeforeModel(t *testing.T) {
	sessionService := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = sessionService.Close() })
	memoryService := inmemory.NewMemoryService()
	t.Cleanup(func() { _ = memoryService.Close() })
	toolInstance := &callableTestTool{name: "delete_all"}
	pendingStore := &memoryPendingStore{call: testPendingCall(), present: true}
	gate, err := NewRedisConfirmationGate(pendingStore, &confirmationRegistry{
		dangerous: map[string]bool{"delete_all": true},
		tools:     map[string]agenttool.Tool{"delete_all": toolInstance},
	})
	if err != nil {
		t.Fatalf("NewRedisConfirmationGate() error = %v", err)
	}
	models := &fakeModelFactory{model: &captureModel{}}
	executor, err := NewExecutor(
		&fakeBackendFactory{
			session: sessionService,
			memory:  storage.MemoryBackend{Service: memoryService},
		},
		models,
		&fakeToolProvider{tools: []agenttool.Tool{toolInstance}},
		nil,
		nil,
		WithConfirmationGate(gate),
	)
	if err != nil {
		t.Fatalf("NewExecutor() error = %v", err)
	}
	snapshot := workerSnapshot()
	snapshot.App.Tools = []string{"delete_all"}
	stream, err := executor.Execute(
		context.Background(),
		snapshot,
		"tenant-a:webui:user-1",
		[]storage.UserEvent{{SenderID: "user-1", Text: "确认"}},
	)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	events := collectEvents(stream)
	if models.calls != 0 || toolInstance.calls != 1 || !hasDecision(events, "tool_confirmed") {
		t.Fatalf("model calls=%d tool calls=%d events=%#v", models.calls, toolInstance.calls, events)
	}
}

func TestExecutorValidationAndFactoryErrors(t *testing.T) {
	if _, err := NewExecutor(nil, &fakeModelFactory{}, &fakeToolProvider{}, nil, nil); err == nil {
		t.Fatal("NewExecutor() accepted nil backend factory")
	}
	executor, err := NewExecutor(
		&fakeBackendFactory{},
		&fakeModelFactory{err: errors.New("model unavailable")},
		&fakeToolProvider{},
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("NewExecutor() error = %v", err)
	}
	if _, err := executor.Execute(context.Background(), workerSnapshot(), "", nil); err == nil {
		t.Fatal("Execute() accepted empty session and messages")
	}
	if _, err := executor.Execute(
		context.Background(),
		workerSnapshot(),
		"session",
		[]storage.UserEvent{{SenderID: "user-1", Text: "hello"}},
	); err == nil {
		t.Fatal("Execute() did not return model factory error")
	}
}

func TestRewriteMessagesUsesStableGroupIdentity(t *testing.T) {
	messages, userID := rewriteMessages(
		"tenant-a:feishu:group:group-1",
		[]storage.UserEvent{
			{SenderID: "user-1", Text: "first"},
			{SenderID: "user-2", Text: "second"},
		},
	)
	if userID != "group:group-1" {
		t.Fatalf("group userID = %q, want group:group-1", userID)
	}
	if messages[0].Content != "[user-1] first" || messages[1].Content != "[user-2] second" {
		t.Fatalf("group messages = %#v", messages)
	}
}

type captureModel struct {
	mu           sync.Mutex
	request      *model.Request
	responseText string
	totalTokens  int
	calls        int
}

func (m *captureModel) GenerateContent(
	_ context.Context,
	request *model.Request,
) (<-chan *model.Response, error) {
	m.mu.Lock()
	m.request = request
	m.calls++
	m.mu.Unlock()
	output := make(chan *model.Response, 1)
	output <- &model.Response{
		ID:     "response-1",
		Object: model.ObjectTypeChatCompletion,
		Choices: []model.Choice{{
			Index:   0,
			Message: model.NewAssistantMessage(m.responseText),
		}},
		Usage: &model.Usage{TotalTokens: m.totalTokens},
		Done:  true,
	}
	close(output)
	return output, nil
}

func (m *captureModel) Info() model.Info {
	return model.Info{Name: "capture-model"}
}

func (m *captureModel) lastRequest() *model.Request {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.request
}

type fakeModelFactory struct {
	model model.Model
	err   error
	calls int
}

func (f *fakeModelFactory) Model(context.Context, tenant.ModelConfig) (model.Model, error) {
	f.calls++
	return f.model, f.err
}

type fakeBackendFactory struct {
	session session.Service
	memory  storage.MemoryBackend
	err     error
}

func (f *fakeBackendFactory) SessionService(tenant.AgentApp) (session.Service, error) {
	return f.session, f.err
}

func (f *fakeBackendFactory) MemoryBackend(tenant.AgentApp) (storage.MemoryBackend, error) {
	return f.memory, f.err
}

func (f *fakeBackendFactory) Close() error { return nil }

type fakeToolProvider struct {
	tools []agenttool.Tool
	err   error
}

func (f *fakeToolProvider) Tools(context.Context, tenant.Snapshot) ([]agenttool.Tool, error) {
	return f.tools, f.err
}

type fakeTool struct {
	name string
}

func (f fakeTool) Declaration() *agenttool.Declaration {
	return &agenttool.Declaration{
		Name:        f.name,
		Description: f.name,
		InputSchema: &agenttool.Schema{Type: "object"},
	}
}

type recordingGovernor struct {
	authorizeErr   error
	recordedTokens int
}

func (g *recordingGovernor) Authorize(
	context.Context,
	tenant.Snapshot,
	[]storage.UserEvent,
) error {
	return g.authorizeErr
}

func (g *recordingGovernor) RecordUsage(_ context.Context, _ string, tokens int) error {
	g.recordedTokens += tokens
	return nil
}

func workerSnapshot() tenant.Snapshot {
	return tenant.Snapshot{
		Tenant: tenant.Tenant{ID: "tenant-a", Name: "Tenant A", IsActive: true},
		App: tenant.AgentApp{
			ID:       "app-a",
			TenantID: "tenant-a",
			AppName:  "tenant-a-support",
			Model: tenant.ModelConfig{
				Provider: "openai-compatible",
				Model:    "test-model",
			},
			Backends: tenant.BackendSelection{Session: "redis", Memory: "pgvector"},
		},
		Binding: tenant.ChannelBinding{
			ID:       "binding-a",
			TenantID: "tenant-a",
			AppID:    "app-a",
			Channel:  "webui",
			RouteKey: "binding-a",
			IsActive: true,
		},
	}
}

func collectEvents(stream <-chan Event) []Event {
	var result []Event
	for event := range stream {
		result = append(result, event)
	}
	return result
}

func hasEvent(events []Event, eventType, text string) bool {
	for _, event := range events {
		if event.Type == eventType && event.Text == text {
			return true
		}
	}
	return false
}

func hasType(events []Event, eventType string) bool {
	for _, event := range events {
		if event.Type == eventType {
			return true
		}
	}
	return false
}

func hasUsage(events []Event, tokens int) bool {
	for _, event := range events {
		if event.Type == "usage" && event.UsageTokens == tokens {
			return true
		}
	}
	return false
}

func hasErrorContaining(events []Event, part string) bool {
	for _, event := range events {
		if event.Type == "error" && strings.Contains(event.Error, part) {
			return true
		}
	}
	return false
}

func toolNames(tools map[string]agenttool.Tool) []string {
	result := make([]string, 0, len(tools))
	for name := range tools {
		result = append(result, name)
	}
	return result
}
