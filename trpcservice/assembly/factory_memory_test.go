package assembly

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/testutil"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/memory/extractor"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type perTenantMemoryProvider struct {
	mu       sync.Mutex
	services map[string]memory.Service
}

func (p *perTenantMemoryProvider) MemoryBackend(_ context.Context, tenantConfig config.TenantConfig, configured model.Model) (MemoryBackend, error) {
	service := memoryinmemory.NewMemoryService(
		memoryinmemory.WithExtractor(newTenantMemoryExtractor(configured)),
		memoryinmemory.WithDisableAutoMemoryOnExternalContext(true),
	)
	p.mu.Lock()
	if p.services == nil {
		p.services = make(map[string]memory.Service)
	}
	p.services[tenantConfig.AppName()] = service
	p.mu.Unlock()
	return MemoryBackend{Service: service, Tools: service.Tools(), release: service.Close}, nil
}

func (p *perTenantMemoryProvider) service(appName string) memory.Service {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.services[appName]
}

type silentAuditSink struct{}

func (silentAuditSink) RecordToolAudit(context.Context, governance.ToolAuditEvent) error { return nil }

type searchThenChatModel struct {
	reply string
	mu    sync.Mutex
	calls int
	texts []string
}

func (m *searchThenChatModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.calls++
	call := m.calls
	m.texts = append(m.texts, requestTextFromModel(request))
	m.mu.Unlock()
	if call == 1 && request != nil && request.Tools[memory.SearchToolName] != nil {
		args, _ := json.Marshal(map[string]string{"query": "简洁"})
		return singleAssistantResponse(model.Message{
			Role: model.RoleAssistant,
			ToolCalls: []model.ToolCall{{
				Type:     "function",
				Function: model.FunctionDefinitionParam{Name: memory.SearchToolName, Arguments: args},
			}},
		}), nil
	}
	if request != nil && request.Tools[memory.AddToolName] != nil {
		return extractorNoopResponse(), nil
	}
	return singleAssistantResponse(model.NewAssistantMessage(m.reply)), nil
}

func (m *searchThenChatModel) Info() model.Info { return model.Info{Name: "search-then-chat"} }

func (m *searchThenChatModel) Texts() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.texts...)
}

func requestTextFromModel(request *model.Request) string {
	if request == nil {
		return ""
	}
	var text strings.Builder
	for _, message := range request.Messages {
		text.WriteString(message.Content)
		text.WriteByte('\n')
		for _, part := range message.ContentParts {
			if part.Text != nil {
				text.WriteString(*part.Text)
			}
			text.WriteByte('\n')
		}
	}
	return text.String()
}

func singleAssistantResponse(message model.Message) <-chan *model.Response {
	responses := make(chan *model.Response, 1)
	responses <- &model.Response{
		ID:      "factory-memory-response",
		Object:  "chat.completion",
		Choices: []model.Choice{{Index: 0, Message: message, FinishReason: model.StringPtr("stop")}},
		Done:    true,
	}
	close(responses)
	return responses
}

func extractorNoopResponse() <-chan *model.Response {
	return singleAssistantResponse(model.NewAssistantMessage(""))
}

func TestFactoryExtractsLongTermMemoryWithTenantModel(t *testing.T) {
	extracted := testutil.NewExtractingModel("tenant-a", "收到", "用户偏好简洁回答")
	models := &recordingModelProvider{model: extracted}
	provider := &perTenantMemoryProvider{}
	factory := NewFactoryWithModelProvider(models, nil, nil, nil, provider, nil, nil)
	t.Cleanup(func() { _ = factory.Close() })

	tenantConfig := config.TenantConfig{
		TenantID: "acme", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Model: config.ModelConfig{ProviderID: "primary", Name: "support"},
	}
	runnerInstance, err := factory.Get(context.Background(), tenantConfig)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	for turn := 0; turn < 3; turn++ {
		events, err := runnerInstance.Run(context.Background(), "user-1", "session-1", model.NewUserMessage("请记住我长期喜欢简洁回答"))
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		for range events {
		}
	}

	userKey := memory.UserKey{AppName: "acme/support", UserID: "user-1"}
	entries := waitForMemories(t, provider.service("acme/support"), userKey, 1)
	if entries[0].Memory.Memory != "用户偏好简洁回答" || entries[0].Memory.Kind != memory.KindFact {
		t.Fatalf("extracted memory = %+v", entries[0].Memory)
	}
}

func TestFactoryIsolatesMemoryExtractorModelsPerTenant(t *testing.T) {
	provider := &perTenantMemoryProvider{}
	factory := NewFactoryWithModelProvider(&tenantExtractingModels{
		models: map[string]model.Model{
			"acme/support":  testutil.NewExtractingModel("acme", "收到", "acme 租户的记忆"),
			"other/support": testutil.NewExtractingModel("other", "收到", "other 租户的记忆"),
		},
	}, nil, nil, nil, provider, nil, nil)
	t.Cleanup(func() { _ = factory.Close() })

	runTenantMemory(t, factory, "acme", "user-1")
	runTenantMemory(t, factory, "other", "user-1")

	acme := waitForMemories(t, provider.service("acme/support"), memory.UserKey{AppName: "acme/support", UserID: "user-1"}, 1)
	other := waitForMemories(t, provider.service("other/support"), memory.UserKey{AppName: "other/support", UserID: "user-1"}, 1)
	if acme[0].Memory.Memory != "acme 租户的记忆" {
		t.Fatalf("acme memory = %q", acme[0].Memory.Memory)
	}
	if other[0].Memory.Memory != "other 租户的记忆" {
		t.Fatalf("other memory = %q", other[0].Memory.Memory)
	}
}

func TestFactoryAllowsFrameworkMemorySearchUnderGovernance(t *testing.T) {
	chat := &searchThenChatModel{reply: "已根据记忆回答"}
	service := memoryinmemory.NewMemoryService(
		memoryinmemory.WithExtractor(extractor.NewExtractor(chat, extractor.WithChecker(extractor.CheckMessageThreshold(1)))),
		memoryinmemory.WithDisableAutoMemoryOnExternalContext(true),
	)
	if err := service.AddMemory(context.Background(), memory.UserKey{
		AppName: "acme/support", UserID: "user-1",
	}, "用户偏好简洁回答", []string{"preference"}); err != nil {
		t.Fatalf("AddMemory() error = %v", err)
	}
	callbacks, err := platformtool.NewGovernanceCallbacks(
		governance.NewStaticToolPolicy(GovernedToolNames(), nil),
		silentAuditSink{},
		platformtool.NewMemoryExecutionLedger(),
		time.Second,
	)
	if err != nil {
		t.Fatalf("NewGovernanceCallbacks() error = %v", err)
	}
	factory := NewFactoryWithModelProvider(
		&recordingModelProvider{model: chat},
		nil, nil, nil,
		&recordingMemoryProvider{service: service},
		nil, callbacks,
	)
	t.Cleanup(func() { _ = factory.Close() })
	runnerInstance, err := factory.Get(context.Background(), config.TenantConfig{
		TenantID: "acme", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Model: config.ModelConfig{ProviderID: "primary", Name: "support"},
	})
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	ctx := governance.WithInvocation(context.Background(), governance.Invocation{
		Execution: governance.ExecutionContext{TenantID: "acme", Role: "member", RequestID: "request-memory", TraceID: "trace-memory", PolicyVersion: "1"},
		Budget:    governance.NewCallBudget(4),
	})
	events, err := runnerInstance.Run(ctx, "user-1", "session-1", model.NewUserMessage("按我的偏好回答"))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for range events {
	}
	found := false
	for _, text := range chat.Texts() {
		if strings.Contains(text, "用户偏好简洁回答") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("governed memory_search did not return stored memory: %v", chat.Texts())
	}
}

func TestFactoryPreloadsMemoryNearUserMessage(t *testing.T) {
	chat := &roleRecordingModel{reply: "已根据记忆回答"}
	service := memoryinmemory.NewMemoryService(
		memoryinmemory.WithDisableAutoMemoryOnExternalContext(true),
	)
	if err := service.AddMemory(context.Background(), memory.UserKey{
		AppName: "acme/support", UserID: "user-1",
	}, "用户偏好简洁回答", []string{"preference"}); err != nil {
		t.Fatalf("AddMemory() error = %v", err)
	}
	factory := NewFactoryWithModelProvider(
		&recordingModelProvider{model: chat},
		nil, nil, nil,
		&recordingMemoryProvider{service: service},
		nil, nil,
	)
	t.Cleanup(func() { _ = factory.Close() })
	runnerInstance, err := factory.Get(context.Background(), config.TenantConfig{
		TenantID: "acme", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Model: config.ModelConfig{ProviderID: "primary", Name: "support"},
	})
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	events, err := runnerInstance.Run(context.Background(), "user-1", "session-preload", model.NewUserMessage("按我的偏好回答"))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for range events {
	}
	if len(chat.requests) == 0 {
		t.Fatal("model received no requests")
	}
	var inSystem, inUser bool
	for _, message := range chat.requests[0].Messages {
		if !strings.Contains(message.Content, "PRELOADED_USER_MEMORIES") {
			continue
		}
		switch message.Role {
		case model.RoleSystem:
			inSystem = true
		case model.RoleUser:
			inUser = true
		}
	}
	if inSystem {
		t.Fatal("preloaded memories were injected as system context")
	}
	if !inUser {
		t.Fatalf("preloaded memories were not injected near the user message: %+v", chat.requests[0].Messages)
	}
}

type roleRecordingModel struct {
	reply    string
	mu       sync.Mutex
	requests []*model.Request
}

func (m *roleRecordingModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cloned := *request
	cloned.Messages = append([]model.Message(nil), request.Messages...)
	m.mu.Lock()
	m.requests = append(m.requests, &cloned)
	m.mu.Unlock()
	return singleAssistantResponse(model.NewAssistantMessage(m.reply)), nil
}

func (m *roleRecordingModel) Info() model.Info { return model.Info{Name: "role-recording"} }

func TestFactoryInjectsFrameworkCurrentDateAndTimeTool(t *testing.T) {
	chat := &roleRecordingModel{reply: "现在是白天"}
	factory := NewFactoryWithModelProvider(&recordingModelProvider{model: chat}, nil, nil, nil, nil, nil, nil)
	t.Cleanup(func() { _ = factory.Close() })
	runnerInstance, err := factory.Get(context.Background(), config.TenantConfig{
		TenantID: "acme", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Model: config.ModelConfig{ProviderID: "primary", Name: "support"},
	})
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	events, err := runnerInstance.Run(context.Background(), "user-1", "session-time", model.NewUserMessage("现在几点"))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for range events {
	}
	if len(chat.requests) == 0 {
		t.Fatal("model received no requests")
	}
	request := chat.requests[0]
	if request.Tools["environment_context_current_time"] == nil {
		t.Fatalf("missing framework current-time tool: %v", request.Tools)
	}
	foundDate := false
	for _, message := range request.Messages {
		if message.Role == model.RoleSystem && strings.Contains(message.Content, "The current date is") {
			foundDate = true
			break
		}
	}
	if !foundDate {
		t.Fatalf("system prompt missing current date: %+v", request.Messages)
	}
}

type tenantExtractingModels struct {
	models map[string]model.Model
}

func (p *tenantExtractingModels) Model(_ context.Context, tenantConfig config.TenantConfig) (model.Model, error) {
	configured, ok := p.models[tenantConfig.AppName()]
	if !ok {
		return nil, fmt.Errorf("no extracting model for %s", tenantConfig.AppName())
	}
	return configured, nil
}

func runTenantMemory(t *testing.T, factory *Factory, tenantID, userID string) {
	t.Helper()
	runnerInstance, err := factory.Get(context.Background(), config.TenantConfig{
		TenantID: tenantID, AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Model: config.ModelConfig{ProviderID: "primary", Name: "support"},
	})
	if err != nil {
		t.Fatalf("Get(%s) error = %v", tenantID, err)
	}
	for turn := 0; turn < 3; turn++ {
		events, err := runnerInstance.Run(context.Background(), userID, tenantID+"-session", model.NewUserMessage("请记住这个长期偏好"))
		if err != nil {
			t.Fatalf("Run(%s) error = %v", tenantID, err)
		}
		for range events {
		}
	}
}

func waitForMemories(t *testing.T, service memory.Service, userKey memory.UserKey, want int) []*memory.Entry {
	t.Helper()
	if service == nil {
		t.Fatal("memory service is nil")
	}
	deadline := time.Now().Add(10 * time.Second)
	var entries []*memory.Entry
	for time.Now().Before(deadline) {
		var err error
		entries, err = service.ReadMemories(context.Background(), userKey, 20)
		if err != nil {
			t.Fatalf("ReadMemories() error = %v", err)
		}
		if len(entries) >= want {
			return entries
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d memories for %s/%s, got %+v", want, userKey.AppName, userKey.UserID, entries)
	return nil
}
