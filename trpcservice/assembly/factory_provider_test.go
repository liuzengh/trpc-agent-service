package assembly

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/internal/testutil"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactinmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	agentknowledge "trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

type recordingModelProvider struct {
	calls int
	model model.Model
}

func (p *recordingModelProvider) Model(context.Context, config.TenantConfig) (model.Model, error) {
	p.calls++
	return p.model, nil
}

type recordingKnowledgeProvider struct{ calls int }

func (p *recordingKnowledgeProvider) Knowledge(context.Context, config.TenantConfig, model.Model) (agentknowledge.Knowledge, error) {
	p.calls++
	return &emptyKnowledge{}, nil
}

type recordingMemoryProvider struct {
	service memory.Service
	model   model.Model
}

func (p *recordingMemoryProvider) MemoryBackend(_ context.Context, _ config.TenantConfig, configured model.Model) (MemoryBackend, error) {
	p.model = configured
	return MemoryBackend{Service: p.service, Tools: p.service.Tools()}, nil
}

type recordingArtifactProvider struct {
	calls   int
	service agentartifact.Service
}

func (p *recordingArtifactProvider) ArtifactService(context.Context, config.TenantConfig) (agentartifact.Service, error) {
	p.calls++
	return p.service, nil
}

func (p *recordingMemoryProvider) Memory(_ context.Context, _ config.TenantConfig, configured model.Model) (memory.Service, error) {
	p.model = configured
	return p.service, nil
}

type capturingModel struct {
	inner   model.Model
	mu      sync.Mutex
	texts   []string
	tools   [][]string
	streams []bool
}

func (m *capturingModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
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
	toolNames := make([]string, 0, len(request.Tools))
	for name := range request.Tools {
		toolNames = append(toolNames, name)
	}
	m.mu.Lock()
	m.texts = append(m.texts, text.String())
	m.tools = append(m.tools, toolNames)
	if request != nil {
		m.streams = append(m.streams, request.Stream)
	}
	m.mu.Unlock()
	return m.inner.GenerateContent(ctx, request)
}

func (m *capturingModel) Info() model.Info { return m.inner.Info() }

type fixedSummarySessionService struct {
	session.Service
	summary string
}

type fixedSessionProvider struct{ service session.Service }

func (p fixedSessionProvider) Session(context.Context, config.TenantConfig) (session.Service, error) {
	return p.service, nil
}

func (s *fixedSummarySessionService) GetSession(ctx context.Context, key session.Key, opts ...session.Option) (*session.Session, error) {
	sess, err := s.Service.GetSession(ctx, key, opts...)
	if err != nil || sess == nil || s.summary == "" {
		return sess, err
	}
	sess.Summaries = map[string]*session.Summary{
		session.SummaryFilterKeyAllContents: {Summary: s.summary},
	}
	return sess, nil
}

type emptyKnowledge struct{}

func (*emptyKnowledge) Search(context.Context, *agentknowledge.SearchRequest) (*agentknowledge.SearchResult, error) {
	return &agentknowledge.SearchResult{}, nil
}

func TestFactoryUsesTenantModelProviderOncePerConfigVersion(t *testing.T) {
	provider := &recordingModelProvider{model: testutil.NewFakeModel("tenant reply")}
	factory := NewFactoryWithModelProvider(provider, nil, nil, nil, nil, nil, nil)
	tenantConfig := config.TenantConfig{
		TenantID: "acme", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Model: config.ModelConfig{ProviderID: "primary", Name: "support"},
	}

	first, err := factory.Get(context.Background(), tenantConfig)
	if err != nil {
		t.Fatalf("Get() first error = %v", err)
	}
	second, err := factory.Get(context.Background(), tenantConfig)
	if err != nil {
		t.Fatalf("Get() second error = %v", err)
	}
	if first != second || provider.calls != 1 {
		t.Fatalf("cached runner/provider calls = %v/%d, want same runner/1", first == second, provider.calls)
	}

	tenantConfig.ConfigVersion++
	if _, err := factory.Get(context.Background(), tenantConfig); err != nil {
		t.Fatalf("Get() new version error = %v", err)
	}
	if provider.calls != 2 {
		t.Fatalf("provider calls = %d, want 2", provider.calls)
	}
}

func TestFactoryResolvesFrameworkKnowledgeOncePerConfigVersion(t *testing.T) {
	models := &recordingModelProvider{model: testutil.NewFakeModel("tenant reply")}
	knowledge := &recordingKnowledgeProvider{}
	factory := NewFactoryWithModelProvider(models, nil, nil, knowledge, nil, nil, nil)
	tenantConfig := config.TenantConfig{
		TenantID: "acme", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Model: config.ModelConfig{ProviderID: "primary", Name: "support"},
	}

	if _, err := factory.Get(context.Background(), tenantConfig); err != nil {
		t.Fatalf("Get() first error = %v", err)
	}
	if _, err := factory.Get(context.Background(), tenantConfig); err != nil {
		t.Fatalf("Get() second error = %v", err)
	}
	if knowledge.calls != 1 {
		t.Fatalf("knowledge provider calls = %d, want 1", knowledge.calls)
	}
}

func TestFactoryResolvesFrameworkArtifactServiceOncePerConfigVersion(t *testing.T) {
	models := &recordingModelProvider{model: testutil.NewFakeModel("tenant reply")}
	artifacts := &recordingArtifactProvider{service: artifactinmemory.NewService()}
	factory := NewFactoryWithModelProvider(models, nil, nil, nil, nil, artifacts, nil)
	tenantConfig := config.TenantConfig{
		TenantID: "acme", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Model: config.ModelConfig{ProviderID: "primary", Name: "support"},
	}

	if _, err := factory.Get(context.Background(), tenantConfig); err != nil {
		t.Fatalf("Get() first error = %v", err)
	}
	if _, err := factory.Get(context.Background(), tenantConfig); err != nil {
		t.Fatalf("Get() second error = %v", err)
	}
	if artifacts.calls != 1 {
		t.Fatalf("artifact provider calls = %d, want 1", artifacts.calls)
	}
	tenantConfig.ConfigVersion++
	if _, err := factory.Get(context.Background(), tenantConfig); err != nil {
		t.Fatalf("Get() new version error = %v", err)
	}
	if artifacts.calls != 2 {
		t.Fatalf("artifact provider calls = %d, want 2", artifacts.calls)
	}
}

func TestFactoryUsesFrameworkMemoryServiceAndTenantModel(t *testing.T) {
	captured := &capturingModel{inner: testutil.NewFakeModel("tenant reply")}
	models := &recordingModelProvider{model: captured}
	service := memoryinmemory.NewMemoryService(
		memoryinmemory.WithExtractor(newTenantMemoryExtractor(captured)),
		memoryinmemory.WithDisableAutoMemoryOnExternalContext(true),
	)
	if err := service.AddMemory(context.Background(), memory.UserKey{
		AppName: "acme/support", UserID: "user-1",
	}, "用户偏好简洁回答", []string{"preference"}); err != nil {
		t.Fatalf("AddMemory() error = %v", err)
	}
	if err := service.AddMemory(context.Background(), memory.UserKey{
		AppName: "other/support", UserID: "user-1",
	}, "另一个租户的私有记忆", []string{"private"}); err != nil {
		t.Fatalf("AddMemory() cross-tenant fixture error = %v", err)
	}
	memoryProvider := &recordingMemoryProvider{service: service}
	factory := NewFactoryWithModelProvider(models, nil, nil, nil, memoryProvider, nil, nil)
	t.Cleanup(func() { _ = factory.Close() })
	runnerInstance, err := factory.Get(context.Background(), config.TenantConfig{
		TenantID: "acme", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Model: config.ModelConfig{ProviderID: "primary", Name: "support"},
	})
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	events, err := runnerInstance.Run(context.Background(), "user-1", "session-1", model.NewUserMessage("你好"))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for range events {
	}
	if memoryProvider.model != captured {
		t.Fatal("memory provider did not receive the tenant runner model")
	}
	captured.mu.Lock()
	defer captured.mu.Unlock()
	if len(captured.texts) == 0 || !strings.Contains(captured.texts[0], "用户偏好简洁回答") {
		t.Fatalf("model request did not preload framework memory: %v", captured.texts)
	}
	if strings.Contains(captured.texts[0], "另一个租户的私有记忆") {
		t.Fatalf("model request leaked cross-tenant framework memory: %v", captured.texts)
	}
	if len(captured.tools) == 0 || !containsString(captured.tools[0], memory.SearchToolName) || containsString(captured.tools[0], memory.LoadToolName) {
		t.Fatalf("model request tools = %v, want %s only", captured.tools, memory.SearchToolName)
	}
	if containsString(captured.tools[0], memory.AddToolName) {
		t.Fatalf("auto-memory add tool was exposed to the agent: %v", captured.tools[0])
	}
}

func TestFactoryEnablesStreamingWithoutGenerationConfig(t *testing.T) {
	captured := &capturingModel{inner: testutil.NewFakeModel("ok")}
	factory := NewFactoryWithModelProvider(&recordingModelProvider{model: captured}, nil, nil, nil, nil, nil, nil)
	tenantConfig := config.TenantConfig{
		TenantID: "acme", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Model: config.ModelConfig{ProviderID: "primary", Name: "support"},
	}
	runnerInstance, err := factory.Get(context.Background(), tenantConfig)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	events, err := runnerInstance.Run(context.Background(), "user-1", "session-1", model.NewUserMessage("你好"))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for range events {
	}
	captured.mu.Lock()
	defer captured.mu.Unlock()
	if len(captured.streams) == 0 || !captured.streams[0] {
		t.Fatalf("request stream = %v, want true even without generation config", captured.streams)
	}
}

func TestFactoryInjectsFrameworkSessionSummaryIntoModelContext(t *testing.T) {
	captured := &capturingModel{inner: testutil.NewFakeModel("ok")}
	baseSessions := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = baseSessions.Close() })
	if _, err := baseSessions.CreateSession(context.Background(), session.Key{
		AppName: "acme/support", UserID: "user-1", SessionID: "session-summary",
	}, nil); err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}
	sessions := &fixedSummarySessionService{Service: baseSessions, summary: "此前已确认使用框架摘要"}
	factory := NewFactoryWithModelProvider(&recordingModelProvider{model: captured}, nil, fixedSessionProvider{service: sessions}, nil, nil, nil, nil)
	t.Cleanup(func() { _ = factory.Close() })
	runnerInstance, err := factory.Get(context.Background(), config.TenantConfig{
		TenantID: "acme", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Model: config.ModelConfig{ProviderID: "primary", Name: "support"},
	})
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	events, err := runnerInstance.Run(context.Background(), "user-1", "session-summary", model.NewUserMessage("继续"))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for range events {
	}
	captured.mu.Lock()
	defer captured.mu.Unlock()
	if len(captured.texts) == 0 || !strings.Contains(captured.texts[0], "此前已确认使用框架摘要") {
		t.Fatalf("model request did not include framework session summary: %v", captured.texts)
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
