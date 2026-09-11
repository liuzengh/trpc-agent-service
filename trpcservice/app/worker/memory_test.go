package worker

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/llm"
	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/bus"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/storage"

	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

// scriptedModel replays a fixed response sequence and keeps every request, so a
// test can assert both what the platform stored and what it actually sent to
// the model.
type scriptedModel struct {
	mu        sync.Mutex
	responses []*model.Response
	requests  []*model.Request
}

func (m *scriptedModel) Info() model.Info { return model.Info{Name: "scripted"} }

func (m *scriptedModel) GenerateContent(_ context.Context, req *model.Request) (<-chan *model.Response, error) {
	m.mu.Lock()
	m.requests = append(m.requests, req)
	idx := len(m.requests) - 1
	resp := finalAssistant("(no scripted response)")
	if idx < len(m.responses) {
		resp = m.responses[idx]
	}
	m.mu.Unlock()

	ch := make(chan *model.Response, 1)
	ch <- resp
	close(ch)
	return ch, nil
}

func (m *scriptedModel) setScript(responses ...*model.Response) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.responses = responses
	m.requests = nil
}

func (m *scriptedModel) sentToModel() []*model.Request {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*model.Request(nil), m.requests...)
}

func finalAssistant(text string) *model.Response {
	return &model.Response{
		Done: true,
		Choices: []model.Choice{{
			Message: model.Message{Role: model.RoleAssistant, Content: text},
		}},
	}
}

// memoryAddCall scripts a model reply that asks the platform to remember
// something: the framework executes the memory tool, then loops back for the
// final answer.
func memoryAddCall(t *testing.T, content string) *model.Response {
	t.Helper()
	args, err := json.Marshal(map[string]any{"memory": content, "topics": []string{"preference"}})
	if err != nil {
		t.Fatalf("marshal memory_add args: %v", err)
	}
	return &model.Response{
		Done: true,
		Choices: []model.Choice{{
			Message: model.Message{
				Role: model.RoleAssistant,
				ToolCalls: []model.ToolCall{{
					ID: "call-memory-add",
					Function: model.FunctionDefinitionParam{
						Name:      memory.AddToolName,
						Arguments: args,
					},
				}},
			},
		}},
	}
}

// memoryFixture builds a worker whose tenants, agents and memory are all
// in-process, plus the model the agents will be built with.
func memoryFixture(t *testing.T) (*Worker, *agent.Manager, *scriptedModel, *storage.Router) {
	t.Helper()
	ctx := context.Background()
	mdl := &scriptedModel{}
	reg := llm.NewRegistry(func(context.Context, llm.Endpoint) (model.Model, error) { return mdl, nil })
	if err := reg.Create(ctx, llm.Endpoint{
		ID: "e-mem", Scope: llm.ScopeTenant, TenantID: "t-mem", Name: "main",
		Provider: "openai", BaseURL: "http://localhost", ModelName: "m",
	}); err != nil {
		t.Fatalf("endpoint: %v", err)
	}
	agents := agent.NewManager(reg)
	if err := agents.Create(ctx, agent.Agent{ID: "a-mem", TenantID: "t-mem", Name: "helper"}); err != nil {
		t.Fatalf("agent: %v", err)
	}
	if _, err := agents.Publish(ctx, "a-mem", agent.RuntimeProfile{
		SystemPrompt: "be helpful", EndpointID: "e-mem",
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	router := storage.NewRouter(tenant.NewManager(),
		storage.SessionConfig{Backend: storage.BackendInMemory},
		storage.MemoryConfig{Backend: storage.BackendInMemory},
	)
	return New(nil, agents, nil, nil, router, nil, nil, nil, nil), agents, mdl, router
}

func inbound(id, tenantID, agentID, sessionID, userID, text string) *bus.Message {
	msg := model.NewUserMessage(text)
	return &bus.Message{
		ID: id, TenantID: tenantID, AgentID: agentID, SessionID: sessionID,
		Channel: "admin", UserID: userID, Content: &msg,
	}
}

// TestMemoryIsOffUntilAPreloadBudgetIsSet pins the switch: one knob decides
// whether a turn resolves a memory backend and pays for the memory tool
// schemas. A disabled node must not acquire either.
func TestMemoryIsOffUntilAPreloadBudgetIsSet(t *testing.T) {
	w, agents, _, _ := memoryFixture(t)
	ctx := context.Background()

	if svc, tools := w.memoryForTurn(ctx, "t-mem"); svc != nil || len(tools) != 0 {
		t.Fatalf("memory must stay off while the preload budget is 0, got svc=%v tools=%d", svc, len(tools))
	}

	agents.SetPreloadMemory(10)
	svc, tools := w.memoryForTurn(ctx, "t-mem")
	if svc == nil {
		t.Fatal("expected a memory service once the preload budget is set")
	}
	if len(tools) == 0 {
		t.Fatal("expected memory tools once the preload budget is set")
	}
	names := map[string]bool{}
	for _, tl := range tools {
		names[tl.Declaration().Name] = true
	}
	for _, want := range []string{memory.AddToolName, memory.SearchToolName} {
		if !names[want] {
			t.Errorf("memory tools %v missing %q", names, want)
		}
	}
	// The framework's default tool set excludes the destructive ones; the
	// platform relies on that default rather than re-listing the tools.
	for _, unwanted := range []string{memory.DeleteToolName, memory.ClearToolName} {
		if names[unwanted] {
			t.Errorf("memory tools %v should not expose %q", names, unwanted)
		}
	}
}

// TestMemoryToolsMakeTheAgentRememberAcrossSessions is the end-to-end contract
// of the memory feature: the model calls memory_add during one turn, the entry
// lands under <tenant, user>, and a later turn of the same user gets it
// injected into the prompt (preload) — while another tenant never sees it.
func TestMemoryToolsMakeTheAgentRememberAcrossSessions(t *testing.T) {
	w, agents, mdl, router := memoryFixture(t)
	agents.SetPreloadMemory(10)
	ctx := context.Background()

	const remembered = "用户喜欢美式咖啡"
	mdl.setScript(memoryAddCall(t, remembered), finalAssistant("好的，我记住了"))
	if _, _, _, err := w.run(ctx, "a-mem", inbound("m-1", "t-mem", "a-mem", "s-1", "u-1", "我喜欢美式咖啡"), "lock-1"); err != nil {
		t.Fatalf("first turn: %v", err)
	}

	mem, err := router.Memories(ctx, "t-mem")
	if err != nil {
		t.Fatalf("memory backend: %v", err)
	}
	stored, err := mem.Service().ReadMemories(ctx, memory.UserKey{AppName: "t-mem", UserID: "u-1"}, 10)
	if err != nil {
		t.Fatalf("read memories: %v", err)
	}
	if len(stored) != 1 || !strings.Contains(stored[0].Memory.Memory, "美式咖啡") {
		t.Fatalf("stored memories = %+v, want the fact the model asked to remember", stored)
	}

	// A different session of the same user: the preload budget injects what the
	// agent already knows without any tool call.
	mdl.setScript(finalAssistant("你平时喝美式"))
	if _, _, _, err := w.run(ctx, "a-mem", inbound("m-2", "t-mem", "a-mem", "s-2", "u-1", "我平时喝什么"), "lock-2"); err != nil {
		t.Fatalf("second turn: %v", err)
	}
	sent := mdl.sentToModel()
	if len(sent) == 0 {
		t.Fatal("model was never called")
	}
	if !requestMentions(sent[len(sent)-1], "美式咖啡") {
		t.Errorf("preloaded memory missing from the second turn's prompt: %+v", sent[len(sent)-1].Messages)
	}

	// Tenant isolation: another tenant's user key is a different memory space.
	other, err := mem.Service().ReadMemories(ctx, memory.UserKey{AppName: "t-other", UserID: "u-2"}, 10)
	if err != nil {
		t.Fatalf("read other tenant memories: %v", err)
	}
	if len(other) != 0 {
		t.Errorf("tenant t-other sees %d memories from t-mem, want 0", len(other))
	}
}

// TestMemoryIsNotInjectedWhenTheFeatureIsOff is the negative half of the
// preload contract: with the budget at 0 the stored memory must not leak into
// the prompt.
func TestMemoryIsNotInjectedWhenTheFeatureIsOff(t *testing.T) {
	w, _, mdl, router := memoryFixture(t)
	ctx := context.Background()

	mem, err := router.Memories(ctx, "t-mem")
	if err != nil {
		t.Fatalf("memory backend: %v", err)
	}
	if err := mem.Add(ctx, "t-mem", "u-1", "用户喜欢美式咖啡", nil); err != nil {
		t.Fatalf("seed memory: %v", err)
	}

	if _, _, _, err := w.run(ctx, "a-mem", inbound("m-1", "t-mem", "a-mem", "s-1", "u-1", "我平时喝什么"), "lock-1"); err != nil {
		t.Fatalf("turn: %v", err)
	}
	if requestMentions(mdl.sentToModel()[0], "美式咖啡") {
		t.Error("memory leaked into the prompt while the feature was disabled")
	}
}

func requestMentions(req *model.Request, needle string) bool {
	if req == nil {
		return false
	}
	for _, msg := range req.Messages {
		if strings.Contains(msg.Content, needle) {
			return true
		}
	}
	return false
}
