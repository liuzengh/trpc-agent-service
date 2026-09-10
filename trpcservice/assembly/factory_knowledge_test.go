package assembly

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	agentknowledge "trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/document"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type staticKnowledge struct {
	documents []*document.Document
}

func (k *staticKnowledge) Search(ctx context.Context, req *agentknowledge.SearchRequest) (*agentknowledge.SearchResult, error) {
	res := &agentknowledge.SearchResult{
		Documents: make([]*agentknowledge.Result, 0, len(k.documents)),
	}
	for _, doc := range k.documents {
		res.Documents = append(res.Documents, &agentknowledge.Result{
			Document: doc,
			Score:    1.0,
		})
		if res.Document == nil {
			res.Document = doc
			res.Score = 1.0
			res.Text = doc.Content
		}
	}
	return res, nil
}

type staticKnowledgeProvider struct {
	knowledge agentknowledge.Knowledge
}

func (p *staticKnowledgeProvider) Knowledge(context.Context, config.TenantConfig, model.Model) (agentknowledge.Knowledge, error) {
	return p.knowledge, nil
}

func TestGovernedToolNamesIncludesFrameworkKnowledgeSearch(t *testing.T) {
	names := GovernedToolNames()
	nameSet := make(map[string]bool, len(names))
	for _, n := range names {
		nameSet[n] = true
	}
	if !nameSet[knowledgeSearchTool] {
		t.Fatalf("GovernedToolNames() missing knowledge tool %q; got: %v", knowledgeSearchTool, names)
	}
}

func TestGovernedToolPolicyAuthorizesKnowledgeSearchTools(t *testing.T) {
	policy := governance.NewStaticToolPolicy(GovernedToolNames(), []string{"password", "token", "secret", "api_key", "authorization"})
	callbacks, err := platformtool.NewGovernanceCallbacks(policy, silentAuditSink{}, platformtool.NewMemoryExecutionLedger(), time.Second)
	if err != nil {
		t.Fatalf("NewGovernanceCallbacks() error = %v", err)
	}

	execCtx := governance.ExecutionContext{
		TenantID:      "acme",
		Role:          "member",
		RequestID:     "request-knowledge-policy",
		TraceID:       "trace-knowledge",
		PolicyVersion: "1",
	}

	req := governance.ToolRequest{
		Name:      knowledgeSearchTool,
		Arguments: []byte(`{"query":"退款政策"}`),
	}
	if err := policy.Authorize(context.Background(), execCtx, req); err != nil {
		t.Fatalf("policy.Authorize(%q) error = %v, want allowed", knowledgeSearchTool, err)
	}

	_ = callbacks
}

type knowledgeSearchThenChatModel struct {
	reply string
	mu    sync.Mutex
	calls int
	texts []string
}

func (m *knowledgeSearchThenChatModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.calls++
	call := m.calls
	m.texts = append(m.texts, requestTextFromModel(request))
	m.mu.Unlock()

	if call == 1 && request != nil && request.Tools["knowledge_search"] != nil {
		args, _ := json.Marshal(map[string]any{"query": "退款"})
		return singleAssistantResponse(model.Message{
			Role: model.RoleAssistant,
			ToolCalls: []model.ToolCall{{
				ID:       "call-knowledge-1",
				Type:     "function",
				Function: model.FunctionDefinitionParam{Name: "knowledge_search", Arguments: args},
			}},
		}), nil
	}
	return singleAssistantResponse(model.NewAssistantMessage(m.reply)), nil
}

func (m *knowledgeSearchThenChatModel) Info() model.Info {
	return model.Info{Name: "knowledge-search-chat"}
}

func (m *knowledgeSearchThenChatModel) Texts() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.texts...)
}

func TestFactoryAllowsKnowledgeSearchUnderGovernance(t *testing.T) {
	chat := &knowledgeSearchThenChatModel{reply: "知识库查询结果已参考"}

	kb := &staticKnowledge{
		documents: []*document.Document{
			{ID: "doc-1", Name: "退货政策.md", Content: "退款政策：支持7天无理由退款"},
		},
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
		nil, nil,
		&staticKnowledgeProvider{knowledge: kb},
		nil, nil, callbacks,
	)
	t.Cleanup(func() { _ = factory.Close() })

	runnerInstance, err := factory.Get(context.Background(), config.TenantConfig{
		TenantID:      "acme",
		AppCode:       "support",
		Status:        config.AgentActive,
		ConfigVersion: 1,
		Instruction:   "你是客服诺娃。",
		Model:         config.ModelConfig{ProviderID: "primary", Name: "support"},
	})
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	ctx := governance.WithInvocation(context.Background(), governance.Invocation{
		Execution: governance.ExecutionContext{
			TenantID:      "acme",
			Role:          "member",
			RequestID:     "request-knowledge-test",
			TraceID:       "trace-knowledge-test",
			PolicyVersion: "1",
		},
		Budget: governance.NewCallBudget(4),
	})

	events, err := runnerInstance.Run(ctx, "user-1", "session-1", model.NewUserMessage("请问支持退款吗？"))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	for range events {
	}

	found := false
	for _, text := range chat.Texts() {
		if strings.Contains(text, "支持7天无理由退款") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("governed knowledge search tool did not return document content: %v", chat.Texts())
	}
}
