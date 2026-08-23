package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/DocJlm/trpc-agent-service/trpcservice/secrets"
	"github.com/DocJlm/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type staticSecrets struct{ values []string }

func (s staticSecrets) Resolve(context.Context, string) ([]string, error) {
	if len(s.values) == 0 {
		return nil, errors.New("missing")
	}
	return s.values, nil
}

var _ secrets.Provider = staticSecrets{}

type fakeModel struct{ calls int }

func (m *fakeModel) Info() model.Info { return model.Info{Name: "fake-model"} }

func (m *fakeModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	m.calls++
	responses := make(chan *model.Response, 1)
	content := "first answer"
	userMessages := 0
	for _, message := range request.Messages {
		if message.Role == model.RoleUser {
			userMessages++
		}
	}
	if userMessages > 1 {
		content = "answer with history"
	}
	responses <- &model.Response{
		Done: true, Model: "fake-model", Usage: &model.Usage{PromptTokens: 3, CompletionTokens: 2},
		Choices: []model.Choice{{Message: model.Message{Role: model.RoleAssistant, Content: content}}},
	}
	close(responses)
	return responses, nil
}

func TestEchoEngine(t *testing.T) {
	result, err := (EchoEngine{}).Run(context.Background(), Request{Content: "hello"})
	if err != nil || result.Content != "echo: hello" || result.Model != "fake" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestToolAllowlist(t *testing.T) {
	engine := NewTRPCEngine(nil)
	allowed := tenant.Tenant{Agent: tenant.AgentProfile{ToolAllowlist: []string{"get_server_time"}}}
	if got := engine.allowedTools(allowed); len(got) != 1 {
		t.Fatalf("allowed tools=%d", len(got))
	}
	denied := tenant.Tenant{Agent: tenant.AgentProfile{ToolAllowlist: []string{"another_tool"}}}
	if got := engine.allowedTools(denied); len(got) != 0 {
		t.Fatalf("denied tools=%d", len(got))
	}
}

func TestTRPCEngineRunAndSessionHistory(t *testing.T) {
	fake := &fakeModel{}
	engine := NewTRPCEngine(
		staticSecrets{values: []string{"test-key"}},
		withModelFactory(func(tenant.ModelProfile, string) model.Model { return fake }),
	)
	defer engine.Close()
	profile := tenant.Tenant{
		ID: "tenant", Enabled: true,
		Agent:   tenant.AgentProfile{ID: "assistant", Version: "1", Instruction: "answer"},
		Model:   tenant.ModelProfile{Provider: "deepseek", Model: "fake-model", APIKeyRef: "test", Timeout: "2s"},
		Backend: tenant.BackendProfile{Session: "memory"},
	}
	first, err := engine.Run(context.Background(), Request{
		Tenant: profile, UserID: "user", SessionID: "session", Content: "hello", TraceID: "trace-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Content != "first answer" || first.PromptTokens != 3 || first.CompletionTokens != 2 {
		t.Fatalf("first=%+v", first)
	}
	second, err := engine.Run(context.Background(), Request{
		Tenant: profile, UserID: "user", SessionID: "session", Content: "again", TraceID: "trace-2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Content != "answer with history" || fake.calls != 2 {
		t.Fatalf("second=%+v calls=%d", second, fake.calls)
	}
}

func TestTRPCEngineConfigurationErrors(t *testing.T) {
	engine := NewTRPCEngine(staticSecrets{})
	_, err := engine.Run(context.Background(), Request{
		Tenant: tenant.Tenant{
			ID: "tenant", Agent: tenant.AgentProfile{ID: "assistant", Version: "1"},
			Model: tenant.ModelProfile{APIKeyRef: "missing"}, Backend: tenant.BackendProfile{Session: "memory"},
		}, UserID: "u", SessionID: "s", Content: "hello",
	})
	if err == nil {
		t.Fatal("missing secret should fail")
	}
	if _, err := (EchoEngine{}).Run(context.Background(), Request{}); err == nil {
		t.Fatal("empty echo input should fail")
	}
}
