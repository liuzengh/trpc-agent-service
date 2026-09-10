package assembly

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/liuzengh/trpc-agent-service/internal/testutil"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactinmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

type saveArtifactModel struct {
	reply string
	calls int
}

func (m *saveArtifactModel) GenerateContent(ctx context.Context, request *model.Request) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.calls++
	if m.calls == 1 && request != nil && request.Tools[tool.SaveArtifactToolName] != nil {
		args, _ := json.Marshal(map[string]string{"filename": "note.txt", "text": "saved-by-agent"})
		return singleAssistantResponse(model.Message{
			Role: model.RoleAssistant,
			ToolCalls: []model.ToolCall{{
				Type:     "function",
				Function: model.FunctionDefinitionParam{Name: tool.SaveArtifactToolName, Arguments: args},
			}},
		}), nil
	}
	return singleAssistantResponse(model.NewAssistantMessage(m.reply)), nil
}

func (m *saveArtifactModel) Info() model.Info { return model.Info{Name: "save-artifact-model"} }

func TestFactorySavesFrameworkArtifactThroughPlatformTool(t *testing.T) {
	artifacts := artifactinmemory.NewService()
	chat := &saveArtifactModel{reply: "已保存"}
	factory := NewFactoryWithModelProvider(
		&recordingModelProvider{model: chat},
		nil, nil, nil, nil,
		&recordingArtifactProvider{service: artifacts},
		nil,
	)
	t.Cleanup(func() { _ = factory.Close() })
	runnerInstance, err := factory.Get(context.Background(), config.TenantConfig{
		TenantID: "acme", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Model: config.ModelConfig{ProviderID: "primary", Name: "support"},
	})
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	events, err := runnerInstance.Run(context.Background(), "user-1", "session-1", model.NewUserMessage("保存一份笔记"))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for range events {
	}
	loaded, err := artifacts.LoadArtifact(context.Background(), agentartifact.SessionInfo{
		AppName: "acme/support", UserID: "user-1", SessionID: "session-1",
	}, "note.txt", nil)
	if err != nil || loaded == nil || string(loaded.Data) != "saved-by-agent" {
		t.Fatalf("LoadArtifact() = %#v, %v", loaded, err)
	}
}

func TestFactoryExposesSaveArtifactToolWhenArtifactServiceConfigured(t *testing.T) {
	captured := &capturingModel{inner: testutil.NewFakeModel("ok")}
	factory := NewFactoryWithModelProvider(
		&recordingModelProvider{model: captured},
		nil, nil, nil, nil,
		&recordingArtifactProvider{service: artifactinmemory.NewService()},
		nil,
	)
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
	captured.mu.Lock()
	defer captured.mu.Unlock()
	if len(captured.tools) == 0 || !containsString(captured.tools[0], tool.SaveArtifactToolName) {
		t.Fatalf("model request tools = %v, want %s", captured.tools, tool.SaveArtifactToolName)
	}
}
