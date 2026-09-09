package governance

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/plugin"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmem "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	"trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestPluginBlocksDeniedToolAtRunnerHook(t *testing.T) {
	filter := NewGovernanceFilter(&tenant.Tenant{ToolPolicy: tenant.ToolPolicy{
		Mode: "whitelist", Allowed: []string{"safe"},
	}})
	plugin := NewPlugin(filter, "test")
	_, err := plugin.beforeTool(context.Background(), &tool.BeforeToolArgs{
		ToolName: "dangerous", Arguments: []byte(`{"target":"prod"}`),
	})
	if err == nil {
		t.Fatal("runner governance hook allowed denied tool")
	}
}

func TestPluginAuditsToolAuthorizationAndOutcome(t *testing.T) {
	var audit bytes.Buffer
	filter := NewGovernanceFilter(&tenant.Tenant{ID: "tenant-a", ToolPolicy: tenant.ToolPolicy{
		Mode: "whitelist", Allowed: []string{"safe"},
	}})
	plugin := NewPlugin(filter, "test", telemetry.NewCollectorWithAuditSink(&audit))
	ctx := ContextWithInvocationAudit(context.Background(), InvocationAuditContext{
		ChannelType: "telegram",
		UserID:      "user-1",
		SessionID:   "session-1",
		AgentName:   "support",
		TraceID:     "0123456789abcdef0123456789abcdef",
	})
	before, err := plugin.beforeTool(ctx, &tool.BeforeToolArgs{
		ToolCallID: "call-1", ToolName: "safe", Arguments: []byte(`{"q":"x"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := plugin.afterTool(before.Context, &tool.AfterToolArgs{
		ToolCallID: "call-1", ToolName: "safe", Result: map[string]interface{}{"ok": true},
	}); err != nil {
		t.Fatal(err)
	}
	output := audit.String()
	for _, expected := range []string{`"tool_name":"safe"`, `"decision":"tool_allowed"`, `"decision":"tool_succeeded"`} {
		if !strings.Contains(output, expected) {
			t.Fatalf("audit output %q does not contain %s", output, expected)
		}
	}
	for _, expected := range []string{`"channel":"telegram"`, `"session_id":"session-1"`, `"agent_name":"support"`, `"trace_id":"0123456789abcdef0123456789abcdef"`} {
		if !strings.Contains(output, expected) {
			t.Fatalf("audit output %q does not contain %s", output, expected)
		}
	}
}

func TestPluginRejectsMalformedArguments(t *testing.T) {
	filter := NewGovernanceFilter(&tenant.Tenant{ToolPolicy: tenant.ToolPolicy{Mode: "blacklist"}})
	plugin := NewPlugin(filter, "test")
	if _, err := plugin.beforeTool(context.Background(), &tool.BeforeToolArgs{
		ToolName: "safe", Arguments: []byte(`{"broken"`),
	}); err == nil {
		t.Fatal("malformed tool arguments were accepted")
	}
}

func TestPluginRejectsMissingTenantWithoutAuditPanic(t *testing.T) {
	var audit bytes.Buffer
	plugin := NewPlugin(NewGovernanceFilter(nil), "test", telemetry.NewCollectorWithAuditSink(&audit))
	if _, err := plugin.beforeTool(nil, &tool.BeforeToolArgs{ToolName: "safe"}); err == nil {
		t.Fatal("missing tenant policy was accepted")
	}
}

func TestInvocationAuditContextNormalizesNilContext(t *testing.T) {
	ctx := ContextWithInvocationAudit(nil, InvocationAuditContext{UserID: "user-1"})
	if got := invocationAuditFromContext(ctx); got.UserID != "user-1" {
		t.Fatalf("audit context=%+v, want user identity", got)
	}
	if got := invocationAuditFromContext(nil); got != (InvocationAuditContext{}) {
		t.Fatalf("nil context audit=%+v, want zero value", got)
	}
	if got := toolLatency(nil, nil); got != 0 {
		t.Fatalf("nil context latency=%s, want zero", got)
	}
}

func TestPluginMasksCompleteModelResponseBeforeEventPersistence(t *testing.T) {
	filter := NewGovernanceFilter(&tenant.Tenant{Governance: tenant.GovernancePolicy{
		DataMasking: []tenant.MaskingRule{{Type: "email", Replace: ""}},
	}})
	pluginInstance := NewPlugin(filter, "model-redaction")
	manager, err := plugin.NewManager(pluginInstance)
	if err != nil {
		t.Fatal(err)
	}

	signature := "provider-signature"
	response := &model.Response{
		Done: true,
		Choices: []model.Choice{{
			Message: model.Message{
				Role:               model.RoleAssistant,
				Content:            "contact user@example.com",
				ReasoningSignature: signature,
				ToolCalls: []model.ToolCall{{
					ID: "call-1",
				}},
			},
		}},
	}
	if _, err := manager.ModelCallbacks().RunAfterModel(context.Background(), &model.AfterModelArgs{Response: response}); err != nil {
		t.Fatal(err)
	}
	if response.Choices[0].Message.Content != "contact u***@example.com" {
		t.Fatalf("model response was not masked: %q", response.Choices[0].Message.Content)
	}
	if response.Choices[0].Message.ReasoningSignature != signature {
		t.Fatalf("provider signature changed: %q", response.Choices[0].Message.ReasoningSignature)
	}
	if len(response.Choices[0].Message.ToolCalls) != 1 || response.Choices[0].Message.ToolCalls[0].ID != "call-1" {
		t.Fatalf("tool call protocol fields changed: %+v", response.Choices[0].Message.ToolCalls)
	}
}

func TestPluginLeavesPartialModelChunksUntouched(t *testing.T) {
	filter := NewGovernanceFilter(&tenant.Tenant{Governance: tenant.GovernancePolicy{
		DataMasking: []tenant.MaskingRule{{Type: "email", Replace: ""}},
	}})
	pluginInstance := NewPlugin(filter, "model-redaction")
	manager, err := plugin.NewManager(pluginInstance)
	if err != nil {
		t.Fatal(err)
	}
	response := &model.Response{
		Done:      false,
		IsPartial: true,
		Choices:   []model.Choice{{Message: model.Message{Content: "user@example.com"}}},
	}
	if _, err := manager.ModelCallbacks().RunAfterModel(context.Background(), &model.AfterModelArgs{Response: response}); err != nil {
		t.Fatal(err)
	}
	if response.Choices[0].Message.Content != "user@example.com" {
		t.Fatalf("partial model chunk was modified: %q", response.Choices[0].Message.Content)
	}
}

type fixedModelForGovernanceTest struct{}

func (fixedModelForGovernanceTest) GenerateContent(context.Context, *model.Request) (<-chan *model.Response, error) {
	responses := make(chan *model.Response, 1)
	responses <- &model.Response{
		Done: true,
		Choices: []model.Choice{{
			Message: model.Message{Role: model.RoleAssistant, Content: "reply user@example.com"},
		}},
	}
	close(responses)
	return responses, nil
}

func (fixedModelForGovernanceTest) Info() model.Info {
	return model.Info{Name: "fixed-governance-model"}
}

func TestPluginMasksTextBeforeRunnerPersistsSessionEvent(t *testing.T) {
	filter := NewGovernanceFilter(&tenant.Tenant{Governance: tenant.GovernancePolicy{
		DataMasking: []tenant.MaskingRule{{Type: "email"}},
	}})
	sessionService := sessioninmem.NewSessionService()
	agent := llmagent.New("governance-agent", llmagent.WithModel(fixedModelForGovernanceTest{}))
	r := runner.NewRunner("governance-app", agent,
		runner.WithSessionService(sessionService),
		runner.WithPlugins(NewPlugin(filter, "model-redaction")),
	)
	events, err := r.Run(context.Background(), "user-1", "session-1", model.NewUserMessage("hello"))
	if err != nil {
		t.Fatal(err)
	}
	for range events {
	}
	sess, err := sessionService.GetSession(context.Background(), session.Key{
		AppName: "governance-app", UserID: "user-1", SessionID: "session-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range sess.Events {
		if event.Response == nil || len(event.Response.Choices) == 0 {
			continue
		}
		content := event.Response.Choices[0].Message.Content
		if strings.Contains(content, "user@example.com") {
			t.Fatalf("raw model text was persisted in session event: %q", content)
		}
	}
}
