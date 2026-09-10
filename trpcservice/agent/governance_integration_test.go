package agent

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type toolRequestModel struct {
	name      string
	arguments string
	calls     int
}

func (m *toolRequestModel) GenerateContent(ctx context.Context, _ *model.Request) (<-chan *model.Response, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.calls++
	response := &model.Response{ID: "tool-request-response", Object: "chat.completion"}
	if m.calls == 1 {
		// First turn: ask for the tool that the policy will deny.
		response.Choices = []model.Choice{{
			Index: 0,
			Message: model.Message{Role: model.RoleAssistant, ToolCalls: []model.ToolCall{{
				Type:     "function",
				Function: model.FunctionDefinitionParam{Name: m.name, Arguments: []byte(m.arguments)},
			}}},
		}}
	} else {
		// Later turns: finish with a plain reply so the run terminates.
		response.Choices = []model.Choice{{
			Index:   0,
			Message: model.NewAssistantMessage("done"),
		}}
	}
	response.Done = true
	responses := make(chan *model.Response, 1)
	responses <- response
	close(responses)
	return responses, nil
}

func (m *toolRequestModel) Info() model.Info { return model.Info{Name: "tool-request-model"} }

type countingCallableTool struct{ calls int }

func (c *countingCallableTool) Declaration() *agenttool.Declaration {
	return &agenttool.Declaration{Name: "query_order", Description: "queries an order"}
}

func (c *countingCallableTool) Call(context.Context, []byte) (any, error) {
	c.calls++
	return "order found", nil
}

// TestAgentLevelGovernanceCallbacksBlockToolExecution pins the fail-closed
// guarantee at the agent level: a model requesting a tool that the policy
// denies must never reach the tool implementation, even though the tool is
// registered with the agent.
func TestAgentLevelGovernanceCallbacksBlockToolExecution(t *testing.T) {
	t.Parallel()

	callbacks, err := tool.NewGovernanceCallbacks(
		governance.NewStaticToolPolicy(nil, nil), // deny all
		&countingAuditSink{},
		tool.NewMemoryExecutionLedger(),
		time.Second,
	)
	if err != nil {
		t.Fatalf("NewGovernanceCallbacks() error = %v", err)
	}
	delegate := &countingCallableTool{}
	llm := llmagent.New(
		"assistant",
		llmagent.WithModel(&toolRequestModel{name: "query_order", arguments: `{}`}),
		llmagent.WithTools([]agenttool.Tool{delegate}),
		llmagent.WithToolCallbacks(callbacks),
	)
	runnerInstance := runner.NewRunner("tenant-a/support", llm)

	ctx := governance.WithInvocation(context.Background(), governance.Invocation{
		Execution: governance.ExecutionContext{
			TenantID: "tenant-a", Role: "user", TraceID: "trace-1", PolicyVersion: "1",
		},
		Budget: governance.NewCallBudget(2),
	})
	events, err := runnerInstance.Run(ctx, "user-1", "session-1", model.NewUserMessage("query my order"))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for range events {
		// Drain the stream so the run completes before assertions.
	}
	if delegate.calls != 0 {
		t.Fatalf("delegate tool calls = %d, want 0 under deny-all policy", delegate.calls)
	}
}

type countingAuditSink struct{ events int }

func (s *countingAuditSink) RecordToolAudit(_ context.Context, _ governance.ToolAuditEvent) error {
	s.events++
	return nil
}
