package agent

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	frameworkagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestAgentLevelGovernanceAllowsRegisteredTool(t *testing.T) {
	callbacks, err := platformtool.NewGovernanceCallbacks(
		governance.NewStaticToolPolicy([]string{"query_order"}, nil),
		&countingAuditSink{},
		platformtool.NewMemoryExecutionLedger(),
		time.Second,
	)
	if err != nil {
		t.Fatalf("NewGovernanceCallbacks() error = %v", err)
	}
	delegate := &countingCallableTool{}
	llm := llmagent.New(
		"assistant",
		llmagent.WithModel(&toolRequestModel{name: "query_order", arguments: "{}"}),
		llmagent.WithTools([]agenttool.Tool{delegate}),
		llmagent.WithToolCallbacks(callbacks),
	)
	runnerInstance := runner.NewRunner("tenant-a/support", llm)
	ctx := governance.WithInvocation(context.Background(), governance.Invocation{
		Execution: governance.ExecutionContext{TenantID: "tenant-a", Role: "user", TraceID: "trace-allowed", RequestID: "request-allowed", PolicyVersion: "1"},
		Budget:    governance.NewCallBudget(1),
	})
	events, err := runnerInstance.Run(ctx, "user-1", "session-1", model.NewUserMessage("query my order"), frameworkagent.WithExecutionTraceEnabled(true))
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	traceSteps := 0
	for agentEvent := range events {
		if !agentEvent.IsRunnerCompletion() || agentEvent.ExecutionTrace == nil {
			continue
		}
		traceSteps = len(agentEvent.ExecutionTrace.Steps)
	}
	if delegate.calls != 1 {
		t.Fatalf("delegate tool calls = %d, want 1", delegate.calls)
	}
	if traceSteps == 0 {
		t.Fatal("framework execution trace was empty for a run that executed a tool")
	}
}
