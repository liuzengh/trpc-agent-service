package tool

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	agenttool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func contextInvocation(budget int) governance.Invocation {
	return governance.Invocation{
		Execution: governance.ExecutionContext{
			TenantID: "tenant-a", Role: "user", TraceID: "trace-1", RequestID: "request-1", PolicyVersion: "2",
		},
		Budget: governance.NewCallBudget(budget),
	}
}

func newCallbacks(t *testing.T, allowed, forbidden []string) (*agenttool.Callbacks, *recordingAuditSink) {
	t.Helper()
	audit := &recordingAuditSink{}
	callbacks, err := NewGovernanceCallbacks(
		governance.NewStaticToolPolicy(allowed, forbidden),
		audit,
		NewMemoryExecutionLedger(),
		time.Second,
	)
	if err != nil {
		t.Fatalf("NewGovernanceCallbacks() error = %v", err)
	}
	return callbacks, audit
}

func TestGovernanceCallbacksDoNotRepeatGuardrailApproval(t *testing.T) {
	t.Parallel()
	callbacks, audit := newCallbacks(t, []string{"count"}, nil)
	invocation := contextInvocation(2)
	invocation.Execution.Role = "member"
	ctx := governance.WithInvocation(context.Background(), invocation)
	if _, err := callbacks.BeforeTool[0](ctx, &agenttool.BeforeToolArgs{ToolName: "count", Arguments: []byte(`{}`)}); err != nil {
		t.Fatalf("BeforeTool() error = %v, want approval policy to be owned by Guardrail", err)
	}
	if len(audit.events) != 0 {
		t.Fatalf("audit events = %+v, want no duplicate approval decision", audit.events)
	}
}

func TestGovernanceCallbacksDenyWithoutInvocation(t *testing.T) {
	t.Parallel()

	callbacks, audit := newCallbacks(t, []string{"count"}, nil)
	_, err := callbacks.BeforeTool[0](context.Background(), &agenttool.BeforeToolArgs{ToolName: "count", Arguments: []byte(`{}`)})
	if !errors.Is(err, governance.ErrInvalidExecutionContext) {
		t.Fatalf("BeforeTool() error = %v, want ErrInvalidExecutionContext", err)
	}
	if len(audit.events) != 0 {
		t.Fatalf("audit events = %d, want 0 for missing identity", len(audit.events))
	}
}

func TestGovernanceCallbacksDenyUnlistedAndForbiddenTools(t *testing.T) {
	t.Parallel()

	unlisted, audit := newCallbacks(t, []string{"other"}, nil)
	ctx := governance.WithInvocation(context.Background(), contextInvocation(2))
	if _, err := unlisted.BeforeTool[0](ctx, &agenttool.BeforeToolArgs{ToolName: "count", Arguments: []byte(`{}`)}); !errors.Is(err, governance.ErrToolDenied) {
		t.Fatalf("BeforeTool() error = %v, want ErrToolDenied", err)
	}
	if len(audit.events) != 1 || audit.events[0].Outcome != governance.ToolOutcomeDenied {
		t.Fatalf("audit events = %v, want one denied", audit.events)
	}

	forbidden, forbiddenAudit := newCallbacks(t, []string{"count"}, []string{"token"})
	if _, err := forbidden.BeforeTool[0](ctx, &agenttool.BeforeToolArgs{ToolName: "count", Arguments: []byte(`{"nested":{"token":"leak"}}`)}); !errors.Is(err, governance.ErrToolDenied) {
		t.Fatalf("BeforeTool() error = %v, want ErrToolDenied for nested forbidden key", err)
	}
	if len(forbiddenAudit.events) != 1 || forbiddenAudit.events[0].ArgumentsDigest == "" {
		t.Fatalf("audit events = %v, want one denied with digest", forbiddenAudit.events)
	}
}

// TestGovernanceCallbacksEnforcePerInvocationBudgetAndTimeout verifies the
// request-scoped budget blocks the second call, and the returned context both
// carries a deadline for per-call timeout and preserves the invocation.
func TestGovernanceCallbacksEnforcePerInvocationBudgetAndTimeout(t *testing.T) {
	t.Parallel()

	callbacks, audit := newCallbacks(t, []string{"count"}, nil)
	ctx := governance.WithInvocation(context.Background(), contextInvocation(1))

	result, err := callbacks.BeforeTool[0](ctx, &agenttool.BeforeToolArgs{ToolName: "count", Arguments: []byte(`{}`)})
	if err != nil {
		t.Fatalf("first BeforeTool() error = %v, want nil", err)
	}
	if result == nil || result.Context == nil {
		t.Fatal("BeforeTool() result context = nil, want timeout context")
	}
	if _, ok := result.Context.Deadline(); !ok {
		t.Fatal("BeforeTool() context has no deadline, want per-call timeout")
	}
	if _, ok := governance.InvocationFromContext(result.Context); !ok {
		t.Fatal("BeforeTool() context lost the governance invocation")
	}

	if _, err := callbacks.BeforeTool[0](ctx, &agenttool.BeforeToolArgs{ToolName: "count", Arguments: []byte(`{"attempt":2}`)}); !errors.Is(err, governance.ErrBudgetExceeded) {
		t.Fatalf("second BeforeTool() error = %v, want ErrBudgetExceeded", err)
	}
	if len(audit.events) != 1 || audit.events[0].Outcome != governance.ToolOutcomeDenied {
		t.Fatalf("audit events = %v, want one denied for budget", audit.events)
	}
}

// TestGovernanceCallbacksAfterToolRecordsOutcome verifies allowed and failed
// executions both produce digest-only audit evidence.
func TestGovernanceCallbacksAfterToolRecordsOutcome(t *testing.T) {
	t.Parallel()

	callbacks, audit := newCallbacks(t, []string{"count"}, nil)
	ctx := governance.WithInvocation(context.Background(), contextInvocation(2))

	result, err := callbacks.BeforeTool[0](ctx, &agenttool.BeforeToolArgs{ToolName: "count", Arguments: []byte(`{"n":1}`)})
	if err != nil {
		t.Fatalf("BeforeTool() error = %v, want nil", err)
	}
	if _, err := callbacks.AfterTool[0](result.Context, &agenttool.AfterToolArgs{ToolName: "count", Arguments: []byte(`{"n":1}`)}); err != nil {
		t.Fatalf("AfterTool() error = %v, want nil", err)
	}
	if len(audit.events) != 1 || audit.events[0].Outcome != governance.ToolOutcomeAllowed {
		t.Fatalf("audit events = %v, want one allowed", audit.events)
	}

	result, err = callbacks.BeforeTool[0](ctx, &agenttool.BeforeToolArgs{ToolName: "count", Arguments: []byte(`{"n":2}`)})
	if err != nil {
		t.Fatalf("second BeforeTool() error = %v, want nil", err)
	}
	if _, err := callbacks.AfterTool[0](result.Context, &agenttool.AfterToolArgs{ToolName: "count", Arguments: []byte(`{"n":2}`), Error: errors.New("boom")}); err != nil {
		t.Fatalf("AfterTool(failed) error = %v, want nil", err)
	}
	if len(audit.events) != 2 || audit.events[1].Outcome != governance.ToolOutcomeFailed {
		t.Fatalf("audit events = %v, want allowed then failed", audit.events)
	}
}
