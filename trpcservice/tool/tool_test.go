package tool_test

import (
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tool"
)

func TestClassifyFailureFailsClosedForSideEffects(t *testing.T) {
	cause := errors.New("provider connection lost")
	if got := tool.ClassifyFailure(tool.SafetySideEffect, cause); got != tool.FailureSideEffectResultUncertain {
		t.Fatalf("side-effect failure class = %q, want uncertain", got)
	}
	if got := tool.ClassifyFailure(tool.SafetyIdempotent, cause); got != tool.FailurePermanent {
		t.Fatalf("unannotated idempotent failure class = %q, want permanent", got)
	}
}

func TestClassifyFailurePreservesExplicitProviderClass(t *testing.T) {
	cause := errors.New("temporary backend failure")
	annotated := tool.NewInfrastructureRetryableFailure(cause)
	if got := tool.ClassifyFailure(tool.SafetySideEffect, annotated); got != tool.FailureInfrastructureRetryable {
		t.Fatalf("explicit failure class = %q, want infrastructure retryable", got)
	}
	if !errors.Is(annotated, cause) {
		t.Fatalf("classified failure did not preserve cause")
	}
}

func TestSafetyAndFailureValidation(t *testing.T) {
	if err := tool.SafetySideEffect.Validate(); err != nil {
		t.Fatalf("valid safety class rejected: %v", err)
	}
	if err := tool.Safety("unknown").Validate(); err == nil {
		t.Fatal("unknown safety class accepted")
	}
	if err := tool.FailureSideEffectResultKnown.Validate(); err != nil {
		t.Fatalf("valid failure class rejected: %v", err)
	}
	if err := tool.FailureClass("unknown").Validate(); err == nil {
		t.Fatal("unknown failure class accepted")
	}
}

func TestAuthorizeExecutionChecksExecutablePolicy(t *testing.T) {
	policy := tenant.ToolPolicy{
		VisibleTools:    []string{"search", "delete"},
		ExecutableTools: []string{"search"},
	}

	if err := tool.AuthorizeExecution(policy, "search"); err != nil {
		t.Fatalf("authorize executable tool: %v", err)
	}
	if err := tool.AuthorizeExecution(policy, "delete"); !errors.Is(err, tool.ErrToolNotExecutable) {
		t.Fatalf("authorize non-executable tool error = %v, want ErrToolNotExecutable", err)
	}
}

func TestAuthorizeVisibilityChecksVisiblePolicy(t *testing.T) {
	policy := tenant.ToolPolicy{
		VisibleTools:    []string{"search"},
		ExecutableTools: []string{"search"},
	}

	if err := tool.AuthorizeVisibility(policy, "search"); err != nil {
		t.Fatalf("authorize visible tool: %v", err)
	}
	if err := tool.AuthorizeVisibility(policy, "delete"); !errors.Is(err, tool.ErrToolNotVisible) {
		t.Fatalf("authorize invisible tool error = %v, want ErrToolNotVisible", err)
	}
}
