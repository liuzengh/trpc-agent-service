package governance

import (
	"context"
	"errors"
	"log/slog"
	"testing"
)

func validExecutionContext() ExecutionContext {
	return ExecutionContext{TenantID: "tenant-a", Role: "user", TraceID: "trace-1", PolicyVersion: "3"}
}

func TestStaticToolPolicyAuthorize(t *testing.T) {
	policy := NewStaticToolPolicy([]string{"query_order", "check_shipping"}, []string{"password", "token", "api_key", "authorization"})

	t.Run("allows listed tool with clean JSON", func(t *testing.T) {
		err := policy.Authorize(context.Background(), validExecutionContext(), ToolRequest{Name: "query_order", Arguments: []byte(`{"order_id":"42"}`)})
		if err != nil {
			t.Fatalf("Authorize() error = %v, want nil", err)
		}
	})

	t.Run("denies a role excluded by the tenant tool matrix", func(t *testing.T) {
		identity := validExecutionContext()
		identity.Role = "member"
		identity.ToolRoles = map[string][]string{"query_order": {"admin"}}
		err := policy.Authorize(context.Background(), identity, ToolRequest{Name: "query_order", Arguments: []byte(`{}`)})
		if !errors.Is(err, ErrToolDenied) {
			t.Fatalf("Authorize() error = %v, want ErrToolDenied", err)
		}
	})

	t.Run("denies unknown tool", func(t *testing.T) {
		err := policy.Authorize(context.Background(), validExecutionContext(), ToolRequest{Name: "refund", Arguments: []byte(`{}`)})
		if !errors.Is(err, ErrToolDenied) {
			t.Fatalf("Authorize() error = %v, want ErrToolDenied", err)
		}
	})

	t.Run("denies missing execution identity", func(t *testing.T) {
		identity := validExecutionContext()
		identity.TenantID = ""
		err := policy.Authorize(context.Background(), identity, ToolRequest{Name: "query_order", Arguments: []byte(`{}`)})
		if !errors.Is(err, ErrInvalidExecutionContext) {
			t.Fatalf("Authorize() error = %v, want ErrInvalidExecutionContext", err)
		}
	})

	t.Run("denies invalid JSON arguments", func(t *testing.T) {
		err := policy.Authorize(context.Background(), validExecutionContext(), ToolRequest{Name: "query_order", Arguments: []byte(`{not-json`)})
		if !errors.Is(err, ErrToolDenied) {
			t.Fatalf("Authorize() error = %v, want ErrToolDenied", err)
		}
	})

	t.Run("denies forbidden argument key at top level", func(t *testing.T) {
		err := policy.Authorize(context.Background(), validExecutionContext(), ToolRequest{Name: "query_order", Arguments: []byte(`{"token":"leak"}`)})
		if !errors.Is(err, ErrToolDenied) {
			t.Fatalf("Authorize() error = %v, want ErrToolDenied", err)
		}
	})

	t.Run("denies forbidden argument key nested and case-insensitive", func(t *testing.T) {
		err := policy.Authorize(context.Background(), validExecutionContext(), ToolRequest{Name: "check_shipping", Arguments: []byte(`{"address":{"Api_Key":"leak"}}`)})
		if !errors.Is(err, ErrToolDenied) {
			t.Fatalf("Authorize() error = %v, want ErrToolDenied", err)
		}
	})
}

func TestStaticToolPolicyAllowsTenantSnapshotToolByExactName(t *testing.T) {
	policy := NewStaticToolPolicy([]string{"duckduckgo_search"}, nil)
	execution := validExecutionContext()
	execution.AllowedTools = map[string]struct{}{"crm_find_customer": {}}
	if err := policy.Authorize(context.Background(), execution, ToolRequest{Name: "crm_find_customer", Arguments: []byte(`{}`)}); err != nil {
		t.Fatalf("Authorize(dynamic exact name) error = %v", err)
	}
	if err := policy.Authorize(context.Background(), execution, ToolRequest{Name: "crm_delete_customer", Arguments: []byte(`{}`)}); !errors.Is(err, ErrToolDenied) {
		t.Fatalf("Authorize(unconfigured dynamic name) = %v, want ErrToolDenied", err)
	}
}

func TestCallBudget(t *testing.T) {
	if err := (*CallBudget)(nil).Consume(); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("nil budget error = %v, want ErrBudgetExceeded", err)
	}

	budget := NewCallBudget(2)
	if err := budget.Consume(); err != nil {
		t.Fatalf("first Consume() error = %v, want nil", err)
	}
	if err := budget.Consume(); err != nil {
		t.Fatalf("second Consume() error = %v, want nil", err)
	}
	if err := budget.Consume(); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("exhausted Consume() error = %v, want ErrBudgetExceeded", err)
	}
}

func TestInvocationValidate(t *testing.T) {
	invocation := Invocation{Execution: validExecutionContext(), Budget: NewCallBudget(1)}
	if err := invocation.Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want nil", err)
	}

	missingBudget := invocation
	missingBudget.Budget = nil
	if err := missingBudget.Validate(); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("missing budget error = %v, want ErrBudgetExceeded", err)
	}

	missingRole := invocation
	missingRole.Execution.Role = ""
	if err := missingRole.Validate(); !errors.Is(err, ErrInvalidExecutionContext) {
		t.Fatalf("missing role error = %v, want ErrInvalidExecutionContext", err)
	}
}

func TestInvocationContextRoundTrip(t *testing.T) {
	invocation := Invocation{Execution: validExecutionContext(), Budget: NewCallBudget(1)}
	ctx := WithInvocation(context.Background(), invocation)
	got, ok := InvocationFromContext(ctx)
	if !ok {
		t.Fatal("InvocationFromContext() ok = false, want true")
	}
	if got.Execution.TenantID != "tenant-a" || got.Execution.PolicyVersion != "3" {
		t.Fatalf("InvocationFromContext() = %+v, want tenant-a policy 3", got)
	}
	if _, ok := InvocationFromContext(context.Background()); ok {
		t.Fatal("InvocationFromContext() on empty context ok = true, want false")
	}
}

func TestLogAuditSinkRecordsRedactedEvent(t *testing.T) {
	sink := NewLogAuditSink(slog.New(slog.NewTextHandler(&discardWriter{}, nil)))
	err := sink.RecordToolAudit(context.Background(), ToolAuditEvent{
		TenantID: "tenant-a", TraceID: "trace-1", PolicyVersion: "3",
		ToolName: "query_order", Outcome: ToolOutcomeAllowed, ArgumentsDigest: "digest",
	})
	if err != nil {
		t.Fatalf("RecordToolAudit() error = %v, want nil", err)
	}
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
