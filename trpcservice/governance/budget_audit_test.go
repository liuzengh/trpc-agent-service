package governance

import (
	"context"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/plugin/guardrail/approval/review"
)

func TestUnitBudgetConsumesAtomicallyAndFailsClosed(t *testing.T) {
	t.Parallel()
	budget := NewUnitBudget(3)
	if err := budget.Consume(2); err != nil {
		t.Fatal(err)
	}
	if err := budget.Consume(1); err != nil {
		t.Fatal(err)
	}
	if err := budget.Consume(1); !errors.Is(err, ErrBudgetUnitsExceeded) {
		t.Fatalf("exhausted Consume() error = %v", err)
	}
	if err := budget.Consume(0); !errors.Is(err, ErrBudgetUnitsExceeded) {
		t.Fatalf("zero Consume() error = %v", err)
	}
	var nilBudget *UnitBudget
	if err := nilBudget.Consume(1); !errors.Is(err, ErrBudgetUnitsExceeded) {
		t.Fatalf("nil Consume() error = %v", err)
	}
}

type auditSinkFunc func(context.Context, ToolAuditEvent) error

func (f auditSinkFunc) RecordToolAudit(ctx context.Context, event ToolAuditEvent) error {
	return f(ctx, event)
}

func TestMultiAuditSinkFansOutAndJoinsFailures(t *testing.T) {
	t.Parallel()
	if _, err := NewMultiAuditSink(auditSinkFunc(func(context.Context, ToolAuditEvent) error { return nil }), nil); err == nil {
		t.Fatal("NewMultiAuditSink() accepted nil sink")
	}
	var calls []string
	errA := errors.New("audit-a failed")
	errB := errors.New("audit-b failed")
	sink, err := NewMultiAuditSink(
		auditSinkFunc(func(context.Context, ToolAuditEvent) error { calls = append(calls, "a"); return errA }),
		auditSinkFunc(func(context.Context, ToolAuditEvent) error { calls = append(calls, "b"); return nil }),
		auditSinkFunc(func(context.Context, ToolAuditEvent) error { calls = append(calls, "c"); return errB }),
	)
	if err != nil {
		t.Fatal(err)
	}
	err = sink.RecordToolAudit(context.Background(), ToolAuditEvent{TenantID: "tenant-a", ToolName: "support.lookup"})
	if !errors.Is(err, errA) || !errors.Is(err, errB) || len(calls) != 3 || calls[0] != "a" || calls[1] != "b" || calls[2] != "c" {
		t.Fatalf("RecordToolAudit() calls=%v error=%v", calls, err)
	}
}

func TestRoleApprovalReviewerRequiresElevatedRole(t *testing.T) {
	t.Parallel()
	reviewer := RoleApprovalReviewer{}
	request := &review.Request{Action: review.Action{ToolName: "support.change"}}
	decision, err := reviewer.Review(context.Background(), request)
	if err != nil || decision.Approved || decision.RiskLevel != "high" {
		t.Fatalf("Review(no invocation) = %#v, %v", decision, err)
	}
	memberCtx := WithInvocation(context.Background(), Invocation{Execution: ExecutionContext{TenantID: "tenant-a", Role: "member", TraceID: "trace-member", PolicyVersion: "1"}, Budget: NewCallBudget(1)})
	decision, err = reviewer.Review(memberCtx, request)
	if err != nil || decision.Approved {
		t.Fatalf("Review(member) = %#v, %v", decision, err)
	}
	adminCtx := WithInvocation(context.Background(), Invocation{Execution: ExecutionContext{TenantID: "tenant-a", Role: "admin", TraceID: "trace-admin", PolicyVersion: "1"}, Budget: NewCallBudget(1)})
	decision, err = reviewer.Review(adminCtx, request)
	if err != nil || !decision.Approved || decision.RiskLevel != "low" {
		t.Fatalf("Review(admin) = %#v, %v", decision, err)
	}
	if !elevatedRole("admin") || elevatedRole("operator") {
		t.Fatal("elevatedRole() policy changed unexpectedly")
	}
}
