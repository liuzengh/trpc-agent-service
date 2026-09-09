package policy

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	trpctool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestIMIdentityAccessIsTenantScopedAndRedactedFromToolContext(t *testing.T) {
	engine := &Engine{Identity: AuthenticatedIdentityAuthorizer{}}
	base := Request{AppID: "app", UserID: "wecom/shared/alice", ExternalUserID: "alice", RequestID: "request", Policy: tenant.ToolPolicy{Allow: []string{"safe"}}}
	allowed := base
	allowed.TenantID = "tenant-a"
	allowed.AllowedUsers = []string{"alice"}
	if _, err := engine.Evaluate(context.Background(), allowed); err != nil {
		t.Fatalf("tenant-a identity rejected: %v", err)
	}
	denied := base
	denied.TenantID = "tenant-b"
	denied.AllowedUsers = []string{"bob"}
	if _, err := engine.Evaluate(context.Background(), denied); !errors.Is(err, ErrIdentityDenied) {
		t.Fatalf("tenant-b identity error=%v", err)
	}
	ctx := WithRequest(context.Background(), engine, allowed)
	stored, ok := FromContext(ctx)
	if !ok {
		t.Fatal("policy context missing")
	}
	if stored.Request.ExternalUserID != "" || stored.Request.ConversationID != "" || stored.Request.AllowedUsers != nil || stored.Request.AllowedChats != nil {
		t.Fatalf("tool context retained IM identity policy: %+v", stored.Request)
	}
}

func TestVisibilityExecutionPermissionAndApproval(t *testing.T) {
	approvals := NewMemoryApprovals()
	engine := &Engine{Identity: AuthenticatedIdentityAuthorizer{}, Approvals: approvals, Budgets: NewMemoryBudget()}
	request := Request{TenantID: "t", AppID: "a", UserID: "u", RequestID: "r", Policy: tenant.ToolPolicy{Allow: []string{"safe", "danger"}, Deny: []string{"blocked"}, RequireApproval: []string{"danger"}, RequestTokenBudget: 10}, EstimatedTokens: 5}
	controls, err := engine.Evaluate(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	safe, danger, blocked := namedTool("safe"), namedTool("danger"), namedTool("blocked")
	if !controls.Visibility(context.Background(), safe) || controls.Visibility(context.Background(), blocked) {
		t.Fatal("visibility policy mismatch")
	}
	if !controls.Execution(context.Background(), safe) || controls.Execution(context.Background(), danger) || controls.Execution(context.Background(), blocked) {
		t.Fatal("execution allowlist mismatch")
	}
	decision, _ := controls.Permission.CheckToolPermission(context.Background(), &trpctool.PermissionRequest{ToolName: "danger"})
	if decision.Action != trpctool.PermissionActionAsk {
		t.Fatalf("decision=%+v", decision)
	}
	approvals.Grant("t", "r", "danger")
	decision, _ = controls.Permission.CheckToolPermission(context.Background(), &trpctool.PermissionRequest{ToolName: "danger"})
	if decision.Action != trpctool.PermissionActionAllow {
		t.Fatalf("approved decision=%+v", decision)
	}
	if err := engine.AuthorizeDirect(context.Background(), request, "danger"); err != nil {
		t.Fatal(err)
	}
}

func TestDangerousToolWaitsForExplicitApproval(t *testing.T) {
	approvals := NewMemoryApprovals()
	engine := &Engine{Identity: AuthenticatedIdentityAuthorizer{}, Approvals: approvals}
	request := Request{TenantID: "t", AppID: "a", UserID: "u", RequestID: "r", Policy: tenant.ToolPolicy{Allow: []string{"danger"}, RequireApproval: []string{"danger"}}}
	done := make(chan error, 1)
	go func() { done <- engine.WaitApproval(context.Background(), request, "danger") }()
	select {
	case err := <-done:
		t.Fatalf("approval did not block: %v", err)
	default:
	}
	approvals.Grant("t", "r", "danger")
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestBudgetRejectsAndIsIdempotent(t *testing.T) {
	budget := NewMemoryBudget()
	request := Request{TenantID: "t", RequestID: "r", Policy: tenant.ToolPolicy{RequestTokenBudget: 2}, EstimatedTokens: 3}
	if !errors.Is(budget.Reserve(context.Background(), request), ErrBudgetExceeded) {
		t.Fatal("token budget not enforced")
	}
	request.EstimatedTokens = 1
	request.Policy.MonthlyCostBudgetCents = 1
	request.EstimatedCostMicros = 9000
	if err := budget.Reserve(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := budget.Reserve(context.Background(), request); err != nil {
		t.Fatal("idempotent reserve failed")
	}
	request.RequestID = "r2"
	request.EstimatedCostMicros = 2000
	if !errors.Is(budget.Reserve(context.Background(), request), ErrBudgetExceeded) {
		t.Fatal("monthly budget not enforced")
	}
}

func TestBudgetReconcileRejectsOutputOverrun(t *testing.T) {
	budget := NewMemoryBudget()
	request := Request{TenantID: "t", RequestID: "r", Policy: tenant.ToolPolicy{RequestTokenBudget: 10}, EstimatedTokens: 2}
	if err := budget.Reserve(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(budget.Reconcile(context.Background(), request, 11, 0), ErrBudgetExceeded) {
		t.Fatal("actual output overrun was accepted")
	}
}

func TestEstimateModelCostSeparatesPromptAndCompletion(t *testing.T) {
	pricing := tenant.ModelPricing{Version: "price-v1", InputMicrosPerMillion: 1_000_000, OutputMicrosPerMillion: 2_000_000}
	if got := EstimateModelCost(pricing, 3, 4); got != 11 {
		t.Fatalf("cost=%d, want 11", got)
	}
	if got := EstimateModelCost(tenant.ModelPricing{InputMicrosPerMillion: 1}, 1, 0); got != 1 {
		t.Fatalf("small call must round up to one micro, got %d", got)
	}
}
