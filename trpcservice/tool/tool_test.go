package tool

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/policy"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"
)

func TestGuardedToolFinalAuthorization(t *testing.T) {
	called := false
	delegate := function.NewFunctionTool(func(_ context.Context, input struct{}) (string, error) { called = true; return "ok", nil }, function.WithName("danger"))
	guard := Guarded{Delegate: delegate}
	request := policy.Request{TenantID: "t", AppID: "a", UserID: "u", RequestID: "r", Policy: tenant.ToolPolicy{Allow: []string{"danger"}, RequireApproval: []string{"danger"}}}
	engine := &policy.Engine{Identity: policy.AuthenticatedIdentityAuthorizer{}, Approvals: policy.NewMemoryApprovals()}
	ctx := policy.WithRequest(context.Background(), engine, request)
	if _, err := guard.Call(ctx, []byte(`{}`)); !errors.Is(err, policy.ErrApprovalRequired) {
		t.Fatalf("error=%v", err)
	}
	if called {
		t.Fatal("unapproved handler executed")
	}
	engine.Approvals.(*policy.MemoryApprovals).Grant("t", "r", "danger")
	if _, err := guard.Call(ctx, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("approved handler did not execute")
	}
}
