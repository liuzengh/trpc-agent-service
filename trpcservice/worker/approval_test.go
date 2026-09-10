package worker

import (
	"context"
	"testing"
	"time"

	platformapproval "github.com/liuzengh/trpc-agent-service/trpcservice/approval"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

func TestReviewToolApprovalTransitionsAskAllowAndDeny(t *testing.T) {
	approvals := &testApprovalRepository{}
	w := Worker{Approvals: approvals}
	exec := securityTestExecution()
	exec.Config.Tools = tenant.ToolPolicy{
		ExecutableTools:     []string{"delete"},
		ReviewRequiredTools: []string{"delete"},
	}
	request := &frameworktool.PermissionRequest{
		ToolName:   "delete",
		ToolCallID: "call-1",
		Arguments:  []byte(`{"resource":"record-1"}`),
	}
	policy := w.toolPermissionPolicy(exec)

	decision, err := policy.CheckToolPermission(context.Background(), request)
	if err != nil || decision.Action != frameworktool.PermissionActionAsk {
		t.Fatalf("pending decision=%q err=%v, want ASK", decision.Action, err)
	}
	if approvals.request.ArgumentDigest != platformapproval.DigestArguments(request.Arguments) {
		t.Fatalf("argument digest=%q", approvals.request.ArgumentDigest)
	}

	approvals.status = platformapproval.StatusApproved
	decision, err = policy.CheckToolPermission(context.Background(), request)
	if err != nil || decision.Action != frameworktool.PermissionActionAllow {
		t.Fatalf("approved decision=%q err=%v, want ALLOW", decision.Action, err)
	}

	approvals.status = platformapproval.StatusDenied
	decision, err = policy.CheckToolPermission(context.Background(), request)
	if err != nil || decision.Action != frameworktool.PermissionActionDeny {
		t.Fatalf("denied decision=%q err=%v, want DENY", decision.Action, err)
	}
}

type testApprovalRepository struct {
	request platformapproval.Request
	status  platformapproval.Status
}

func (r *testApprovalRepository) ResolveOrCreate(_ context.Context, request platformapproval.Request) (platformapproval.Record, error) {
	r.request = request
	status := r.status
	if status == "" {
		status = platformapproval.StatusPending
	}
	decidedAt := (*time.Time)(nil)
	if status != platformapproval.StatusPending {
		now := time.Now()
		decidedAt = &now
	}
	return platformapproval.Record{
		ApprovalID:     "approval-1",
		TenantID:       request.TenantID,
		AppID:          request.AppID,
		ConfigVersion:  request.ConfigVersion,
		RequestID:      request.RequestID,
		SessionID:      request.SessionID,
		ToolName:       request.ToolName,
		ToolCallID:     request.ToolCallID,
		ArgumentDigest: request.ArgumentDigest,
		Status:         status,
		ExpiresAt:      request.ExpiresAt,
		CreatedAt:      time.Now(),
		DecidedAt:      decidedAt,
	}, nil
}

func (r *testApprovalRepository) List(context.Context, platformapproval.Query) ([]platformapproval.Record, error) {
	return nil, nil
}

func (r *testApprovalRepository) Decide(context.Context, string, string, string, platformapproval.Status) (platformapproval.Record, error) {
	return platformapproval.Record{}, nil
}
