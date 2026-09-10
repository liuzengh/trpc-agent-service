package governance

import (
	"context"
	"errors"
	"strings"

	"trpc.group/trpc-go/trpc-agent-go/plugin/guardrail/approval/review"
)

// RoleApprovalReviewer is the framework Guardrail reviewer for tools that
// require confirmation. Operator and admin may proceed; everyone else is denied.
type RoleApprovalReviewer struct{}

func (RoleApprovalReviewer) Review(ctx context.Context, _ *review.Request) (*review.Decision, error) {
	invocation, ok := InvocationFromContext(ctx)
	if !ok || !elevatedRole(invocation.Execution.Role) {
		return &review.Decision{Approved: false, RiskLevel: "high", Reason: "tool requires operator confirmation"}, nil
	}
	return &review.Decision{Approved: true, RiskLevel: "low", Reason: "elevated tenant role"}, nil
}

// InteractiveApprovalReviewer reuses the framework Approval Reviewer seam but
// delegates non-Web confirmations to the distributed platform approval broker.
// The Worker never sees channel credentials; it only waits for a decision.
type InteractiveApprovalReviewer struct {
	broker ApprovalRequester
}

func NewInteractiveApprovalReviewer(broker ApprovalRequester) (*InteractiveApprovalReviewer, error) {
	if broker == nil {
		return nil, errors.New("interactive approval broker is required")
	}
	return &InteractiveApprovalReviewer{broker: broker}, nil
}

func (r *InteractiveApprovalReviewer) Review(ctx context.Context, request *review.Request) (*review.Decision, error) {
	invocation, ok := InvocationFromContext(ctx)
	if !ok || request == nil || strings.TrimSpace(request.Action.ToolName) == "" {
		return &review.Decision{Approved: false, RiskLevel: "high", Reason: "approval context is incomplete"}, nil
	}
	execution := invocation.Execution
	if execution.Channel == "web" {
		return (RoleApprovalReviewer{}).Review(ctx, request)
	}
	approved, err := r.broker.Request(ctx, ApprovalRequest{
		TenantID: execution.TenantID, AppCode: execution.AppCode, ConfigVersion: execution.ConfigVersion,
		Channel: execution.Channel, BindingID: execution.BindingID, ConversationID: execution.ConversationID,
		ConversationScope: execution.ConversationScope, ExternalUserID: execution.ExternalUserID,
		ProgressMessageID: execution.ProgressMessageID, ProviderReplyToken: execution.ProviderReplyToken,
		ToolName: request.Action.ToolName, ToolDescription: request.Action.ToolDescription,
	})
	if err != nil {
		return nil, err
	}
	if !approved {
		return &review.Decision{Approved: false, RiskLevel: "high", Reason: "user rejected tool execution"}, nil
	}
	return &review.Decision{Approved: true, RiskLevel: "low", Reason: "user approved tool execution"}, nil
}
