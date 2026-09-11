package governance

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
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
	audits storage.AuditRecorder
}

func NewInteractiveApprovalReviewer(broker ApprovalRequester, audits storage.AuditRecorder) (*InteractiveApprovalReviewer, error) {
	if broker == nil {
		return nil, errors.New("interactive approval broker is required")
	}
	if audits == nil {
		return nil, errors.New("interactive approval audit recorder is required")
	}
	return &InteractiveApprovalReviewer{broker: broker, audits: audits}, nil
}

func (r *InteractiveApprovalReviewer) Review(ctx context.Context, request *review.Request) (*review.Decision, error) {
	invocation, ok := InvocationFromContext(ctx)
	if !ok || request == nil || strings.TrimSpace(request.Action.ToolName) == "" {
		return &review.Decision{Approved: false, RiskLevel: "high", Reason: "approval context is incomplete"}, nil
	}
	execution := invocation.Execution
	started := time.Now().UTC()
	if err := r.recordAudit(ctx, execution, request.Action.ToolName, "approval.requested", "pending", "", 0, started); err != nil {
		return nil, err
	}
	approved, err := r.broker.Request(ctx, ApprovalRequest{
		TenantID: execution.TenantID, AppCode: execution.AppCode, ConfigVersion: execution.ConfigVersion,
		RequestID: execution.RequestID, TraceID: execution.TraceID,
		Channel: execution.Channel, BindingID: execution.BindingID, ConversationID: execution.ConversationID,
		ConversationScope: execution.ConversationScope, ExternalUserID: execution.ExternalUserID,
		RequesterUserID:   execution.UserID,
		ProgressMessageID: execution.ProgressMessageID, ProviderReplyToken: execution.ProviderReplyToken,
		ToolName: request.Action.ToolName, ToolDescription: request.Action.ToolDescription,
	})
	if err != nil {
		if errors.Is(err, ErrApprovalExpired) {
			if auditErr := r.recordAudit(ctx, execution, request.Action.ToolName, "approval.expired", "rejected", "approval_expired", time.Since(started), time.Now().UTC()); auditErr != nil {
				return nil, errors.Join(err, auditErr)
			}
			return &review.Decision{Approved: false, RiskLevel: "high", Reason: "approval request expired"}, nil
		}
		if auditErr := r.recordAudit(ctx, execution, request.Action.ToolName, "approval.failed", "failed", "approval_failed", time.Since(started), time.Now().UTC()); auditErr != nil {
			return nil, errors.Join(err, auditErr)
		}
		return nil, err
	}
	if !approved {
		if err := r.recordAudit(ctx, execution, request.Action.ToolName, "approval.rejected", "rejected", "", time.Since(started), time.Now().UTC()); err != nil {
			return nil, err
		}
		return &review.Decision{Approved: false, RiskLevel: "high", Reason: "user rejected tool execution"}, nil
	}
	if err := r.recordAudit(ctx, execution, request.Action.ToolName, "approval.approved", "approved", "", time.Since(started), time.Now().UTC()); err != nil {
		return nil, err
	}
	return &review.Decision{Approved: true, RiskLevel: "low", Reason: "user approved tool execution"}, nil
}

func (r *InteractiveApprovalReviewer) recordAudit(
	ctx context.Context,
	execution ExecutionContext,
	toolName, action, decision, errorType string,
	latency time.Duration,
	createdAt time.Time,
) error {
	if err := r.audits.RecordAudit(ctx, storage.AuditEvent{
		TenantID: execution.TenantID, TraceID: execution.TraceID, RequestID: execution.RequestID,
		Channel: execution.Channel, UserID: execution.UserID, SessionID: execution.SessionID,
		AgentName: execution.AgentName, ToolName: toolName,
		Action: action, Result: decision, Decision: decision, ErrorType: errorType,
		LatencyMS: latency.Milliseconds(), CreatedAt: createdAt,
	}); err != nil {
		return fmt.Errorf("record approval audit: %w", err)
	}
	return nil
}
