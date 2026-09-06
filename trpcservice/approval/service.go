package approval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type Service struct {
	repository Repository
	journal    gateway.Journal
	audit      audit.Writer
}

func NewService(
	repository Repository,
	journal gateway.Journal,
	auditWriter audit.Writer,
) (*Service, error) {
	if repository == nil || journal == nil {
		return nil, fmt.Errorf("approval repository and journal are required")
	}
	return &Service{repository: repository, journal: journal, audit: auditWriter}, nil
}

func (s *Service) HandleApprovalDecision(
	ctx context.Context,
	input gateway.ApprovalDecisionInput,
) (bool, error) {
	status, approvalID, ok := ParseDecisionCommand(input.Text)
	if !ok && !looksLikeDecision(input.Text) {
		return false, nil
	}
	ctx, span := otel.Tracer("trpc-agent-service/approval").Start(ctx, "approval.decide")
	span.SetAttributes(attribute.String("tenant.id", input.TenantID), attribute.String("channel.binding.id", input.ChannelBindingID))
	defer span.End()
	if !ok {
		span.SetAttributes(attribute.String("approval.decision", "invalid_command"))
		return true, s.reject(ctx, input, "", "approval_invalid_command", formatHelp(input.Text))
	}
	record, err := s.repository.Decide(ctx, Decision{
		ApprovalID:        approvalID,
		TenantID:          input.TenantID,
		ChannelBindingID:  input.ChannelBindingID,
		UserID:            input.UserID,
		SessionID:         input.SessionID,
		ExternalMessageID: input.ExternalMessageID,
		Status:            status,
		Reason:            "IM user decision",
	})
	if err != nil {
		// A permanent denial must be acknowledged without falling through to
		// normal Agent execution or making the provider retry a poison message.
		if errorType := decisionErrorType(err); errorType != "" {
			return true, s.reject(ctx, input, approvalID, errorType, rejectionReply(errorType))
		}
		return true, err
	}
	// A human decision is a NEW inbound trace. Link it to the original tool
	// request; do not keep a span open while waiting or reparent to an old run.
	if origin := originSpanContext(record.OriginTraceParent); origin.IsValid() {
		span.AddLink(trace.Link{SpanContext: origin})
	}
	span.SetAttributes(attribute.String("approval.id", record.ApprovalID),
		attribute.String("approval.decision", record.Status), attribute.String("approval.origin_request_id", record.RequestID))
	scope, err := runtimecontext.NewScope(
		record.TenantID,
		record.AppID,
		record.RevisionID,
		input.ChannelType,
		record.ChannelBindingID,
	)
	if err != nil {
		return true, err
	}
	var accepted gateway.AcceptResult
	if record.Status == StatusApproved {
		// Keep the original continuation identity/payload for safe redelivery
		// of approvals created by older deployments.
		accepted, err = s.journal.Accept(ctx, gateway.InboundRequest{
			Scope: scope, ExternalMessageID: record.DecisionMessageID,
			UserID: record.UserID, SessionID: record.SessionID, ChatType: input.ChatType,
			Text:              record.ResumeText + "\n\n[平台可信上下文：用户已批准工具 " + record.ToolName + "]",
			ReplyTarget:       record.ReplyTarget,
			ApprovedToolCalls: []governance.ApprovedToolCall{{ToolName: record.ToolName, ArgumentsHash: record.ArgumentsHash}},
			ApprovalID:        record.ApprovalID,
		})
		if err != nil {
			return true, fmt.Errorf("enqueue approval continuation: %w", err)
		}
	}
	feedbackInput := input
	feedbackInput.Scope = scope
	// Provider redelivery keeps one receipt; a new user message gets a new
	// deterministic status receipt without changing the original continuation.
	feedbackInput.ExternalMessageID = input.ExternalMessageID
	feedbackInput.Text = record.Status + " " + record.ApprovalID
	feedbackInput.ReplyTarget = record.ReplyTarget
	text := "平台确认：审批 " + record.ApprovalID + " 已拒绝。本审批不会创建工具执行任务。"
	if record.Status == StatusApproved {
		text = "平台确认：审批 " + record.ApprovalID + " 已批准，执行任务已提交。批准不等于执行成功，执行结果会单独返回。"
	}
	if input.ExternalMessageID != record.DecisionMessageID {
		text = "平台提示：审批 " + record.ApprovalID + " 已拒绝。无需重复操作，本审批不会创建工具执行任务。"
		if record.Status == StatusApproved {
			text = "平台提示：审批 " + record.ApprovalID + " 已批准，本次重复确认不会额外创建执行任务；批准不等于执行成功，请查看原任务的执行结果。"
		}
	}
	receipt, err := s.feedback(ctx, feedbackInput, text)
	if err != nil {
		return true, err
	}
	if err := s.repository.MarkResumed(ctx, record.ApprovalID); err != nil {
		return true, err
	}
	if s.audit != nil {
		if err := s.audit.Record(ctx, audit.Event{
			TenantID:         record.TenantID,
			Channel:          input.ChannelType,
			ChannelBindingID: record.ChannelBindingID,
			UserID:           record.UserID,
			SessionID:        record.SessionID,
			MessageID:        input.ExternalMessageID,
			RequestID:        record.RequestID,
			RevisionID:       record.RevisionID,
			ToolName:         record.ToolName,
			Decision:         "approval_" + record.Status,
			TraceID:          audit.TraceID(ctx),
			Details: map[string]any{
				"approval_id":             record.ApprovalID,
				"continuation_request_id": accepted.RequestID,
				"receipt_request_id":      receipt.RequestID,
				"duplicate":               receipt.Duplicate,
			},
		}); err != nil {
			return true, err
		}
	}
	return true, nil
}

// Feedback has a separate stable ID from a tool continuation. It is persisted
// through Inbox/Outbound but never creates a Worker task or model invocation.
func (s *Service) feedback(ctx context.Context, input gateway.ApprovalDecisionInput, text string) (gateway.AcceptResult, error) {
	if input.Scope.TenantID != input.TenantID || input.Scope.ChannelBindingID != input.ChannelBindingID || input.Scope.ChannelType != input.ChannelType {
		return gateway.AcceptResult{}, fmt.Errorf("approval feedback scope mismatch")
	}
	hash := sha256.Sum256([]byte(input.ChannelBindingID + "\x00" + input.ExternalMessageID))
	return s.journal.Accept(ctx, gateway.InboundRequest{
		Scope: input.Scope, ExternalMessageID: "approval-receipt-" + hex.EncodeToString(hash[:]),
		UserID: input.UserID, SessionID: input.SessionID, ChatType: input.ChatType,
		Text: input.Text, ReplyTarget: input.ReplyTarget, DirectReply: text,
	})
}

func (s *Service) reject(ctx context.Context, input gateway.ApprovalDecisionInput, approvalID, errorType, text string) error {
	receipt, err := s.feedback(ctx, input, text)
	if err != nil {
		return err
	}
	if s.audit == nil {
		return nil
	}
	return s.audit.Record(ctx, audit.Event{
		TenantID: input.TenantID, Channel: input.ChannelType,
		ChannelBindingID: input.ChannelBindingID, UserID: input.UserID, SessionID: input.SessionID,
		MessageID: input.ExternalMessageID, RequestID: receipt.RequestID,
		Decision: "approval_rejected", ErrorType: errorType, TraceID: audit.TraceID(ctx),
		Details: map[string]any{"approval_id": approvalID, "receipt_request_id": receipt.RequestID, "duplicate": receipt.Duplicate},
	})
}

var approvalIDPattern = regexp.MustCompile(`apr_[0-9a-fA-F]{32}`)

func looksLikeDecision(text string) bool {
	lower := strings.ToLower(strings.TrimSpace(text))
	if strings.Contains(lower, "apr_") {
		return true
	}
	// This is only routing, never authorization. Malformed/ambiguous intent
	// gets a fixed help message, not an inferred approve/deny decision.
	fields := strings.Fields(lower)
	if len(fields) > 1 && strings.HasPrefix(fields[0], "@") {
		fields = fields[1:]
	}
	first := ""
	if len(fields) > 0 {
		first, _, _ = strings.Cut(strings.TrimPrefix(fields[0], "/"), "@")
	}
	for _, prefix := range []string{"批准", "拒绝", "同意", "取消", "撤销", "approve", "deny", "reject", "cancel"} {
		if first == prefix || (strings.Contains(lower, "审批") && strings.Contains(lower, prefix)) {
			return true
		}
	}
	return lower == "算了" || lower == "不用了" || lower == "不要执行" || lower == "先别执行" || lower == "取消操作"
}

func formatHelp(text string) string {
	id := "apr_审批编号"
	if matches := approvalIDPattern.FindAllString(text, -1); len(matches) == 1 {
		id = matches[0]
	}
	return "平台提示：未识别到有效的审批命令，本条消息没有更改审批状态，也没有执行工具。\n请使用 Telegram 的“回复”操作，回复审批提示；正文仅复制下面其中一行，不要带说明文字：\n\n批准 " + id + "\n\n拒绝 " + id
}

func rejectionReply(errorType string) string {
	switch errorType {
	case "approval_expired":
		return "平台提示：审批已过期，本次决定未被接受，也未创建工具执行任务。请重新发起申请。"
	case "approval_conflict":
		return "平台提示：审批已处理或决策消息发生冲突，不能通过本条消息更改原决定；未创建新的工具执行任务。"
	default:
		return "平台提示：审批不存在、无权处理或不属于当前会话。本条消息未处理审批，也未创建工具执行任务。"
	}
}

func ParseDecisionCommand(text string) (string, string, bool) {
	fields := strings.Fields(strings.TrimSpace(text))
	if len(fields) != 2 || !strings.HasPrefix(fields[1], "apr_") {
		return "", "", false
	}
	var status string
	switch strings.ToLower(fields[0]) {
	case "批准", "同意", "approve":
		status = StatusApproved
	case "拒绝", "deny", "reject":
		status = StatusDenied
	default:
		return "", "", false
	}
	if len(fields[1]) != len("apr_")+32 {
		return "", "", false
	}
	if _, err := hex.DecodeString(strings.TrimPrefix(fields[1], "apr_")); err != nil {
		return "", "", false
	}
	return status, fields[1], true
}

func decisionErrorType(err error) string {
	switch {
	case errors.Is(err, ErrForbidden):
		return "approval_forbidden"
	case errors.Is(err, ErrExpired):
		return "approval_expired"
	case errors.Is(err, ErrConflict):
		return "approval_conflict"
	case errors.Is(err, ErrNotFound):
		return "approval_not_found"
	default:
		return ""
	}
}

var _ gateway.ApprovalDecisionHandler = (*Service)(nil)
