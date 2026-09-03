package approval

import (
	"context"
	"fmt"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
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
	if !ok {
		return false, nil
	}
	record, err := s.repository.Decide(ctx, Decision{
		ApprovalID:        approvalID,
		TenantID:          input.TenantID,
		ChannelBindingID:  input.ChannelBindingID,
		UserID:            input.UserID,
		ExternalMessageID: input.ExternalMessageID,
		Status:            status,
		Reason:            "IM user decision",
	})
	if err != nil {
		return true, err
	}
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
	text := record.ResumeText
	approvedCalls := []governance.ApprovedToolCall(nil)
	if record.Status == StatusApproved {
		approvedCalls = []governance.ApprovedToolCall{{
			ToolName: record.ToolName, ArgumentsHash: record.ArgumentsHash,
		}}
		text += "\n\n[平台可信上下文：用户已批准工具 " + record.ToolName + "]"
	} else {
		text = "用户拒绝执行工具 " + record.ToolName + "。请确认操作已取消，不要调用该工具。"
	}
	_, err = s.journal.Accept(ctx, gateway.InboundRequest{
		Scope:             scope,
		ExternalMessageID: record.DecisionMessageID,
		UserID:            record.UserID,
		SessionID:         record.SessionID,
		ChatType:          input.ChatType,
		Text:              text,
		ReplyTarget:       record.ReplyTarget,
		ApprovedToolCalls: approvedCalls,
		ApprovalID:        record.ApprovalID,
	})
	if err != nil {
		return true, fmt.Errorf("enqueue approval continuation: %w", err)
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
			MessageID:        record.DecisionMessageID,
			RequestID:        record.RequestID,
			RevisionID:       record.RevisionID,
			ToolName:         record.ToolName,
			Decision:         "approval_" + record.Status,
			TraceID:          audit.TraceID(ctx),
			Details: map[string]any{
				"approval_id": record.ApprovalID,
			},
		}); err != nil {
			return true, err
		}
	}
	return true, nil
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
	return status, fields[1], true
}

var _ gateway.ApprovalDecisionHandler = (*Service)(nil)
