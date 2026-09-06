package gateway

import (
	"context"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
)

// ApprovalDecisionInput carries one already verified and normalized IM text
// message to the approval subsystem.
type ApprovalDecisionInput struct {
	Scope             runtimecontext.Scope
	TenantID          string
	ChannelType       string
	ChannelBindingID  string
	ExternalMessageID string
	UserID            string
	SessionID         string
	ChatType          string
	Text              string
	ReplyTarget       string
}

type ApprovalDecisionHandler interface {
	HandleApprovalDecision(ctx context.Context, input ApprovalDecisionInput) (bool, error)
}
