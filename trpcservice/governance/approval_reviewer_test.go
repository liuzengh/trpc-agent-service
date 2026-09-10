package governance

import (
	"context"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/plugin/guardrail/approval/review"
)

type approvalRequesterFunc func(context.Context, ApprovalRequest) (bool, error)

func (f approvalRequesterFunc) Request(ctx context.Context, request ApprovalRequest) (bool, error) {
	return f(ctx, request)
}

func TestInteractiveApprovalReviewerUsesIMRoute(t *testing.T) {
	var got ApprovalRequest
	reviewer, err := NewInteractiveApprovalReviewer(approvalRequesterFunc(func(_ context.Context, request ApprovalRequest) (bool, error) {
		got = request
		return true, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithInvocation(context.Background(), Invocation{
		Execution: ExecutionContext{
			TenantID: "trailforge", AppCode: "assistant", ConfigVersion: 8, Role: "member", TraceID: "trace-1", PolicyVersion: "8",
			Channel: "telegram", BindingID: "bot-a", ConversationID: "chat-1", ConversationScope: "direct", ExternalUserID: "tg-user",
			ProgressMessageID: "progress-1", ProviderReplyToken: "reply-token-1",
		},
		Budget: NewCallBudget(1),
	})
	decision, err := reviewer.Review(ctx, &review.Request{Action: review.Action{
		ToolName: "request_refund", ToolDescription: "为指定订单发起退款申请",
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !decision.Approved || got.BindingID != "bot-a" || got.ExternalUserID != "tg-user" ||
		got.ProgressMessageID != "progress-1" || got.ProviderReplyToken != "reply-token-1" ||
		got.ToolName != "request_refund" || got.ToolDescription != "为指定订单发起退款申请" {
		t.Fatalf("decision/request = %#v / %#v", decision, got)
	}
}
