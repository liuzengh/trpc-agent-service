package governance

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/plugin/guardrail/approval/review"
)

type approvalRequesterFunc func(context.Context, ApprovalRequest) (bool, error)

func (f approvalRequesterFunc) Request(ctx context.Context, request ApprovalRequest) (bool, error) {
	return f(ctx, request)
}

func TestInteractiveApprovalReviewerUsesIMRoute(t *testing.T) {
	var got ApprovalRequest
	audits := storage.NewMemoryStateStore()
	reviewer, err := NewInteractiveApprovalReviewer(approvalRequesterFunc(func(_ context.Context, request ApprovalRequest) (bool, error) {
		got = request
		return true, nil
	}), audits)
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
	events, err := audits.ListAudit(context.Background(), "trailforge", "trace-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Action != "approval.requested" || events[1].Action != "approval.approved" ||
		events[0].ToolName != "request_refund" || events[1].Decision != "approved" {
		t.Fatalf("approval audit events = %+v", events)
	}
}

func TestInteractiveApprovalReviewerPersistsExpiredApproval(t *testing.T) {
	audits := storage.NewMemoryStateStore()
	reviewer, err := NewInteractiveApprovalReviewer(approvalRequesterFunc(func(context.Context, ApprovalRequest) (bool, error) {
		return false, ErrApprovalExpired
	}), audits)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithInvocation(context.Background(), Invocation{Execution: ExecutionContext{
		TenantID: "tenant-a", AppCode: "support", ConfigVersion: 3, Role: "member", TraceID: "trace-expired", RequestID: "message-1", PolicyVersion: "3",
		Channel: "wecom", BindingID: "support-wecom", ConversationID: "customer-1", ExternalUserID: "customer-1",
	}, Budget: NewCallBudget(1)})
	decision, err := reviewer.Review(ctx, &review.Request{Action: review.Action{ToolName: "request_refund", ToolDescription: "提交退款"}})
	if err != nil {
		t.Fatalf("Review() error = %v, want nil", err)
	}
	if decision == nil || decision.Approved || decision.Reason != "approval request expired" {
		t.Fatalf("Review() decision = %#v, want expired denial", decision)
	}
	events, err := audits.ListAudit(context.Background(), "tenant-a", "trace-expired")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Action != "approval.requested" || events[1].Action != "approval.expired" ||
		events[1].Decision != "rejected" || events[1].ErrorType != "approval_expired" || events[1].CreatedAt.Before(events[0].CreatedAt) {
		t.Fatalf("approval audit events = %+v", events)
	}
}

func TestInteractiveApprovalReviewerPropagatesInfrastructureFailure(t *testing.T) {
	audits := storage.NewMemoryStateStore()
	wantErr := errors.New("redis unavailable")
	reviewer, err := NewInteractiveApprovalReviewer(approvalRequesterFunc(func(context.Context, ApprovalRequest) (bool, error) {
		return false, wantErr
	}), audits)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithInvocation(context.Background(), Invocation{Execution: ExecutionContext{
		TenantID: "tenant-a", AppCode: "support", ConfigVersion: 3, Role: "member", TraceID: "trace-failed", RequestID: "message-2", PolicyVersion: "3",
		Channel: "feishu", BindingID: "support-feishu", ConversationID: "chat-1", ExternalUserID: "user-1",
	}, Budget: NewCallBudget(1)})
	decision, err := reviewer.Review(ctx, &review.Request{Action: review.Action{ToolName: "request_refund"}})
	if !errors.Is(err, wantErr) || decision != nil {
		t.Fatalf("Review() = %#v, %v, want infrastructure error", decision, err)
	}
}

func TestInteractiveApprovalReviewerFailsClosedForWeb(t *testing.T) {
	audits := storage.NewMemoryStateStore()
	brokerCalls := 0
	reviewer, err := NewInteractiveApprovalReviewer(approvalRequesterFunc(func(context.Context, ApprovalRequest) (bool, error) {
		brokerCalls++
		return true, nil
	}), audits)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithInvocation(context.Background(), Invocation{Execution: ExecutionContext{
		TenantID: "tenant-a", AppCode: "support", ConfigVersion: 3, Role: "admin", TraceID: "trace-web", RequestID: "message-web", PolicyVersion: "3",
		Channel: "web", BindingID: "web-console", ConversationID: "chat-1", ExternalUserID: "admin-1", UserID: "admin-1", SessionID: "tenant-a/support/session/1",
	}, Budget: NewCallBudget(1)})
	decision, err := reviewer.Review(ctx, &review.Request{Action: review.Action{ToolName: "request_refund"}})
	if err != nil {
		t.Fatal(err)
	}
	if decision == nil || decision.Approved || decision.Reason != "web interactive approval is unavailable" || brokerCalls != 0 {
		t.Fatalf("Review(web) = %#v, broker calls=%d, want fail-closed denial", decision, brokerCalls)
	}
	events, err := audits.ListAudit(context.Background(), "tenant-a", "trace-web")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Action != "approval.rejected" || events[0].Decision != "rejected" || events[0].ErrorType != "web_approval_unavailable" {
		t.Fatalf("web approval audit events = %+v", events)
	}
}
