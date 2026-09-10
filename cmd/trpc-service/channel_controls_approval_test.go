package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type approvalFlowBroker struct {
	pending    []governance.PendingApproval
	resolved   governance.PendingApproval
	resolveErr error
	notifiedID string
}

func (b *approvalFlowBroker) Request(context.Context, governance.ApprovalRequest) (bool, error) {
	return false, errors.New("unexpected approval request")
}

func (b *approvalFlowBroker) ListPending(context.Context, int) ([]governance.PendingApproval, error) {
	return append([]governance.PendingApproval(nil), b.pending...), nil
}

func (b *approvalFlowBroker) MarkNotified(_ context.Context, _ string, notificationID string) error {
	b.notifiedID = notificationID
	return nil
}

func (b *approvalFlowBroker) Resolve(context.Context, governance.ApprovalResolution) (governance.PendingApproval, error) {
	return b.resolved, b.resolveErr
}

type approvalFlowSender struct {
	sends               []channels.OutboundMessage
	progressCardUpdates []string
}

func (s *approvalFlowSender) Send(_ context.Context, _ channels.ReplyTarget, message channels.OutboundMessage) (channels.SendReceipt, error) {
	s.sends = append(s.sends, message)
	return channels.SendReceipt{ExternalMessageID: "new-message"}, nil
}

func (s *approvalFlowSender) UpdateProgressCard(_ context.Context, _ channels.ReplyTarget, messageID string, _ channels.InteractiveCard) error {
	s.progressCardUpdates = append(s.progressCardUpdates, messageID)
	return nil
}

func TestApprovalPromptCardExplainsOperationBeforeTechnicalToolName(t *testing.T) {
	t.Parallel()
	card := approvalPromptCard(governance.PendingApproval{
		Token: "token-1", ToolName: "request_refund", ToolDescription: "为指定订单发起退款申请",
	})
	if card.Title != "确认执行操作" || !strings.Contains(card.Body, "为指定订单发起退款申请") ||
		strings.Contains(card.Body, "request_refund") {
		t.Fatalf("approval card = %#v", card)
	}
	if len(card.Actions) != 2 || card.Actions[0].Label != "确认执行" || card.Actions[1].Label != "取消" {
		t.Fatalf("approval actions = %#v", card.Actions)
	}
}

func TestApprovalOperationSummaryDoesNotExposeModelToolPolicy(t *testing.T) {
	t.Parallel()
	description := "为测试订单 TF-1002 正式提交退款申请，这是有副作用的操作。仅当用户明确要求发起/提交退款时调用；查询订单状态时绝不能调用。"
	if got, want := approvalOperationSummary(description, "request_refund"), "为测试订单 TF-1002 正式提交退款申请"; got != want {
		t.Fatalf("approvalOperationSummary() = %q, want %q", got, want)
	}
}

func TestApprovalPromptCardHasReadableFallbackWithoutDescription(t *testing.T) {
	t.Parallel()
	card := approvalPromptCard(governance.PendingApproval{Token: "token-1", ToolName: "request_refund"})
	if !strings.Contains(card.Body, "执行操作：request_refund") {
		t.Fatalf("approval fallback card = %#v", card)
	}
}

func TestApprovalReconcileReusesIngressProgressMessage(t *testing.T) {
	broker := &approvalFlowBroker{pending: []governance.PendingApproval{{
		Token: "token-1", TenantID: "tenant-a", AppCode: "support", ConfigVersion: 3,
		Channel: string(channels.Feishu), BindingID: "bot-a", ConversationID: "chat-a", ConversationScope: string(channels.ConversationDirect),
		ExternalUserID: "user-a", ProgressMessageID: "progress-1", ToolName: "request_refund", ToolDescription: "提交退款申请",
	}}}
	sender := &approvalFlowSender{}
	handler := &channelControlHandler{
		approvals: broker,
		resolveSender: func(context.Context, string, string, uint64, channels.BindingKey) (channels.Sender, error) {
			return sender, nil
		},
	}

	if err := handler.reconcileApprovals(context.Background()); err != nil {
		t.Fatalf("reconcileApprovals() error = %v", err)
	}
	if len(sender.progressCardUpdates) != 1 || sender.progressCardUpdates[0] != "progress-1" {
		t.Fatalf("progress card updates = %#v, want original progress message", sender.progressCardUpdates)
	}
	if len(sender.sends) != 0 {
		t.Fatalf("approval created %d extra messages, want zero", len(sender.sends))
	}
	if broker.notifiedID != "progress-1" {
		t.Fatalf("notification id = %q, want progress-1", broker.notifiedID)
	}
}

func TestRepeatedApprovalClickIsSilent(t *testing.T) {
	broker := &approvalFlowBroker{resolveErr: governance.ErrApprovalResolved}
	sender := &approvalFlowSender{}
	handler := &channelControlHandler{
		approvals: broker,
		resolveSender: func(context.Context, string, string, uint64, channels.BindingKey) (channels.Sender, error) {
			return sender, nil
		},
	}
	snapshot := tenant.Snapshot{Config: config.TenantConfig{TenantID: "tenant-a", AppCode: "support", ConfigVersion: 3}}
	inbound := channels.InboundMessage{
		Channel: channels.Feishu, ConversationID: "chat-a", SenderID: "user-a", ConversationScope: channels.ConversationDirect,
		Action: &channels.InboundAction{ActionID: "approval:approve:token-1", OriginMessageID: "progress-1"},
	}

	if err := handler.handleAction(context.Background(), snapshot, "bot-a", inbound); err != nil {
		t.Fatalf("handleAction() error = %v", err)
	}
	if len(sender.sends) != 0 {
		t.Fatalf("repeated approval click sent %d extra messages, want silent no-op", len(sender.sends))
	}
}
