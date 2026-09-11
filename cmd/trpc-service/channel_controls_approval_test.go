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

func (b *approvalFlowBroker) MarkNotified(_ context.Context, _ governance.PendingApproval, notificationID string) error {
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

type approvalUpdateSender struct {
	sends       []channels.OutboundMessage
	updateToken string
	updateCard  channels.InteractiveCard
	updateErr   error
	sendErr     error
}

func (s *approvalUpdateSender) Send(_ context.Context, _ channels.ReplyTarget, message channels.OutboundMessage) (channels.SendReceipt, error) {
	s.sends = append(s.sends, message)
	return channels.SendReceipt{ExternalMessageID: "replacement"}, s.sendErr
}

func (s *approvalUpdateSender) UpdateCard(_ context.Context, _ channels.ReplyTarget, token string, card channels.InteractiveCard) error {
	s.updateToken = token
	s.updateCard = card
	return s.updateErr
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
		ExternalUserID: "user-a", ProgressMessageID: "progress-1", ProviderReplyToken: "reply-token-1",
		ToolName: "request_refund", ToolDescription: "提交退款申请",
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

func TestApprovalReconcileFallsBackToNewCardWhenRecoveredReplyTokenIsGone(t *testing.T) {
	broker := &approvalFlowBroker{pending: []governance.PendingApproval{{
		Token: "token-1", TenantID: "tenant-a", AppCode: "support", ConfigVersion: 3,
		Channel: string(channels.WeCom), BindingID: "bot-a", ConversationID: "chat-a", ConversationScope: string(channels.ConversationDirect),
		ExternalUserID: "user-a", ProgressMessageID: "progress-1",
		ToolName: "request_refund", ToolDescription: "提交退款申请",
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
	if len(sender.progressCardUpdates) != 0 {
		t.Fatalf("recovered approval attempted progress update without reply token: %#v", sender.progressCardUpdates)
	}
	if len(sender.sends) != 1 || sender.sends[0].Card == nil {
		t.Fatalf("recovered approval sends = %#v, want one proactive approval card", sender.sends)
	}
	if broker.notifiedID != "new-message" {
		t.Fatalf("notification id = %q, want proactive send receipt", broker.notifiedID)
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

func TestReplaceApprovalCardUsesNativeUpdateThenStableMessageFallback(t *testing.T) {
	t.Parallel()
	card := channels.InteractiveCard{Title: "已确认", Body: "操作已确认", State: "approved"}
	baseInbound := channels.InboundMessage{
		Channel: channels.WeCom, ConversationID: "chat-a", ConversationScope: channels.ConversationDirect,
		Action: &channels.InboundAction{Token: "callback-token", OriginMessageID: "message-1"},
	}

	t.Run("native card updater", func(t *testing.T) {
		sender := &approvalUpdateSender{}
		handler := &channelControlHandler{resolveSender: func(context.Context, string, string, uint64, channels.BindingKey) (channels.Sender, error) {
			return sender, nil
		}}
		if err := handler.replaceApprovalCard(context.Background(), "tenant-a", "support", 3, "bot-a", baseInbound, card); err != nil {
			t.Fatal(err)
		}
		if sender.updateToken != "callback-token" || sender.updateCard.State != "approved" || len(sender.sends) != 0 {
			t.Fatalf("native update token/card/sends = %q %#v %#v", sender.updateToken, sender.updateCard, sender.sends)
		}
	})

	t.Run("message replacement fallback", func(t *testing.T) {
		sender := &approvalUpdateSender{updateErr: errors.New("callback expired")}
		handler := &channelControlHandler{resolveSender: func(context.Context, string, string, uint64, channels.BindingKey) (channels.Sender, error) {
			return sender, nil
		}}
		if err := handler.replaceApprovalCard(context.Background(), "tenant-a", "support", 3, "bot-a", baseInbound, card); err != nil {
			t.Fatal(err)
		}
		if len(sender.sends) != 1 || sender.sends[0].UpdateMessageID != "message-1" || sender.sends[0].Card == nil || sender.sends[0].Card.State != "approved" {
			t.Fatalf("fallback sends = %#v", sender.sends)
		}
	})

	t.Run("both update paths fail", func(t *testing.T) {
		sender := &approvalUpdateSender{updateErr: errors.New("callback expired"), sendErr: errors.New("message unavailable")}
		handler := &channelControlHandler{resolveSender: func(context.Context, string, string, uint64, channels.BindingKey) (channels.Sender, error) {
			return sender, nil
		}}
		err := handler.replaceApprovalCard(context.Background(), "tenant-a", "support", 3, "bot-a", baseInbound, card)
		if err == nil || !strings.Contains(err.Error(), "update provider approval card") || !strings.Contains(err.Error(), "replace provider approval message") {
			t.Fatalf("combined update error = %v", err)
		}
	})

	t.Run("no in-place capability", func(t *testing.T) {
		sender := &approvalFlowSender{}
		handler := &channelControlHandler{resolveSender: func(context.Context, string, string, uint64, channels.BindingKey) (channels.Sender, error) {
			return sender, nil
		}}
		inbound := baseInbound
		inbound.Action = &channels.InboundAction{}
		err := handler.replaceApprovalCard(context.Background(), "tenant-a", "support", 3, "bot-a", inbound, card)
		if err == nil || !strings.Contains(err.Error(), "does not support in-place") {
			t.Fatalf("unsupported update error = %v", err)
		}
	})

	t.Run("resolver failure", func(t *testing.T) {
		wantErr := errors.New("sender unavailable")
		handler := &channelControlHandler{resolveSender: func(context.Context, string, string, uint64, channels.BindingKey) (channels.Sender, error) {
			return nil, wantErr
		}}
		if err := handler.replaceApprovalCard(context.Background(), "tenant-a", "support", 3, "bot-a", baseInbound, card); !errors.Is(err, wantErr) {
			t.Fatalf("resolver error = %v", err)
		}
	})
}

func TestApprovalResultTitleMatchesDecision(t *testing.T) {
	t.Parallel()
	if got := approvalResultTitle(true); got != "已确认" {
		t.Fatalf("approved title = %q", got)
	}
	if got := approvalResultTitle(false); got != "已取消" {
		t.Fatalf("rejected title = %q", got)
	}
}

func TestTelegramStartIntroducesDirectUseAndDiscoverableCommands(t *testing.T) {
	sender := &approvalFlowSender{}
	handler := &channelControlHandler{
		approvals: &approvalFlowBroker{},
		resolveSender: func(context.Context, string, string, uint64, channels.BindingKey) (channels.Sender, error) {
			return sender, nil
		},
	}
	snapshot := tenant.Snapshot{Config: config.TenantConfig{TenantID: "tenant-a", AppCode: "support", ConfigVersion: 1}}
	inbound := channels.InboundMessage{
		MessageID: "message-1", Channel: channels.Telegram, ConversationID: "chat-1", SenderID: "customer-1",
		ConversationScope: channels.ConversationDirect, Text: "/start",
	}

	handled, err := handler.handle(context.Background(), snapshot, "support-bot", inbound)
	if err != nil || !handled {
		t.Fatalf("handle(/start) = handled %v, error %v", handled, err)
	}
	if len(sender.sends) != 1 {
		t.Fatalf("/start sends = %d, want 1", len(sender.sends))
	}
	reply := sender.sends[0].Text
	for _, text := range []string{"直接发送消息", "/new", "/help"} {
		if !strings.Contains(reply, text) {
			t.Fatalf("/start reply %q does not introduce %q", reply, text)
		}
	}
	if strings.Contains(strings.ToLower(reply), "/link") || strings.Contains(reply, "关联账号") {
		t.Fatalf("/start must not mix bot usage with login identity linking: %q", reply)
	}
}

func TestHelpListsNewSessionWithoutLoginIdentityLinking(t *testing.T) {
	sender := &approvalFlowSender{}
	handler := &channelControlHandler{
		approvals: &approvalFlowBroker{},
		resolveSender: func(context.Context, string, string, uint64, channels.BindingKey) (channels.Sender, error) {
			return sender, nil
		},
	}
	snapshot := tenant.Snapshot{Config: config.TenantConfig{TenantID: "tenant-a", AppCode: "support", ConfigVersion: 1}}
	inbound := channels.InboundMessage{
		MessageID: "message-1", Channel: channels.Feishu, ConversationID: "chat-1", SenderID: "customer-1",
		ConversationScope: channels.ConversationDirect, Text: "/help",
	}

	handled, err := handler.handle(context.Background(), snapshot, "support-bot", inbound)
	if err != nil || !handled || len(sender.sends) != 1 {
		t.Fatalf("handle(/help) = handled %v, sends %d, error %v", handled, len(sender.sends), err)
	}
	reply := sender.sends[0].Text
	if !strings.Contains(reply, "/new") || strings.Contains(strings.ToLower(reply), "/link") {
		t.Fatalf("/help reply = %q", reply)
	}
}
