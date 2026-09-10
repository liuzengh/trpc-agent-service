package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type channelSenderResolver func(context.Context, string, string, uint64, channels.BindingKey) (channels.Sender, error)

type channelControlHandler struct {
	approvals     governance.ApprovalBroker
	resolveSender channelSenderResolver
}

func newChannelControlHandler(
	approvals governance.ApprovalBroker,
	resolveSender channelSenderResolver,
) (*channelControlHandler, error) {
	if approvals == nil || resolveSender == nil {
		return nil, errors.New("channel control dependencies are incomplete")
	}
	return &channelControlHandler{approvals: approvals, resolveSender: resolveSender}, nil
}

func (h *channelControlHandler) reconcileApprovals(ctx context.Context) error {
	pending, err := h.approvals.ListPending(ctx, 50)
	if err != nil {
		return err
	}
	for _, approval := range pending {
		channel := channels.Channel(approval.Channel)
		if channel != channels.Telegram && channel != channels.WeCom && channel != channels.Feishu {
			continue
		}
		sender, err := h.resolveSender(ctx, approval.TenantID, approval.AppCode, approval.ConfigVersion, channels.BindingKey{Channel: channel, BindingID: approval.BindingID})
		if err != nil {
			return fmt.Errorf("resolve approval sender: %w", err)
		}
		card := approvalPromptCard(approval)
		target := channels.ReplyTarget{
			TenantID: approval.TenantID, Channel: channel, BindingID: approval.BindingID,
			ConversationID: approval.ConversationID, ConversationScope: channels.ConversationScope(approval.ConversationScope),
			ProviderReplyToken: approval.ProviderReplyToken,
		}
		if progressID := strings.TrimSpace(approval.ProgressMessageID); progressID != "" {
			if updater, ok := sender.(channels.ProgressCardSender); ok {
				if err := updater.UpdateProgressCard(ctx, target, progressID, card); err != nil {
					return fmt.Errorf("update approval progress message: %w", err)
				}
				if err := h.approvals.MarkNotified(ctx, approval.Token, progressID); err != nil {
					return fmt.Errorf("mark approval card notified: %w", err)
				}
				continue
			}
		}
		receipt, err := sender.Send(ctx, target, channels.OutboundMessage{Card: &card, IdempotencyKey: "approval:" + approval.Token})
		if err != nil {
			return fmt.Errorf("send approval card: %w", err)
		}
		if err := h.approvals.MarkNotified(ctx, approval.Token, receipt.ExternalMessageID); err != nil {
			return fmt.Errorf("mark approval card notified: %w", err)
		}
	}
	return nil
}

func approvalPromptCard(approval governance.PendingApproval) channels.InteractiveCard {
	description := approvalOperationSummary(approval.ToolDescription, approval.ToolName)
	return channels.InteractiveCard{
		Title: "确认执行操作",
		Body:  description + "\n\n确认后将执行此操作。",
		State: "pending",
		Actions: []channels.CardAction{
			{ActionID: "approval:approve:" + approval.Token, Label: "确认执行", Style: "primary"},
			{ActionID: "approval:reject:" + approval.Token, Label: "取消", Style: "danger"},
		},
	}
}

func (h *channelControlHandler) handle(ctx context.Context, snapshot tenant.Snapshot, bindingID string, inbound channels.InboundMessage) (bool, error) {
	if inbound.Action != nil {
		// Provider-native actions are reserved for platform controls such as
		// approvals. Unknown action IDs never reach the LLM as user text.
		return true, h.handleAction(ctx, snapshot, bindingID, inbound)
	}
	fields := strings.Fields(strings.TrimSpace(inbound.Text))
	if len(fields) == 0 {
		return false, nil
	}
	command := strings.ToLower(strings.SplitN(fields[0], "@", 2)[0])
	switch command {
	case "/help":
		return true, h.sendReply(ctx, snapshot, bindingID, inbound, "可用指令：/new 开启新会话。")
	case "/start":
		if inbound.Channel != channels.Telegram {
			return false, nil
		}
		return true, h.sendReply(ctx, snapshot, bindingID, inbound, "机器人已就绪。")
	default:
		return false, nil
	}
}

func (h *channelControlHandler) handleAction(ctx context.Context, snapshot tenant.Snapshot, bindingID string, inbound channels.InboundMessage) error {
	if inbound.Action == nil {
		return nil
	}
	token, approved, ok := parseApprovalAction(inbound.Action.ActionID)
	if !ok {
		return h.sendReply(ctx, snapshot, bindingID, inbound, "该交互已失效或无法识别，请重新发起操作。")
	}
	resolved, err := h.approvals.Resolve(ctx, governance.ApprovalResolution{
		Token: token, TenantID: snapshot.Config.TenantID, Channel: string(inbound.Channel), BindingID: bindingID,
		ConversationID: inbound.ConversationID, ExternalUserID: inbound.SenderID, Approved: approved,
	})
	if err != nil {
		switch {
		case errors.Is(err, governance.ErrApprovalExpired):
			return h.replaceApprovalCard(ctx, snapshot.Config.TenantID, snapshot.Config.AppCode, snapshot.Config.ConfigVersion, bindingID, inbound, channels.InteractiveCard{
				Title: "确认已过期", Body: "该确认已过期，请重新发起操作。", State: "rejected",
			})
		case errors.Is(err, governance.ErrApprovalResolved):
			// Provider callbacks may be delivered more than once or race with the
			// card replacement. Resolve is atomic, so a repeated click is a
			// successful idempotent no-op and must not create another message.
			return nil
		case errors.Is(err, governance.ErrApprovalRouteMismatch):
			return h.sendReply(ctx, snapshot, bindingID, inbound, "你无权处理这项确认。")
		default:
			return err
		}
	}
	slog.Info("approval action resolved", "channel", inbound.Channel, "binding_id", bindingID, "approved", approved)
	description := approvalOperationSummary(resolved.ToolDescription, resolved.ToolName)
	state, body := "rejected", "已取消："+description
	if approved {
		state, body = "approved", "已确认："+description
	}
	card := channels.InteractiveCard{Title: approvalResultTitle(approved), Body: body, State: state}
	return h.replaceApprovalCard(ctx, resolved.TenantID, resolved.AppCode, resolved.ConfigVersion, bindingID, inbound, card)
}

func (h *channelControlHandler) replaceApprovalCard(
	ctx context.Context,
	tenantID, appCode string,
	configVersion uint64,
	bindingID string,
	inbound channels.InboundMessage,
	card channels.InteractiveCard,
) error {
	sender, err := h.resolveSender(ctx, tenantID, appCode, configVersion, channels.BindingKey{Channel: inbound.Channel, BindingID: bindingID})
	if err != nil {
		return err
	}
	target := channels.ReplyTarget{
		TenantID: tenantID, Channel: inbound.Channel, BindingID: bindingID,
		ConversationID: inbound.ConversationID, ConversationScope: inbound.ConversationScope,
		ProviderReplyToken: inbound.ProviderReplyToken,
	}
	var updateErr error
	if updater, ok := sender.(channels.CardUpdater); ok && strings.TrimSpace(inbound.Action.Token) != "" {
		if err := updater.UpdateCard(ctx, target, inbound.Action.Token, card); err == nil {
			slog.Info("approval card updated", "channel", inbound.Channel, "binding_id", bindingID, "state", card.State)
			return nil
		} else {
			updateErr = fmt.Errorf("update provider approval card: %w", err)
		}
	}
	if strings.TrimSpace(inbound.Action.OriginMessageID) != "" {
		_, err := sender.Send(ctx, target, channels.OutboundMessage{Card: &card, UpdateMessageID: inbound.Action.OriginMessageID})
		if err == nil {
			slog.Info("approval card updated", "channel", inbound.Channel, "binding_id", bindingID, "state", card.State)
			return nil
		}
		updateErr = errors.Join(updateErr, fmt.Errorf("replace provider approval message: %w", err))
	}
	if updateErr != nil {
		return updateErr
	}
	return errors.New("provider does not support in-place approval updates")
}

func approvalOperationSummary(description, toolName string) string {
	text := strings.TrimSpace(description)
	if text == "" {
		if name := strings.TrimSpace(toolName); name != "" {
			return "执行操作：" + name
		}
		return "执行这项操作"
	}
	for _, separator := range []string{"。", "；", "\n", "，这是"} {
		if index := strings.Index(text, separator); index > 0 {
			text = strings.TrimSpace(text[:index])
		}
	}
	runes := []rune(text)
	if len(runes) > 80 {
		text = string(runes[:80]) + "…"
	}
	return text
}

func approvalResultTitle(approved bool) string {
	if approved {
		return "已确认"
	}
	return "已取消"
}

func parseApprovalAction(actionID string) (string, bool, bool) {
	parts := strings.Split(strings.TrimSpace(actionID), ":")
	if len(parts) != 3 || parts[0] != "approval" || strings.TrimSpace(parts[2]) == "" {
		return "", false, false
	}
	switch parts[1] {
	case "approve":
		return parts[2], true, true
	case "reject":
		return parts[2], false, true
	default:
		return "", false, false
	}
}

func (h *channelControlHandler) sendReply(ctx context.Context, snapshot tenant.Snapshot, bindingID string, inbound channels.InboundMessage, text string) error {
	sender, err := h.resolveSender(ctx, snapshot.Config.TenantID, snapshot.Config.AppCode, snapshot.Config.ConfigVersion, channels.BindingKey{Channel: inbound.Channel, BindingID: bindingID})
	if err != nil {
		return fmt.Errorf("resolve control reply sender: %w", err)
	}
	_, err = sender.Send(ctx, channels.ReplyTarget{
		TenantID: snapshot.Config.TenantID, Channel: inbound.Channel, BindingID: bindingID, ConversationID: inbound.ConversationID,
		ConversationScope: inbound.ConversationScope, ProviderReplyToken: inbound.ProviderReplyToken,
	}, channels.OutboundMessage{Text: text, IdempotencyKey: "control:" + inbound.MessageID})
	if err != nil {
		return fmt.Errorf("send platform control reply: %w", err)
	}
	return nil
}
