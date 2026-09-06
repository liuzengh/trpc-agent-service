package gateway

import (
	"context"
	"fmt"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecommcp"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// CallbackGateway verifies provider callbacks and persists every decoded
// message before returning the provider acknowledgement.
type CallbackGateway struct {
	repository controlplane.Repository
	registry   *channels.Registry
	intake     *Intake
	approvals  ApprovalDecisionHandler
}

type CallbackGatewayOption func(*CallbackGateway)

func WithApprovalDecisionHandler(handler ApprovalDecisionHandler) CallbackGatewayOption {
	return func(gateway *CallbackGateway) { gateway.approvals = handler }
}

func NewCallbackGateway(
	repository controlplane.Repository,
	registry *channels.Registry,
	intake *Intake,
	opts ...CallbackGatewayOption,
) (*CallbackGateway, error) {
	if repository == nil || registry == nil || intake == nil {
		return nil, fmt.Errorf("callback Gateway dependencies are required")
	}
	gateway := &CallbackGateway{repository: repository, registry: registry, intake: intake}
	for _, opt := range opts {
		if opt != nil {
			opt(gateway)
		}
	}
	return gateway, nil
}

func (g *CallbackGateway) Handle(
	ctx context.Context,
	channelType string,
	callbackKey string,
	request *http.Request,
) (channels.CallbackResult, error) {
	binding, err := g.repository.GetChannelBindingByCallbackKey(ctx, callbackKey)
	if err != nil {
		return channels.CallbackResult{}, err
	}
	if binding.Status != controlplane.StatusActive || binding.ChannelType != channelType {
		return channels.CallbackResult{}, fmt.Errorf("callback binding is unavailable")
	}
	ctx, span := otel.Tracer("trpc-agent-service/channel").Start(ctx, "channel.callback")
	span.SetAttributes(attribute.String("tenant.id", binding.TenantID), attribute.String("channel.type", channelType),
		attribute.String("channel.binding.id", binding.ID))
	defer span.End()
	adapter, err := g.registry.Get(channelType)
	if err != nil {
		return channels.CallbackResult{}, err
	}
	callbackAdapter, ok := adapter.(channels.CallbackAdapter)
	if !ok {
		return channels.CallbackResult{}, fmt.Errorf("channel %q does not accept callbacks", channelType)
	}
	result, err := callbackAdapter.Callback(ctx, binding, request)
	if err != nil {
		return channels.CallbackResult{}, err
	}
	for _, message := range result.Messages {
		if err := g.acceptVerifiedMessage(ctx, binding, message); err != nil {
			return channels.CallbackResult{}, err
		}
	}
	if result.StatusCode == 0 {
		result.StatusCode = http.StatusOK
	}
	return result, nil
}

// AcceptPolled is an internal ingress, not a new HTTP endpoint. Recheck the
// binding version after network I/O, so disabled/reconfigured subscriptions do
// not enqueue stale messages. The adapter has already enforced group/sender scope.
func (g *CallbackGateway) AcceptPolled(ctx context.Context, binding controlplane.ChannelBinding, message channels.InboundEnvelope) error {
	current, err := g.repository.GetChannelBinding(ctx, binding.TenantID, binding.ID)
	if err != nil || current.Status != controlplane.StatusActive || current.ChannelType != wecommcp.ChannelType || current.Version != binding.Version || current.AppID != binding.AppID || current.CallbackKey != binding.CallbackKey {
		return fmt.Errorf("polled channel binding changed or unavailable")
	}
	return g.acceptVerifiedMessage(ctx, current, message)
}

func (g *CallbackGateway) acceptVerifiedMessage(ctx context.Context, binding controlplane.ChannelBinding, message channels.InboundEnvelope) error {
	if strings.TrimSpace(message.Text) == "" {
		return nil
	}
	userID, sessionID := channels.RuntimeIdentity(
		binding.ID,
		message.ExternalUserID,
		message.ExternalChatID,
		message.ExternalThreadID,
		message.ChatType,
	)
	// Count all verified callback messages once, including approval/control
	// feedback which deliberately bypasses the normal Agent intake path.
	if g.intake.quota != nil {
		if err := g.intake.quota.AllowInbound(ctx, binding.TenantID, userID); err != nil {
			if g.intake.audit != nil {
				_ = g.intake.audit.Record(ctx, audit.Event{TenantID: binding.TenantID, Channel: binding.ChannelType, ChannelBindingID: binding.ID,
					UserID: userID, SessionID: sessionID, MessageID: message.ExternalMessageID, TraceID: audit.TraceID(ctx), Decision: "inbound_rate_rejected", ErrorType: "tenant_quota"})
			}
			return fmt.Errorf("callback rate limit: %w", err)
		}
	}
	if feedback := unsupportedMessageReply(message); feedback != "" {
		if _, err := g.intake.Accept(ctx, IntakeRequest{
			rateChecked: true,
			BindingKey:  binding.CallbackKey, ExternalMessageID: message.ExternalMessageID,
			UserID: userID, SessionID: sessionID, ChatType: message.ChatType,
			Text: message.Text, ReplyTarget: message.ReplyTarget, DirectReply: feedback,
		}); err != nil {
			return fmt.Errorf("persist unsupported message feedback: %w", err)
		}
		return nil
	}
	if g.approvals != nil {
		scope, err := g.intake.resolveScope(ctx, binding.CallbackKey, userID, sessionID)
		if err != nil {
			return err
		}
		handled, err := g.approvals.HandleApprovalDecision(ctx, ApprovalDecisionInput{
			Scope:             scope,
			TenantID:          binding.TenantID,
			ChannelType:       binding.ChannelType,
			ChannelBindingID:  binding.ID,
			ExternalMessageID: message.ExternalMessageID,
			UserID:            userID,
			SessionID:         sessionID,
			ChatType:          message.ChatType,
			Text:              message.Text,
			ReplyTarget:       message.ReplyTarget,
		})
		if err != nil {
			return fmt.Errorf("handle approval decision: %w", err)
		}
		if handled {
			return nil
		}
	}
	if _, err := g.intake.Accept(ctx, IntakeRequest{
		rateChecked:       true,
		BindingKey:        binding.CallbackKey,
		ExternalMessageID: message.ExternalMessageID,
		UserID:            userID,
		SessionID:         sessionID,
		ChatType:          message.ChatType,
		Text:              message.Text,
		ReplyTarget:       message.ReplyTarget,
	}); err != nil {
		return fmt.Errorf("persist callback message: %w", err)
	}
	return nil
}

func unsupportedMessageReply(message channels.InboundEnvelope) string {
	if message.Edited {
		return "平台提示：当前不支持通过编辑消息修改已提交的请求或审批。本次编辑未触发 Agent 或工具，原任务状态不变；如需新请求，请另发一条文本消息。"
	}
	if message.MessageType != "" && message.MessageType != "text" {
		return "平台提示：当前通道只处理文本，尚未下载或读取本条图片、文件或其他媒体，也未据此执行工具。请把需要处理的内容作为新的文本消息发送；附件说明不会被当作审批命令。"
	}
	return ""
}
