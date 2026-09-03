package gateway

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

// CallbackGateway verifies provider callbacks and persists every decoded
// message before returning the provider acknowledgement.
type CallbackGateway struct {
	repository controlplane.Repository
	registry   *channels.Registry
	intake     *Intake
}

func NewCallbackGateway(
	repository controlplane.Repository,
	registry *channels.Registry,
	intake *Intake,
) (*CallbackGateway, error) {
	if repository == nil || registry == nil || intake == nil {
		return nil, fmt.Errorf("callback Gateway dependencies are required")
	}
	return &CallbackGateway{repository: repository, registry: registry, intake: intake}, nil
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
		if strings.TrimSpace(message.Text) == "" {
			continue
		}
		userID, sessionID := channels.RuntimeIdentity(
			binding.ID,
			message.ExternalUserID,
			message.ExternalChatID,
			message.ExternalThreadID,
			message.ChatType,
		)
		if _, err := g.intake.Accept(ctx, IntakeRequest{
			BindingKey:        binding.CallbackKey,
			ExternalMessageID: message.ExternalMessageID,
			UserID:            userID,
			SessionID:         sessionID,
			ChatType:          message.ChatType,
			Text:              message.Text,
			ReplyTarget:       message.ReplyTarget,
		}); err != nil {
			return channels.CallbackResult{}, fmt.Errorf("persist callback message: %w", err)
		}
	}
	if result.StatusCode == 0 {
		result.StatusCode = http.StatusOK
	}
	return result, nil
}
