package channels

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"

	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/delivery"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
)

const wecomAIBotDefaultMessageBytes = 20480

// WeComAIBotClient is the small transport surface needed by the channel
// adapter. The long-connection bridge supplies one client per binding.
type WeComAIBotClient interface {
	IsConnected() bool
	SendMarkdown(chatID, content string) (providerRequestID string, err error)
}

// WeComAIBot adapts the intelligent robot API-mode long connection to the
// same deterministic delivery contract used by webhook-based channels.
// Inbound frames are handled by aibotbridge, while outbound messages use the
// authenticated WebSocket client registered for their binding.
type WeComAIBot struct {
	mu      sync.RWMutex
	clients map[string]WeComAIBotClient
}

func NewWeComAIBot() *WeComAIBot {
	return &WeComAIBot{clients: make(map[string]WeComAIBotClient)}
}

func (*WeComAIBot) Name() string { return "wecom-aibot" }

// RegisterClient publishes or removes the live transport for one binding.
func (w *WeComAIBot) RegisterClient(bindingID string, client WeComAIBotClient) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if client == nil {
		delete(w.clients, bindingID)
		return
	}
	w.clients[bindingID] = client
}

func (*WeComAIBot) Verify(*http.Request, []byte, config.ChannelConfig) error {
	return ErrUnsupportedEvent
}

func (*WeComAIBot) Parse([]byte, config.ChannelConfig) (ParsedWebhook, error) {
	return ParsedWebhook{}, ErrUnsupportedEvent
}

func (*WeComAIBot) Plan(binding config.ChannelConfig, msg domain.OutboundMessage) ([]delivery.Part, error) {
	limit := binding.MaxMessageLength
	if limit <= 0 {
		limit = wecomAIBotDefaultMessageBytes
	}
	pieces, err := chunksUTF8Bytes(msg.Text, limit)
	if err != nil {
		return nil, err
	}
	parts := make([]delivery.Part, 0, len(pieces))
	for index, piece := range pieces {
		part := cloneOutbound(msg)
		part.Text = piece
		parts = append(parts, delivery.Part{Message: part, Index: index, Total: len(pieces)})
	}
	return parts, nil
}

func (w *WeComAIBot) Deliver(ctx context.Context, binding config.ChannelConfig, request delivery.Request) delivery.Result {
	if err := ctx.Err(); err != nil {
		return delivery.Result{Outcome: delivery.RetryableNotSent, ErrorType: "request_cancelled", Err: err}
	}
	if strings.TrimSpace(request.Message.Target) == "" {
		return delivery.Result{
			Outcome: delivery.PermanentRejected, ErrorType: "request_invalid",
			Err: errors.New("wecom intelligent bot target is empty"),
		}
	}
	w.mu.RLock()
	client := w.clients[binding.BindingID]
	w.mu.RUnlock()
	if client == nil || !client.IsConnected() {
		// No frame was written, so the durable sender may retry safely after the
		// long connection authenticates or reconnects.
		return delivery.Result{
			Outcome: delivery.RetryableNotSent, ErrorType: "connection_unavailable",
			Err: errors.New("wecom intelligent bot connection is unavailable"),
		}
	}
	providerRequestID, err := client.SendMarkdown(request.Message.Target, request.Message.Text)
	if err != nil {
		// The SDK can return after a write while waiting for the server ACK. Do
		// not automatically repeat an operation whose acceptance is uncertain.
		return delivery.Result{
			Outcome: delivery.Unknown, ErrorType: "provider_ack_unknown",
			ProviderRequestID: providerRequestID,
			Err:               errors.New("wecom intelligent bot send acknowledgement failed"),
		}
	}
	return delivery.Result{Outcome: delivery.Confirmed, ProviderRequestID: providerRequestID}
}
