// Package wecom implements the Enterprise WeChat intelligent bot long connection.
package wecom

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/channels"
	"github.com/DocJlm/trpc-agent-service/trpcservice/secrets"
	"github.com/go-sphere/wecom-aibot-go-sdk/aibot"
	"github.com/google/uuid"
)

const ChannelName = "wecom"

type Adapter struct {
	tenantID      string
	bindingID     string
	credentialRef string
	provider      secrets.Provider
	handler       channels.InboundHandler

	mu     sync.RWMutex
	health channels.ChannelHealth
	client *aibot.WSClient
	frames map[string]*aibot.WsFrame
}

func New(tenantID, bindingID, credentialRef string, provider secrets.Provider, handler channels.InboundHandler) *Adapter {
	return &Adapter{
		tenantID: tenantID, bindingID: bindingID, credentialRef: credentialRef,
		provider: provider, handler: handler, frames: make(map[string]*aibot.WsFrame),
		health: channels.ChannelHealth{State: "created", LastChanged: time.Now()},
	}
}

func (a *Adapter) ID() string { return a.bindingID }

func (a *Adapter) Run(ctx context.Context) error {
	if a.provider == nil || a.handler == nil {
		return errors.New("wecom adapter requires secret provider and inbound handler")
	}
	values, err := a.provider.Resolve(ctx, a.credentialRef)
	if err != nil {
		a.setHealth(false, "credential_error", err)
		return fmt.Errorf("resolve wecom credentials: %w", err)
	}
	if len(values) != 2 {
		return fmt.Errorf("wecom credential must contain BotID and Secret, got %d values", len(values))
	}
	botID, botSecret := values[0], values[1]
	switch order := strings.ToLower(strings.TrimSpace(os.Getenv("WECOM_CREDENTIAL_ORDER"))); order {
	case "", "botid_secret":
	case "secret_botid":
		botID, botSecret = values[1], values[0]
	default:
		return fmt.Errorf("unsupported WECOM_CREDENTIAL_ORDER %q", order)
	}
	client := aibot.NewWSClient(aibot.WSClientOptions{
		BotID: botID, Secret: botSecret, MaxReconnectAttempts: -1,
		ReconnectInterval: 1000, HeartbeatInterval: 30000,
	})
	a.mu.Lock()
	a.client = client
	a.mu.Unlock()
	client.OnAuthenticated(func() { a.setHealth(true, "ready", nil) })
	client.OnReconnecting(func(attempt int) {
		a.setHealth(false, fmt.Sprintf("reconnecting_%d", attempt), nil)
	})
	client.OnDisconnected(func(reason string) {
		a.setHealth(false, "disconnected", errors.New(reason))
	})
	client.OnError(func(err error) { a.setHealth(false, "error", err) })
	client.OnMessageText(func(frame *aibot.WsFrame) {
		message, normalizeErr := NormalizeText(a.tenantID, a.bindingID, frame)
		if normalizeErr != nil {
			a.setHealth(client.IsConnected(), "message_error", normalizeErr)
			return
		}
		a.mu.Lock()
		a.frames[message.ReplyToken] = frame
		a.mu.Unlock()
		if handleErr := a.handler(ctx, message); handleErr != nil {
			a.setHealth(client.IsConnected(), "handler_error", handleErr)
		}
	})
	a.setHealth(false, "connecting", nil)
	client.Connect()
	<-ctx.Done()
	client.Disconnect()
	a.setHealth(false, "stopped", nil)
	return nil
}

func (a *Adapter) Send(_ context.Context, out channels.OutboundEnvelope) error {
	if out.BindingID != a.bindingID || out.Channel != ChannelName {
		return errors.New("outbound envelope does not belong to this wecom binding")
	}
	a.mu.RLock()
	client := a.client
	frame := a.frames[out.ReplyToken]
	a.mu.RUnlock()
	if client == nil {
		return errors.New("wecom client is not started")
	}
	if frame != nil {
		streamID := out.ID
		if streamID == "" {
			streamID = uuid.NewString()
		}
		if _, err := client.ReplyStream(frame, streamID, out.Content, out.Final, nil, nil); err != nil {
			return fmt.Errorf("wecom stream reply: %w", err)
		}
		if out.Final {
			a.mu.Lock()
			delete(a.frames, out.ReplyToken)
			a.mu.Unlock()
		}
		return nil
	}
	if out.ExternalConversationID == "" {
		return errors.New("wecom reply frame expired and conversation id is empty")
	}
	if _, err := client.SendMarkdown(out.ExternalConversationID, out.Content); err != nil {
		return fmt.Errorf("wecom proactive markdown: %w", err)
	}
	return nil
}

func (a *Adapter) Health() channels.ChannelHealth {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.health
}

func (a *Adapter) setHealth(ready bool, state string, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.health.Ready = ready
	a.health.State = state
	a.health.LastChanged = time.Now()
	a.health.LastError = ""
	if err != nil {
		a.health.LastError = err.Error()
	}
}

func NormalizeText(tenantID, bindingID string, frame *aibot.WsFrame) (channels.InboundEnvelope, error) {
	if frame == nil {
		return channels.InboundEnvelope{}, errors.New("nil wecom frame")
	}
	var message aibot.TextMessage
	if err := aibot.ParseMessageBody(frame, &message); err != nil {
		return channels.InboundEnvelope{}, fmt.Errorf("decode wecom text: %w", err)
	}
	conversationType := channels.ConversationP2P
	if message.ChatType == "group" {
		conversationType = channels.ConversationGroup
	}
	conversationID := message.ChatID
	if conversationID == "" {
		conversationID = message.From.UserID
	}
	receivedAt := time.Now()
	if message.CreateTime > 0 {
		receivedAt = time.Unix(message.CreateTime, 0)
	}
	envelope := channels.InboundEnvelope{
		TenantID: tenantID, BindingID: bindingID, Channel: ChannelName,
		ExternalMessageID: message.MsgID, ExternalUserID: message.From.UserID,
		ExternalConversationID: conversationID, ConversationType: conversationType,
		Content: message.Text.Content, ReceivedAt: receivedAt,
		TraceID: uuid.NewString(), ReplyToken: frame.Headers.ReqID,
		MentionedBot: conversationType == channels.ConversationGroup,
	}
	if err := envelope.Validate(); err != nil {
		return channels.InboundEnvelope{}, err
	}
	return envelope, nil
}
