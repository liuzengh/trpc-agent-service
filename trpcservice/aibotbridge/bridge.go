// Package aibotbridge owns the lifecycle of Enterprise WeChat intelligent
// robot WebSocket clients and translates callback frames into worker tasks.
package aibotbridge

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"log"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	aibot "github.com/go-sphere/wecom-aibot-go-sdk/aibot"
	"github.com/gorilla/websocket"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/cyl6/trpc-agent-service/trpcservice/channels"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
	"github.com/cyl6/trpc-agent-service/trpcservice/queue"
	"github.com/cyl6/trpc-agent-service/trpcservice/tenant"
	"github.com/cyl6/trpc-agent-service/trpcservice/worker"
)

const channelType = "wecom-aibot"

type callbackBody struct {
	MsgID      string       `json:"msgid"`
	AIBotID    string       `json:"aibotid"`
	ChatID     string       `json:"chatid"`
	ChatType   string       `json:"chattype"`
	CreateTime int64        `json:"create_time"`
	MsgType    string       `json:"msgtype"`
	From       callbackFrom `json:"from"`
	Text       textContent  `json:"text"`
	Voice      textContent  `json:"voice"`
	Image      mediaContent `json:"image"`
	File       mediaContent `json:"file"`
	Video      mediaContent `json:"video"`
	Mixed      mixedContent `json:"mixed"`
}

type callbackFrom struct {
	UserID string `json:"userid"`
}

type textContent struct {
	Content string `json:"content"`
}

type mediaContent struct {
	URL string `json:"url"`
}

type mixedContent struct {
	Items []mixedItem `json:"msg_item"`
}

type mixedItem struct {
	MsgType string       `json:"msgtype"`
	Text    textContent  `json:"text"`
	Image   mediaContent `json:"image"`
}

// Manager reconciles authenticated transports before Registry publication.
// Reconcile must not read Registry: it runs under the Registry publication lock.
type managedClient interface {
	channels.WeComAIBotClient
	Close()
}
type connection struct {
	client      managedClient
	fingerprint [32]byte
}
type Manager struct {
	mu      sync.Mutex
	clients map[string]connection
	adapter *channels.WeComAIBot
	closed  bool
	ctx     context.Context
	connect func(context.Context, config.ChannelConfig, string, string, string) (managedClient, error)
}

func Start(ctx context.Context, tenants *tenant.Registry, dispatcher queue.Dispatcher, adapter *channels.WeComAIBot) (*Manager, error) {
	if tenants == nil || dispatcher == nil || adapter == nil {
		return nil, errors.New("wecom intelligent bot bridge dependencies are unavailable")
	}
	manager := &Manager{adapter: adapter, ctx: ctx, clients: make(map[string]connection)}
	manager.connect = func(connectCtx context.Context, binding config.ChannelConfig, botID, secret, tenantID string) (managedClient, error) {
		if err := connectCtx.Err(); err != nil {
			return nil, errors.New("wecom intelligent bot connection preparation cancelled")
		}
		client := newLiveClient(binding, botID, secret)
		authenticated := make(chan struct{}, 1)
		client.sdk.OnAuthenticated(func() {
			client.authenticated.Store(true)
			select {
			case authenticated <- struct{}{}:
			default:
			}
		})
		client.sdk.OnDisconnected(func(string) { client.authenticated.Store(false) })
		client.sdk.OnReconnecting(func(int) { client.authenticated.Store(false) })
		client.sdk.OnError(func(error) { client.authenticated.Store(false) })
		client.sdk.OnMessage(func(frame *aibot.WsFrame) {
			handleFrame(ctx, frame, binding.BindingID, botID, tenants, dispatcher, tenantID)
		})
		client.sdk.Connect()
		select {
		case <-authenticated:
			return client, nil
		case <-connectCtx.Done():
			client.Close()
			return nil, errors.New("wecom intelligent bot authentication did not complete")
		}
	}
	// Installing the hook under the Registry lock also reconciles the latest
	// snapshot, closing the race with a control-plane refresh during startup.
	if err := tenants.SetBeforePublish(manager.Reconcile); err != nil {
		manager.Close()
		return nil, err
	}
	return manager, nil
}

// Reconcile resolves credentials each time, detecting rotation even if an env
// reference is unchanged. New clients authenticate before any adapter swap;
// a failed batch closes staged clients and retains the previous mappings.
func (m *Manager) Reconcile(tenants []config.TenantConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("wecom intelligent bot bridge is closed")
	}
	type desiredConnection struct {
		binding                 config.ChannelConfig
		botID, secret, tenantID string
		fingerprint             [32]byte
	}
	desired := make(map[string]desiredConnection)
	seen := make(map[string]bool)
	for _, tenantConfig := range tenants {
		if !tenantConfig.Enabled {
			continue
		}
		for _, binding := range tenantConfig.Channels {
			if !binding.Enabled || binding.Type != channelType {
				continue
			}
			botID, err := config.Secret(binding.BotIDEnv)
			if err != nil {
				return errors.New("wecom intelligent bot id is unavailable")
			}
			secret, err := config.Secret(binding.BotSecretEnv)
			if err != nil {
				return errors.New("wecom intelligent bot secret is unavailable")
			}
			if seen[botID] {
				return errors.New("wecom intelligent bot has duplicate bindings")
			}
			seen[botID] = true
			raw, _ := json.Marshal([]string{tenantConfig.TenantID, binding.BindingID, botID, secret, binding.WebSocketURL})
			desired[binding.BindingID] = desiredConnection{binding, botID, secret, tenantConfig.TenantID, sha256.Sum256(raw)}
		}
	}
	// Bound the whole publication, not each binding separately, so a tenant
	// with many bindings cannot hold the Registry lock for N authentication timeouts.
	connectCtx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
	defer cancel()
	staged := make(map[string]connection)
	ids := make([]string, 0, len(desired))
	for id := range desired {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		target := desired[id]
		if old, ok := m.clients[id]; ok && old.fingerprint == target.fingerprint {
			continue
		}
		client, err := m.connect(connectCtx, target.binding, target.botID, target.secret, target.tenantID)
		if err != nil {
			for _, pending := range staged {
				pending.client.Close()
			}
			return errors.New("wecom intelligent bot connection preparation failed")
		}
		staged[id] = connection{client, target.fingerprint}
	}
	for id, old := range m.clients {
		_, retained := desired[id]
		_, replaced := staged[id]
		if !retained || replaced {
			m.adapter.RegisterClient(id, nil)
			old.client.Close()
			delete(m.clients, id)
		}
	}
	for id, pending := range staged {
		m.clients[id] = pending
		m.adapter.RegisterClient(id, pending.client)
	}
	return nil
}

func newLiveClient(binding config.ChannelConfig, botID, secret string) *liveClient {
	options := aibot.WSClientOptions{
		BotID: botID, Secret: secret,
		WsDialer:             &websocket.Dialer{HandshakeTimeout: 5 * time.Second},
		MaxReconnectAttempts: -1,
		Logger:               quietLogger{},
	}
	if binding.WebSocketURL != "" {
		options.WSURL = binding.WebSocketURL
	}
	return &liveClient{sdk: aibot.NewWSClient(options)}
}

type liveClient struct {
	sdk           *aibot.WSClient
	authenticated atomic.Bool
}

func (c *liveClient) Close() {
	c.authenticated.Store(false)
	c.sdk.Disconnect()
}

func (c *liveClient) IsConnected() bool {
	return c != nil && c.authenticated.Load() && c.sdk.IsConnected()
}

func (c *liveClient) SendMarkdown(chatID, content string) (string, error) {
	frame, err := c.sdk.SendMarkdown(chatID, content)
	if frame == nil {
		return "", err
	}
	return frame.Headers.ReqID, err
}

func (m *Manager) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	for id, client := range m.clients {
		m.adapter.RegisterClient(id, nil)
		client.client.Close()
	}
	return nil
}

func handleFrame(
	ctx context.Context,
	frame *aibot.WsFrame,
	bindingID, expectedBotID string,
	tenants *tenant.Registry,
	dispatcher queue.Dispatcher,
	expectedTenant ...string,
) {
	if frame == nil {
		return
	}
	var body callbackBody
	if err := json.Unmarshal(frame.Body, &body); err != nil {
		log.Printf("wecom intelligent bot binding %s ignored a malformed callback", bindingID)
		return
	}
	message, err := normalize(body, bindingID, expectedBotID)
	if err != nil {
		log.Printf("wecom intelligent bot binding %s ignored an unsupported callback", bindingID)
		return
	}
	binding, err := tenants.ResolveBinding(channelType, bindingID)
	if err != nil {
		log.Printf("wecom intelligent bot binding %s is no longer active", bindingID)
		return
	}
	if len(expectedTenant) > 0 && binding.Tenant.TenantID != expectedTenant[0] {
		return
	}
	currentBotID, secretErr := config.Secret(binding.Channel.BotIDEnv)
	if secretErr != nil || currentBotID != expectedBotID {
		return
	}
	message.TenantID = binding.Tenant.TenantID
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if err := dispatcher.Submit(ctx, worker.Task{
		Tenant: binding.Tenant, Binding: binding.Channel, Message: message,
		Deliver: true, TraceCarrier: map[string]string(carrier),
	}); err != nil {
		log.Printf("wecom intelligent bot binding %s could not enqueue a callback", bindingID)
	}
}

func normalize(body callbackBody, bindingID, expectedBotID string) (domain.InboundMessage, error) {
	if body.MsgID == "" || body.AIBotID != expectedBotID || body.From.UserID == "" {
		return domain.InboundMessage{}, errors.New("invalid callback identity")
	}
	scope := domain.ScopeDirect
	target := body.From.UserID
	if body.ChatType == "group" {
		scope = domain.ScopeGroup
		target = body.ChatID
	} else if body.ChatType != "single" {
		return domain.InboundMessage{}, errors.New("unsupported chat type")
	}
	if target == "" {
		return domain.InboundMessage{}, errors.New("missing reply target")
	}
	text, attachments, err := callbackContent(body)
	if err != nil {
		return domain.InboundMessage{}, err
	}
	receivedAt := time.Now().UTC()
	if body.CreateTime > 0 {
		receivedAt = time.Unix(body.CreateTime, 0).UTC()
	}
	return domain.InboundMessage{
		BindingID: bindingID, Channel: channelType,
		ExternalMessageID: body.MsgID, ExternalUserID: body.From.UserID,
		ConversationID: target, Scope: scope, Text: text,
		Attachments: attachments, ReceivedAt: receivedAt, ReplyTarget: target,
	}, nil
}

func callbackContent(body callbackBody) (string, []domain.Attachment, error) {
	switch body.MsgType {
	case "text":
		if strings.TrimSpace(body.Text.Content) == "" {
			return "", nil, errors.New("empty text")
		}
		return body.Text.Content, nil, nil
	case "voice":
		if strings.TrimSpace(body.Voice.Content) == "" {
			return "", nil, errors.New("empty voice transcript")
		}
		return body.Voice.Content, nil, nil
	case "image":
		return "", []domain.Attachment{{Type: "image", URL: body.Image.URL}}, nil
	case "file":
		return "", []domain.Attachment{{Type: "file", URL: body.File.URL}}, nil
	case "video":
		return "", []domain.Attachment{{Type: "video", URL: body.Video.URL}}, nil
	case "mixed":
		var textParts []string
		attachments := make([]domain.Attachment, 0)
		for _, item := range body.Mixed.Items {
			switch item.MsgType {
			case "text":
				if strings.TrimSpace(item.Text.Content) != "" {
					textParts = append(textParts, item.Text.Content)
				}
			case "image":
				attachments = append(attachments, domain.Attachment{Type: "image", URL: item.Image.URL})
			}
		}
		if len(textParts) == 0 && len(attachments) == 0 {
			return "", nil, errors.New("empty mixed message")
		}
		return strings.Join(textParts, "\n"), attachments, nil
	default:
		return "", nil, errors.New("unsupported message type")
	}
}

// quietLogger intentionally discards SDK-formatted payloads and provider
// error strings. The bridge emits only stable connection categories.
type quietLogger struct{}

func (quietLogger) Debug(string, ...interface{}) {}
func (quietLogger) Info(string, ...interface{})  {}
func (quietLogger) Warn(string, ...interface{})  {}
func (quietLogger) Error(string, ...interface{}) {}
