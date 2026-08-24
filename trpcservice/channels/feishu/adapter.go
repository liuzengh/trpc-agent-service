// Package feishu implements the Feishu event long connection and message reply API.
package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/channels"
	"github.com/DocJlm/trpc-agent-service/trpcservice/secrets"
	"github.com/google/uuid"
	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"
)

const ChannelName = "feishu"

type Adapter struct {
	tenantID      string
	bindingID     string
	credentialRef string
	provider      secrets.Provider
	handler       channels.InboundHandler

	mu        sync.RWMutex
	health    channels.ChannelHealth
	api       *lark.Client
	botOpenID string
}

func New(tenantID, bindingID, credentialRef string, provider secrets.Provider, handler channels.InboundHandler) *Adapter {
	return &Adapter{
		tenantID: tenantID, bindingID: bindingID, credentialRef: credentialRef,
		provider: provider, handler: handler,
		health: channels.ChannelHealth{State: "created", LastChanged: time.Now()},
	}
}

func (a *Adapter) ID() string { return a.bindingID }

func (a *Adapter) Run(ctx context.Context) error {
	if a.provider == nil || a.handler == nil {
		return errors.New("feishu adapter requires secret provider and inbound handler")
	}
	values, err := a.provider.Resolve(ctx, a.credentialRef)
	if err != nil {
		a.setHealth(false, "credential_error", err)
		return fmt.Errorf("resolve feishu credentials: %w", err)
	}
	if len(values) != 2 {
		return fmt.Errorf("feishu credential must contain AppID and App Secret, got %d values", len(values))
	}
	api := lark.NewClient(values[0], values[1])
	botOpenID, err := resolveBotOpenID(ctx, api)
	if err != nil {
		a.setHealth(false, "identity_error", err)
		return fmt.Errorf("resolve feishu bot identity: %w", err)
	}
	eventHandler := dispatcher.NewEventDispatcher("", "").OnP2MessageReceiveV1(
		func(handlerCtx context.Context, event *larkim.P2MessageReceiveV1) error {
			message, mentionAll, normalizeErr := NormalizeMessage(a.tenantID, a.bindingID, botOpenID, event)
			if normalizeErr != nil {
				return normalizeErr
			}
			if !channels.ShouldHandleGroup(message.ConversationType, message.MentionedBot, mentionAll) {
				return nil
			}
			return a.handler(handlerCtx, message)
		},
	)
	a.mu.Lock()
	a.api = api
	a.botOpenID = botOpenID
	a.mu.Unlock()
	client := larkws.NewClient(
		values[0], values[1], larkws.WithEventHandler(eventHandler),
		// Info logs include the ephemeral WSS bootstrap URL. Keep production
		// output at error level so access tickets never enter application logs.
		larkws.WithLogLevel(larkcore.LogLevelError),
	)
	a.setHealth(false, "connecting", nil)
	started := make(chan error, 1)
	go func() { started <- client.Start(ctx) }()
	readyTimer := time.NewTimer(time.Second)
	defer readyTimer.Stop()
	select {
	case err := <-started:
		a.setHealth(false, "connection_error", err)
		return fmt.Errorf("start feishu long connection: %w", err)
	case <-readyTimer.C:
		// v3.7.2 has no public OnReady hook. Start only blocks after the WSS
		// bootstrap and dial succeed, so surviving this grace period is ready.
		a.setHealth(true, "ready", nil)
	case <-ctx.Done():
		a.setHealth(false, "stopped", nil)
		return nil
	}
	select {
	case err := <-started:
		a.setHealth(false, "connection_error", err)
		return fmt.Errorf("feishu long connection stopped: %w", err)
	case <-ctx.Done():
		a.setHealth(false, "stopped", nil)
		return nil
	}
}

func (a *Adapter) Send(ctx context.Context, out channels.OutboundEnvelope) error {
	if out.BindingID != a.bindingID || out.Channel != ChannelName {
		return errors.New("outbound envelope does not belong to this feishu binding")
	}
	if out.ReplyToMessageID == "" {
		return errors.New("feishu reply_to_message_id is required")
	}
	a.mu.RLock()
	api := a.api
	a.mu.RUnlock()
	if api == nil {
		return errors.New("feishu client is not started")
	}
	content, err := json.Marshal(map[string]string{"text": feishuPlainText(out.Content)})
	if err != nil {
		return fmt.Errorf("encode feishu text: %w", err)
	}
	dedupeID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(out.ID)).String()
	req := larkim.NewReplyMessageReqBuilder().
		MessageId(out.ReplyToMessageID).
		Body(larkim.NewReplyMessageReqBodyBuilder().
			Content(string(content)).MsgType("text").Uuid(dedupeID).Build()).
		Build()
	resp, err := api.Im.Message.Reply(ctx, req)
	if err != nil {
		return fmt.Errorf("feishu reply request: %w", err)
	}
	if !resp.Success() {
		return fmt.Errorf("feishu reply rejected: code=%d msg=%s", resp.Code, resp.Msg)
	}
	return nil
}

// feishuPlainText removes formatting markers that Feishu's text message type
// displays literally. The adapter intentionally keeps using text replies so
// quoted replies retain their native chat appearance; richer Markdown output
// would require a different message type such as an interactive card.
func feishuPlainText(content string) string {
	for {
		start := strings.Index(content, "**")
		if start < 0 {
			return content
		}
		remainder := content[start+2:]
		end := strings.Index(remainder, "**")
		if end < 0 {
			return content
		}
		content = content[:start] + remainder[:end] + remainder[end+2:]
	}
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

func NormalizeMessage(tenantID, bindingID, botOpenID string, event *larkim.P2MessageReceiveV1) (channels.InboundEnvelope, bool, error) {
	if event == nil || event.Event == nil || event.Event.Message == nil || event.Event.Sender == nil {
		return channels.InboundEnvelope{}, false, errors.New("incomplete feishu message event")
	}
	message := event.Event.Message
	if value(message.MessageType) != "text" {
		return channels.InboundEnvelope{}, false, fmt.Errorf("unsupported feishu message type %q", value(message.MessageType))
	}
	var body struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal([]byte(value(message.Content)), &body); err != nil {
		return channels.InboundEnvelope{}, false, fmt.Errorf("decode feishu text: %w", err)
	}
	mentionAll := false
	mentionedBot := false
	for _, mention := range message.Mentions {
		if mention == nil {
			continue
		}
		key, name := value(mention.Key), strings.ToLower(value(mention.Name))
		if key == "@_all" || name == "all" || name == "所有人" {
			mentionAll = true
		}
		mentionOpenID := ""
		if mention.Id != nil {
			mentionOpenID = value(mention.Id.OpenId)
		}
		if botOpenID != "" && mentionOpenID == botOpenID {
			mentionedBot = true
			body.Text = strings.ReplaceAll(body.Text, key, "")
		}
	}
	conversationType := channels.ConversationP2P
	if chatType := value(message.ChatType); chatType == "group" || chatType == "topic_group" {
		conversationType = channels.ConversationGroup
	}
	userID := ""
	if event.Event.Sender.SenderId != nil {
		userID = firstNonEmpty(
			value(event.Event.Sender.SenderId.OpenId),
			value(event.Event.Sender.SenderId.UserId),
			value(event.Event.Sender.SenderId.UnionId),
		)
	}
	eventID := value(message.MessageId)
	traceID := uuid.NewString()
	if event.EventV2Base != nil && event.EventV2Base.Header != nil {
		if event.EventV2Base.Header.EventID != "" {
			eventID = event.EventV2Base.Header.EventID
		}
	}
	receivedAt := time.Now()
	if createTime := value(message.CreateTime); createTime != "" {
		if millis, err := strconv.ParseInt(createTime, 10, 64); err == nil {
			receivedAt = time.UnixMilli(millis)
		}
	}
	envelope := channels.InboundEnvelope{
		TenantID: tenantID, BindingID: bindingID, Channel: ChannelName,
		ExternalMessageID: eventID, ExternalUserID: userID,
		ExternalConversationID: value(message.ChatId), ConversationType: conversationType,
		Content: strings.TrimSpace(body.Text), ReceivedAt: receivedAt, TraceID: traceID,
		ReplyToken: value(message.MessageId), MentionedBot: mentionedBot,
	}
	if err := envelope.Validate(); err != nil {
		return channels.InboundEnvelope{}, false, err
	}
	return envelope, mentionAll, nil
}

func resolveBotOpenID(ctx context.Context, api *lark.Client) (string, error) {
	if api == nil {
		return "", errors.New("feishu client is nil")
	}
	resp, err := api.Get(ctx, "/open-apis/bot/v3/info", nil, larkcore.AccessTokenTypeTenant)
	if err != nil {
		return "", err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("bot info request returned HTTP %d", resp.StatusCode)
	}
	var body struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
		Bot  struct {
			OpenID string `json:"open_id"`
		} `json:"bot"`
	}
	if err := json.Unmarshal(resp.RawBody, &body); err != nil {
		return "", fmt.Errorf("decode bot info: %w", err)
	}
	if body.Code != 0 {
		return "", fmt.Errorf("bot info rejected: code=%d msg=%s", body.Code, body.Msg)
	}
	if body.Bot.OpenID == "" {
		return "", errors.New("bot info did not return open_id")
	}
	return body.Bot.OpenID, nil
}

func value(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func firstNonEmpty(values ...string) string {
	for _, item := range values {
		if item != "" {
			return item
		}
	}
	return ""
}
