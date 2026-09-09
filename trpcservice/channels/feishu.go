package channels

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"

	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

const defaultFeishuRequestTimeout = 30 * time.Second

type feishuWebSocketClient interface {
	Start(context.Context) error
	Close()
}

type feishuMessageClient interface {
	Reply(context.Context, *larkim.ReplyMessageReq, ...larkcore.RequestOptionFunc) (*larkim.ReplyMessageResp, error)
}

// FeishuAdapter receives im.message.receive_v1 events over Feishu's official
// long connection and replies to the originating message through OpenAPI.
type FeishuAdapter struct {
	bindingID string
	accountID string
	appID     string

	ws       feishuWebSocketClient
	messages feishuMessageClient

	mu      sync.RWMutex
	started bool
	ready   bool
	sink    IngressSink
	cancel  context.CancelFunc
}

func NewFeishuAdapter(bindingID, accountID, appID, appSecret string) (*FeishuAdapter, error) {
	if strings.TrimSpace(bindingID) == "" || strings.TrimSpace(accountID) == "" || strings.TrimSpace(appID) == "" || strings.TrimSpace(appSecret) == "" {
		return nil, errors.New("feishu binding, account, app ID and app secret are required")
	}

	adapter := &FeishuAdapter{bindingID: bindingID, accountID: accountID, appID: appID}
	eventHandler := dispatcher.NewEventDispatcher("", "").
		OnP2MessageReceiveV1(adapter.handleMessage)
	adapter.ws = larkws.NewClient(appID, appSecret,
		larkws.WithEventHandler(eventHandler),
		larkws.WithLogLevel(larkcore.LogLevelError),
		larkws.WithOnReady(func() { adapter.markFeishuReady(true) }),
		larkws.WithOnReconnecting(func() { adapter.markFeishuReady(false) }),
		larkws.WithOnReconnected(func() { adapter.markFeishuReady(true) }),
		larkws.WithOnDisconnected(func() { adapter.markFeishuReady(false) }),
		larkws.WithOnError(func(error) { adapter.markFeishuReady(false) }),
	)
	client := lark.NewClient(appID, appSecret,
		lark.WithLogLevel(larkcore.LogLevelError),
		lark.WithReqTimeout(defaultFeishuRequestTimeout),
	)
	adapter.messages = client.Im.V1.Message
	return adapter, nil
}

func (a *FeishuAdapter) Name() string { return a.bindingID }

func (a *FeishuAdapter) Ready(context.Context) error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if !a.started || !a.ready || a.ws == nil || a.messages == nil {
		return errors.New("feishu adapter is not ready")
	}
	return nil
}

func (a *FeishuAdapter) Start(ctx context.Context, sink IngressSink) error {
	if sink == nil {
		return errors.New("feishu ingress sink is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runCtx, cancel := context.WithCancel(ctx)
	a.mu.Lock()
	if a.started {
		a.mu.Unlock()
		cancel()
		return errors.New("feishu adapter is already running")
	}
	if a.ws == nil || a.messages == nil {
		a.mu.Unlock()
		cancel()
		return errors.New("feishu adapter client is unavailable")
	}
	a.started, a.sink, a.cancel = true, sink, cancel
	ws := a.ws
	a.mu.Unlock()

	err := ws.Start(runCtx)
	stoppedByCancellation := runCtx.Err() != nil || errors.Is(err, context.Canceled)
	cancel()
	a.mu.Lock()
	a.started, a.ready, a.sink, a.cancel = false, false, nil, nil
	a.mu.Unlock()
	if stoppedByCancellation {
		return nil
	}
	if err != nil {
		return fmt.Errorf("feishu long connection stopped: %w", err)
	}
	return errors.New("feishu long connection stopped unexpectedly")
}

func (a *FeishuAdapter) handleMessage(ctx context.Context, event *larkim.P2MessageReceiveV1) error {
	ctx, span := telemetry.Start(ctx, "channel.ingress")
	defer span.End()
	if event == nil || event.Event == nil || event.Event.Sender == nil || event.Event.Message == nil {
		return nil
	}
	header := event.EventV2Base
	if header != nil && header.Header != nil && header.Header.AppID != "" && header.Header.AppID != a.appID && header.Header.AppID != a.accountID {
		return nil
	}
	sender := event.Event.Sender
	if sender.SenderType == nil || *sender.SenderType != "user" {
		return nil
	}
	actorID := feishuUserID(sender.SenderId)
	current := event.Event.Message
	if actorID == "" || current.MessageId == nil || strings.TrimSpace(*current.MessageId) == "" || current.ChatId == nil || strings.TrimSpace(*current.ChatId) == "" || current.MessageType == nil || *current.MessageType != "text" || current.Content == nil {
		return nil
	}
	text, err := feishuText(*current.Content, current.Mentions)
	if err != nil || strings.TrimSpace(text) == "" {
		return nil
	}
	conversationType := message.ConversationDirect
	if current.ChatType == nil {
		return nil
	}
	if *current.ChatType == "group" {
		conversationType = message.ConversationGroup
	} else if *current.ChatType != "p2p" {
		return nil
	}
	receivedAt := time.Now().UTC()
	if current.CreateTime != nil {
		if milliseconds, parseErr := strconv.ParseInt(*current.CreateTime, 10, 64); parseErr == nil {
			receivedAt = time.UnixMilli(milliseconds).UTC()
		}
	}
	platformRequestID := ""
	if header != nil && header.Header != nil {
		platformRequestID = header.Header.EventID
	}

	a.mu.RLock()
	sink := a.sink
	a.mu.RUnlock()
	if sink == nil {
		return errors.New("feishu ingress sink is unavailable")
	}
	_, err = sink.Accept(ctx, message.InboundMessage{
		Channel: "feishu", BindingID: a.bindingID, ExternalAccountID: a.accountID,
		PlatformMessageID: *current.MessageId, ActorUserID: actorID,
		ConversationID: *current.ChatId, ConversationType: conversationType,
		ReplyToMessageID: *current.MessageId, Text: text,
		PlatformRequestID: platformRequestID, ReceivedAt: receivedAt,
	})
	return err
}

func (a *FeishuAdapter) Send(ctx context.Context, outbound message.OutboundMessage) error {
	if strings.TrimSpace(outbound.Text) == "" || strings.TrimSpace(outbound.ReplyToMessageID) == "" {
		return errors.New("feishu outbound target is incomplete")
	}
	if a.messages == nil {
		return errors.New("feishu message client is unavailable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	content, err := json.Marshal(map[string]string{"text": outbound.Text})
	if err != nil {
		return err
	}
	body := larkim.NewReplyMessageReqBodyBuilder().
		Content(string(content)).
		MsgType("text").
		ReplyInThread(false)
	if outbound.RequestID != "" {
		body.Uuid(outbound.RequestID)
	}
	req := larkim.NewReplyMessageReqBuilder().
		MessageId(outbound.ReplyToMessageID).
		Body(body.Build()).
		Build()
	resp, err := a.messages.Reply(ctx, req)
	if err != nil {
		return fmt.Errorf("feishu reply failed: %w", err)
	}
	if resp == nil {
		return errors.New("feishu reply response is missing")
	}
	if !resp.Success() {
		return fmt.Errorf("feishu reply rejected with code %d: %s", resp.Code, resp.Msg)
	}
	return nil
}

func (a *FeishuAdapter) Close() error {
	a.mu.Lock()
	cancel := a.cancel
	ws := a.ws
	a.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if ws != nil {
		ws.Close()
	}
	return nil
}

func (a *FeishuAdapter) markFeishuReady(value bool) {
	a.mu.Lock()
	a.ready = value
	a.mu.Unlock()
}

func feishuUserID(value *larkim.UserId) string {
	if value == nil {
		return ""
	}
	for _, candidate := range []*string{value.OpenId, value.UserId, value.UnionId} {
		if candidate != nil && strings.TrimSpace(*candidate) != "" {
			return *candidate
		}
	}
	return ""
}

func feishuText(content string, mentions []*larkim.MentionEvent) (string, error) {
	var body struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal([]byte(content), &body); err != nil {
		return "", err
	}
	text := body.Text
	for _, mention := range mentions {
		if mention != nil && mention.Key != nil {
			text = strings.ReplaceAll(text, *mention.Key, "")
		}
	}
	return strings.TrimSpace(text), nil
}

var _ feishuWebSocketClient = (*larkws.Client)(nil)
