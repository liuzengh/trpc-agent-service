// Feishu (Lark) long-connection implementation of channels.Conn.
//
// The Lark SDK's ws.Client establishes the WSS channel and the event
// dispatcher routes im.message.receive_v1 events here; the raw JSON is
// forwarded verbatim so the adapter's existing Event normalization is reused.
// Replies go out over the Lark OpenAPI (im/message/create) — the long
// connection only receives events.
//
// NOTE: requires live app credentials to actually connect; the code is
// unit-tested for the message-building side only.
package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher/callback"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/channels"
)

// Conn implements channels.Conn over the Lark long connection.
type Conn struct {
	appID     string
	appSecret string

	events  chan []byte
	api     *lark.Client
	client  *larkws.Client
	handler *dispatcher.EventDispatcher

	mu        sync.RWMutex
	botOpenID string

	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// botInfoResp is the subset of GET /open-apis/bot/v3/info we need.
type botInfoResp struct {
	Code int `json:"code"`
	Bot  struct {
		OpenID string `json:"open_id"`
	} `json:"bot"`
}

// NewConn builds a Lark long-connection client and starts it in the
// background. Recv surfaces incoming events; Send calls the OpenAPI. The bot's
// own open_id is fetched from bot/v3/info for group @-mention gating.
func NewConn(appID, appSecret string) *Conn {
	c := newConn(appID, appSecret)
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.client = larkws.NewClient(appID, appSecret, larkws.WithEventHandler(c.handler))
	go c.supervise(ctx)
	// Resolve the bot's own open_id before returning so the adapter can gate
	// group @-mentions from the first event. Bounded (see fetchBotOpenID);
	// on failure the empty value makes mentioned() accept group messages
	// rather than silently dropping them all.
	c.fetchBotOpenID()
	return c
}

// NewWebhookConn builds a Conn whose inbound events are pushed over HTTP
// (Feishu's event-subscription "请求网址" mode) instead of a long connection.
//
// Only the inbound transport changes: sending, cards and attachment download
// already go through the OpenAPI with the app credentials, so a webhook-mode
// binding behaves exactly like a long-connection one from the pipeline's point
// of view. Events reach the adapter through Feed.
func NewWebhookConn(appID, appSecret string) *Conn {
	c := newConn(appID, appSecret)
	// No long connection to supervise, but Recv still honours cancellation, so
	// the context exists for Close to cancel.
	_, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.fetchBotOpenID()
	return c
}

// newConn builds the platform-independent parts of a Conn: the OpenAPI client,
// the event buffer and the dispatcher that normalizes SDK callbacks into the
// adapter's event stream. Both transports share it so a new event type has to be
// registered once.
func newConn(appID, appSecret string) *Conn {
	c := &Conn{
		appID:     appID,
		appSecret: appSecret,
		events:    make(chan []byte, 64),
		api:       lark.NewClient(appID, appSecret),
		done:      make(chan struct{}),
	}
	c.handler = dispatcher.NewEventDispatcher("", "").
		OnP2MessageReceiveV1(func(_ context.Context, ev *larkim.P2MessageReceiveV1) error {
			raw, err := json.Marshal(ev)
			if err != nil {
				return err
			}
			c.push(raw, "message")
			return nil
		}).
		// Interactive-card buttons (the approval card). The click arrives on the
		// same stream as messages; it is re-encoded into the adapter's event
		// stream so the decision travels the exact same path as a typed reply
		// (dedup, bus idempotency, worker approval resolution, audit).
		OnP2CardActionTrigger(func(_ context.Context, ev *callback.CardActionTriggerEvent) (*callback.CardActionTriggerResponse, error) {
			if ev == nil || ev.Event == nil {
				return nil, nil
			}
			raw, err := json.Marshal(toCardActionEnvelope(ev))
			if err != nil {
				return nil, err
			}
			c.push(raw, "card action")
			// Ack with a toast: without a response the client shows a generic
			// failure even though the decision was accepted.
			return &callback.CardActionTriggerResponse{
				Toast: &callback.Toast{Type: "info", Content: "已提交，Agent 收到你的决定"},
			}, nil
		})
	return c
}

// Feed hands one authenticated HTTP callback body to the event stream, so the
// adapter's existing decode/dedup pipeline handles it unchanged. It reports
// false when the buffer is full: the HTTP layer then answers 503 and the platform
// retries, which is strictly better than dropping a user's message.
func (c *Conn) Feed(payload []byte) bool {
	return c.push(payload, "webhook event")
}

// push enqueues one raw event without blocking the caller (an SDK dispatcher, or
// an HTTP handler that must answer promptly).
func (c *Conn) push(raw []byte, what string) bool {
	select {
	case <-c.done:
		return false
	default:
	}
	select {
	case c.events <- raw:
		return true
	default:
		slog.Warn("feishu: inbound buffer full, dropping event", "kind", what)
		return false
	}
}

// reconnectMaxBackoff caps the reconnect delay.
const reconnectMaxBackoff = 30 * time.Second

// DownloadMedia fetches a Feishu attachment through the message-resource API
// (image/file/audio share it; only the type parameter differs).
func (c *Conn) DownloadMedia(ctx context.Context, att channels.MediaAttachment) ([]byte, string, error) {
	if att.MessageID == "" || att.FileKey == "" {
		return nil, "", errors.New("feishu: attachment needs message id and file key")
	}
	resourceType := "file"
	switch att.Kind {
	case channels.MediaImage:
		resourceType = "image"
	case channels.MediaVoice:
		resourceType = "audio"
	}
	req := larkim.NewGetMessageResourceReqBuilder().
		MessageId(att.MessageID).
		FileKey(att.FileKey).
		Type(resourceType).
		Build()
	resp, err := c.api.Im.MessageResource.Get(ctx, req)
	if err != nil {
		return nil, "", fmt.Errorf("feishu: get message resource: %w", err)
	}
	if !resp.Success() {
		return nil, "", fmt.Errorf("feishu: get message resource: code=%d msg=%s", resp.Code, resp.Msg)
	}
	// The SDK streams the resource; bound the read so an oversized attachment
	// cannot exhaust memory before the adapter's own limit check.
	data, err := io.ReadAll(io.LimitReader(resp.File, maxMediaBytes+1))
	if err != nil {
		return nil, "", fmt.Errorf("feishu: read message resource: %w", err)
	}
	if len(data) == 0 {
		return nil, "", errors.New("feishu: message resource is empty")
	}
	if len(data) > maxMediaBytes {
		return nil, "", fmt.Errorf("feishu: attachment exceeds %d bytes", maxMediaBytes)
	}
	mime := ""
	if resp.FileName != "" {
		mime = mimeForName(resp.FileName)
	}
	if mime == "" {
		mime = mimeForKind(att.Kind)
	}
	return data, mime, nil
}

// maxMediaBytes bounds one attachment, matching the adapter's own limit.
const maxMediaBytes = 8 << 20

// mimeForKind is the fallback mime type when Feishu sends no filename.
func mimeForKind(kind string) string {
	switch kind {
	case channels.MediaImage:
		return "image/png"
	case channels.MediaVoice:
		return "audio/opus"
	default:
		return "application/octet-stream"
	}
}

// mimeForName derives a mime type from the resource filename.
func mimeForName(name string) string {
	switch strings.ToLower(filepath.Ext(name)) {
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	case ".pdf":
		return "application/pdf"
	case ".txt", ".md", ".csv", ".json", ".log":
		return "text/plain; charset=utf-8"
	default:
		return ""
	}
}

// toCardActionEnvelope converts an SDK card-action callback into the envelope
// the adapter decodes (see feishu.go). Button values are copied to
// string→string so the payload is stable across the JSON round trip.
func toCardActionEnvelope(ev *callback.CardActionTriggerEvent) cardActionEnvelope {
	act := cardActionPayload{}
	if ev.Event != nil {
		act.MessageID = ev.Event.Context.OpenMessageID
		if ev.Event.Operator != nil {
			act.OpenID = ev.Event.Operator.OpenID
			if ev.Event.Operator.UserID != nil {
				act.UserID = *ev.Event.Operator.UserID
			}
		}
		act.Value = map[string]string{}
		if ev.Event.Action != nil {
			for k, v := range ev.Event.Action.Value {
				if s, ok := v.(string); ok {
					act.Value[k] = s
				}
			}
		}
		// The callback token doubles as a per-click unique id when the event
		// header is absent.
		if act.EventID == "" {
			act.EventID = ev.Event.Token
		}
	}
	return cardActionEnvelope{CardAction: &act}
}

// supervise keeps the long connection alive. The SDK's Start returns when the
// connection ends for good (network drop, credential rotation, server drain),
// and a ws.Client is not restartable — so a new client is built and started
// with exponential backoff until Close. Without this a single disconnect left
// the adapter permanently deaf: Recv kept returning "long connection closed"
// while the adapter's read loop had already given up.
func (c *Conn) supervise(ctx context.Context) {
	backoff := time.Second
	for {
		err := c.client.Start(ctx)
		if ctx.Err() != nil {
			return
		}
		slog.Warn("feishu: long connection ended, reconnecting", "err", err, "in", backoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
		if backoff < reconnectMaxBackoff {
			if backoff *= 2; backoff > reconnectMaxBackoff {
				backoff = reconnectMaxBackoff
			}
		}
		c.client = larkws.NewClient(c.appID, c.appSecret, larkws.WithEventHandler(c.handler))
	}
}

// fetchBotOpenID resolves the bot's own open_id once, for group @-mention
// gating. Bounded so NewConn never blocks Reload for long; on failure
// botOpenID stays empty and mentioned() falls back to accepting group
// messages rather than dropping them all.
func (c *Conn) fetchBotOpenID() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.api.Get(ctx, "/open-apis/bot/v3/info", nil, larkcore.AccessTokenTypeTenant)
	if err != nil {
		slog.Warn("feishu: fetch bot open_id failed, group @-mention gating disabled", "err", err)
		return
	}
	var info botInfoResp
	if err := json.Unmarshal(resp.RawBody, &info); err != nil || info.Code != 0 || info.Bot.OpenID == "" {
		slog.Warn("feishu: bot open_id unavailable, group @-mention gating disabled", "code", info.Code)
		return
	}
	c.mu.Lock()
	c.botOpenID = info.Bot.OpenID
	c.mu.Unlock()
}

// BotOpenID returns the bot's own open_id (empty until fetchBotOpenID succeeds).
func (c *Conn) BotOpenID() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.botOpenID
}

// Recv returns the next event's JSON, or the ctx/shutdown error. A dropped
// long connection does not end this call: supervise reconnects, so the adapter
// stays attached and waits for events instead of shutting itself down.
func (c *Conn) Recv(ctx context.Context) ([]byte, error) {
	select {
	case b := <-c.events:
		return b, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, errors.New("feishu: long connection closed")
	}
}

// Send delivers a text message to a chat over the Lark OpenAPI. Both single
// (p2p) and group chats carry a stable chat_id, so receive_id_type is always
// chat_id — never open_id (whose presence depends on app permissions and is
// the source of 230001 invalid receive_id).
func (c *Conn) Send(ctx context.Context, target, chatType, text string) error {
	content, _ := json.Marshal(map[string]string{"text": text})
	return c.send(ctx, target, larkim.MsgTypeText, string(content))
}

// SendStream collects all streaming chunks and sends as a single text message.
// Feishu does not natively support message-in-place streaming updates.
func (c *Conn) SendStream(ctx context.Context, target, chatType string, stream <-chan string) error {
	var fullText string
	for chunk := range stream {
		fullText += chunk
	}
	if fullText == "" {
		return nil
	}
	return c.Send(ctx, target, chatType, fullText)
}

// SendCard sends an interactive card: markdown body plus one button per action.
// A button's callback value is echoed back by Feishu to the card-action handler
// (see the dispatcher registration in NewConn), which is how an approval
// decision comes back without the user typing anything.
func (c *Conn) SendCard(ctx context.Context, target, chatType string, card channels.Card) error {
	content := renderCard(card)
	return c.send(ctx, target, "interactive", content)
}

// renderCard builds the Feishu interactive-card JSON.
func renderCard(card channels.Card) string {
	elements := make([]map[string]any, 0, 2)
	if card.Content != "" {
		elements = append(elements, map[string]any{"tag": "markdown", "content": card.Content})
	}
	if len(card.Actions) > 0 {
		buttons := make([]map[string]any, 0, len(card.Actions))
		for i, act := range card.Actions {
			value := map[string]string{}
			for k, v := range act.Value {
				value[k] = v
			}
			btnType := "default"
			if i == 0 {
				btnType = "primary"
			}
			buttons = append(buttons, map[string]any{
				"tag":  "button",
				"text": map[string]any{"tag": "plain_text", "content": act.Text},
				"type": btnType,
				// behaviors.callback keeps the click inside the long connection:
				// Feishu POSTs the value to our card-action handler.
				"behaviors": []map[string]any{{"type": "callback", "value": value}},
				"value":     value,
			})
		}
		elements = append(elements, map[string]any{"tag": "action", "actions": buttons})
	}
	payload := map[string]any{"elements": elements}
	if card.Title != "" {
		payload["header"] = map[string]any{
			"title": map[string]any{"tag": "plain_text", "content": card.Title},
		}
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		// A card that cannot be encoded is a programming error; fall back to the
		// body text so the user still gets the message.
		return fmt.Sprintf(`{"elements":[{"tag":"markdown","content":"%s"}]}`, escapeJSON(card.Content))
	}
	return string(raw)
}

func (c *Conn) send(ctx context.Context, target string, msgType, content string) error {
	body := larkim.NewCreateMessageReqBodyBuilder().
		ReceiveId(target).
		MsgType(msgType).
		Content(content).
		Build()
	req := larkim.NewCreateMessageReqBuilder().
		ReceiveIdType("chat_id").
		Body(body).
		Build()
	resp, err := c.api.Im.Message.Create(ctx, req)
	if err != nil {
		return fmt.Errorf("feishu: create message: %w", err)
	}
	if !resp.Success() {
		return fmt.Errorf("feishu: create message code=%d msg=%s", resp.Code, resp.Msg)
	}
	return nil
}

// escapeJSON returns a JSON-escaped string suitable for embedding inside a
// Feishu card markdown content literal. json.Marshal wraps the value in double
// quotes; we strip the surrounding quotes so callers can embed the result
// directly inside a JSON string literal.
func escapeJSON(s string) string {
	b, _ := json.Marshal(s)
	return string(b[1 : len(b)-1])
}

// Close stops the long connection and unblocks Recv.
func (c *Conn) Close() error {
	c.once.Do(func() {
		c.cancel()
		// Recv waits on done as well as ctx: closing it releases a reader that
		// is not watching this connection's context.
		close(c.done)
	})
	return nil
}
