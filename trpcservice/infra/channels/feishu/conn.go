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
	"log/slog"
	"sync"
	"time"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/channels"
)

// Conn implements channels.Conn over the Lark long connection.
type Conn struct {
	appID     string
	appSecret string

	events chan []byte
	api    *lark.Client
	client *larkws.Client

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
	c := &Conn{
		appID:     appID,
		appSecret: appSecret,
		events:    make(chan []byte, 64),
		api:       lark.NewClient(appID, appSecret),
		done:      make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel

	handler := dispatcher.NewEventDispatcher("", "").
		OnP2MessageReceiveV1(func(_ context.Context, ev *larkim.P2MessageReceiveV1) error {
			raw, err := json.Marshal(ev)
			if err != nil {
				return err
			}
			select {
			case c.events <- raw:
			default:
				// Drop when the adapter is not draining rather than block the
				// SDK dispatcher; a dropped inbound is a lost message.
				slog.Warn("feishu: inbound buffer full, dropping event")
			}
			return nil
		})
	c.client = larkws.NewClient(appID, appSecret, larkws.WithEventHandler(handler))
	go func() {
		_ = c.client.Start(ctx)
		close(c.done)
	}()
	// Resolve the bot's own open_id before returning so the adapter can gate
	// group @-mentions from the first event. Bounded (see fetchBotOpenID);
	// on failure the empty value makes mentioned() accept group messages
	// rather than silently dropping them all.
	c.fetchBotOpenID()
	return c
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

// Recv returns the next event's JSON, or the ctx/connection shutdown error.
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

// SendCard sends an interactive card message via Feishu.
func (c *Conn) SendCard(ctx context.Context, target, chatType string, card channels.Card) error {
	content := fmt.Sprintf(`{"elements":[{"tag":"markdown","content":"%s"}]}`, escapeJSON(card.Content))
	return c.send(ctx, target, "interactive", content)
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

// Close stops the long connection.
func (c *Conn) Close() error {
	c.once.Do(func() { c.cancel() })
	return nil
}
