// Enterprise WeChat AI-bot long connection (WSS) implementation of
// channels.Conn, backed by github.com/go-sphere/wecom-aibot-go-sdk/aibot.
//
// The SDK owns the WSS handshake, aibot_subscribe authentication, heartbeats
// and frame framing; this type translates incoming aibot text messages into
// the XML Message shape the Adapter already normalizes (and dedups), and
// sends replies back over the same connection.
//
// Only text messages are surfaced for now: image / file / voice / video /
// mixed and event frames (enter_chat, template_card, feedback) are ignored.
//
// NOTE: requires live bot credentials to actually connect; the code is
// unit-tested for the translation and send-body building only.
package wecom

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"sync"

	"github.com/go-sphere/wecom-aibot-go-sdk/aibot"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

// errClosed is returned by Recv once the connection has been closed.
var errClosed = errors.New("wecom: connection closed")

// Conn implements channels.Conn over the enterprise WeChat AI-bot long
// connection. NewConn builds the SDK client and starts it immediately.
type Conn struct {
	client *aibot.WSClient

	events chan []byte // buffered XML-encoded Message, drained by Adapter.Start
	once   sync.Once
	done   chan struct{}
}

// NewConn builds and connects a WeCom AI-bot long-connection client. It never
// blocks: the SDK dials in the background and reconnects on its own schedule.
func NewConn(botID, secret string) *Conn {
	c := &Conn{
		events: make(chan []byte, 64),
		done:   make(chan struct{}),
	}
	c.client = aibot.NewWSClient(aibot.WSClientOptions{
		BotID:  botID,
		Secret: secret,
	})
	c.client.OnMessageText(func(frame *aibot.WsFrame) {
		raw, err := translateText(frame)
		if err != nil || raw == nil {
			return // non-text or malformed: nothing to surface
		}
		select {
		case c.events <- raw:
		case <-c.done:
		default: // adapter not draining; drop rather than block the SDK reader
		}
	})
	c.client.Connect()
	return c
}

// translateText converts an aibot text message frame into the XML Message
// shape the Adapter normalizes. It returns (nil, nil) for frames that carry
// no dedup key (msgid), so malformed pushes are skipped.
func translateText(frame *aibot.WsFrame) ([]byte, error) {
	var tm aibot.TextMessage
	if err := aibot.ParseMessageBody(frame, &tm); err != nil {
		return nil, err
	}
	if tm.MsgID == "" {
		return nil, nil // undeduplicatable
	}
	// aibot fields map 1:1 onto the XML Message shape: from.userid ->
	// FromUserName, msgid -> MsgId, chatid -> ChatId (group only; empty for
	// single chat, which ToInbound then keys on FromUserName).
	m := Message{
		FromUserName: tm.From.UserID,
		CreateTime:   tm.CreateTime,
		MsgType:      tm.MsgType,
		Content:      tm.Text.Content,
		MsgId:        tm.MsgID,
		ChatId:       tm.ChatID,
	}
	return xml.Marshal(m)
}

// Recv blocks until the next translated message is available, the ctx is
// done, or the connection closes.
func (c *Conn) Recv(ctx context.Context) ([]byte, error) {
	select {
	case raw := <-c.events:
		return raw, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, errClosed
	}
}

// Send delivers a text reply to the target (single-chat userid or group
// chatid) over the same long connection.
func (c *Conn) Send(_ context.Context, target, text string) error {
	body := aibot.CreateTextReplyBody(text)
	if _, err := c.client.SendMessage(target, body); err != nil {
		return fmt.Errorf("wecom: send: %w", err)
	}
	return nil
}

// Close disconnects the underlying SDK client exactly once.
func (c *Conn) Close() error {
	c.once.Do(func() {
		c.client.Disconnect()
		close(c.done)
	})
	return nil
}

// Compile-time assertion that Conn satisfies the unified contract.
var _ channels.Conn = (*Conn)(nil)
