// Enterprise WeChat AI-bot long connection (WSS) implementation of
// channels.Conn, backed by github.com/go-sphere/wecom-aibot-go-sdk/aibot.
//
// The SDK owns the WSS handshake, aibot_subscribe authentication, heartbeats
// and frame framing; this type translates incoming aibot text messages into
// the XML Message shape the Adapter already normalizes (and dedups), and
// sends replies back over the same connection.
//
// Reply model (WeCom stream protocol): the moment a message arrives we open a
// stream placeholder (finish=false, "正在处理…") — this is the reply WeCom
// requires within the callback timeout, so redelivery stops and the client
// shows an in-progress message instead of nothing. The real reply later
// finishes the SAME stream.id in place (finish=true), so the user sees one
// message that is replaced, never an empty ack plus a separate push.
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
	"log/slog"
	"sync"

	"github.com/go-sphere/wecom-aibot-go-sdk/aibot"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/channels"
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

	// mu guards pending: per reply-target, the FIFO of open stream
	// placeholders awaiting their real reply. The worker processes one session
	// serially and replies are appended in message order, so popping the queue
	// keeps each reply glued to the placeholder it must finish — even when the
	// user fires several messages before the first reply lands.
	mu      sync.Mutex
	pending map[string][]*pendingStream
}

// pendingStream is an open stream placeholder (finish=false) that the real
// reply will finish in place (same stream.id, finish=true). frame carries the
// req_id the callback expects us to respond on.
type pendingStream struct {
	frame    *aibot.WsFrame
	streamID string
}

// thinkingMsg is the placeholder content shown while the agent works. The
// WeCom client replaces it with the final reply (stream.content is full-text
// and each push replaces the previous display), so the user never sees an
// empty "loading" bubble linger.
const thinkingMsg = "正在处理，请稍候…"

// NewConn builds and connects a WeCom AI-bot long-connection client. It never
// blocks: the SDK dials in the background and reconnects on its own schedule.
func NewConn(botID, secret string) *Conn {
	c := &Conn{
		events:  make(chan []byte, 64),
		done:    make(chan struct{}),
		pending: make(map[string][]*pendingStream),
	}
	c.client = aibot.NewWSClient(aibot.WSClientOptions{
		BotID:  botID,
		Secret: secret,
	})
	c.client.OnMessageText(func(frame *aibot.WsFrame) {
		raw, target, err := translateText(frame)
		if err != nil || raw == nil || target == "" {
			return // non-text, malformed or undeduplicatable: nothing to surface
		}
		// Open a stream placeholder immediately: this is the reply WeCom
		// expects within the callback timeout (the SDK does not auto-ack), so
		// redelivery stops and the client shows an in-progress message. The
		// real reply later finishes this stream in place. Done in a goroutine
		// because ReplyStream blocks waiting for the ack of our own reply.
		ps := &pendingStream{frame: frame, streamID: fmt.Sprintf("s_%s", frame.Headers.ReqID)}
		c.mu.Lock()
		c.pending[target] = append(c.pending[target], ps)
		c.mu.Unlock()
		go func() {
			_, _ = c.client.ReplyStream(frame, ps.streamID, thinkingMsg, false, nil, nil)
		}()
		select {
		case c.events <- raw:
		case <-c.done:
		default:
			// Adapter not draining: drop rather than block the SDK reader. A
			// dropped inbound is a lost user message, so make it loud.
			slog.Warn("wecom: inbound buffer full, dropping message")
		}
	})
	c.client.Connect()
	return c
}

// translateText converts an aibot text message frame into the XML Message
// shape the Adapter normalizes. It returns (nil, "", nil) for frames that
// carry no dedup key (msgid), so malformed pushes are skipped. target is the
// reply address the later outbound message will carry: chatid for group chat,
// the sender's userid for single chat — the same value ToInbound keys on.
func translateText(frame *aibot.WsFrame) ([]byte, string, error) {
	var tm aibot.TextMessage
	if err := aibot.ParseMessageBody(frame, &tm); err != nil {
		return nil, "", err
	}
	if tm.MsgID == "" {
		return nil, "", nil // undeduplicatable
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
	raw, err := xml.Marshal(m)
	target := tm.ChatID
	if target == "" {
		target = tm.From.UserID
	}
	return raw, target, err
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

// Send finishes the target's oldest open placeholder stream in place with the
// real reply text (finish=true, same stream.id). If no placeholder is pending
// (e.g. an approval notice that arrived after its message already resolved, or
// a push with no inbound), it falls back to the active-push markdown channel.
// chatType is unused (WeCom addresses both single and group by their id).
func (c *Conn) Send(_ context.Context, target, _ string, text string) error {
	c.mu.Lock()
	queue := c.pending[target]
	var ps *pendingStream
	if len(queue) > 0 {
		ps = queue[0]
		if len(queue) == 1 {
			delete(c.pending, target)
		} else {
			c.pending[target] = queue[1:]
		}
	}
	c.mu.Unlock()

	if ps != nil {
		if _, err := c.client.ReplyStream(ps.frame, ps.streamID, text, true, nil, nil); err == nil {
			return nil
		}
		// Placeholder finish failed (e.g. the stream timed out): fall through
		// to an active push so the user still gets the answer.
	}
	if _, err := c.client.SendMarkdown(target, text); err != nil {
		return fmt.Errorf("wecom: send: %w", err)
	}
	return nil
}

// SendStream drains the string channel and delivers the accumulated text as a
// single active markdown push. The aibot SDK (v1.0.4) only supports stream
// replies tied to an inbound callback frame (ReplyStream); there is no
// CreateStream / active stream-push API, so progressive updates are not
// available for outbound-initiated streams. We therefore collect all chunks
// and send the final result in one shot, matching the fallback behaviour of
// Send when no inbound placeholder is pending.
func (c *Conn) SendStream(_ context.Context, target, _ string, stream <-chan string) error {
	var full string
	for chunk := range stream {
		full += chunk
	}
	if full == "" {
		return nil
	}
	return c.Send(context.Background(), target, "", full)
}

// SendCard sends the card payload as an active markdown push. The aibot SDK
// only exposes SendTemplateCard (for structured template cards) and
// SendMarkdown (for plain markdown); channels.Card is a generic payload with
// no guaranteed 1:1 mapping to a template card, so we render the card content
// as markdown text.
func (c *Conn) SendCard(_ context.Context, target, _ string, card channels.Card) error {
	if card.Content == "" {
		return nil
	}
	return c.Send(context.Background(), target, "", card.Content)
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
