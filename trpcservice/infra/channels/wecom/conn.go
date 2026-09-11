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
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

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

	// placeholders bounds the concurrent placeholder sends. The SDK call has
	// no timeout, so an ack that never arrives would otherwise leave one
	// goroutine parked per inbound message; with the bound, a stuck SDK costs
	// a slot instead of unbounded memory (the real reply still goes out).
	placeholders *slotLimiter

	// mu guards pending: per reply-target, the FIFO of open stream
	// placeholders awaiting their real reply. The worker processes one session
	// serially and replies are appended in message order, so popping the queue
	// keeps each reply glued to the placeholder it must finish — even when the
	// user fires several messages before the first reply lands.
	mu      sync.Mutex
	pending map[string][]*pendingStream
}

// placeholderSlots is how many placeholder sends may be in flight at once.
const placeholderSlots = 8

// slotLimiter is a counting semaphore with a non-blocking acquire.
type slotLimiter struct {
	slots chan struct{}
}

func newSlotLimiter(n int) *slotLimiter {
	return &slotLimiter{slots: make(chan struct{}, n)}
}

// tryAcquire takes a slot, reporting false when none is free.
func (l *slotLimiter) tryAcquire() bool {
	if l == nil || l.slots == nil {
		return false
	}
	select {
	case l.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// release returns a slot.
func (l *slotLimiter) release() {
	if l == nil || l.slots == nil {
		return
	}
	select {
	case <-l.slots:
	default:
	}
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

// Media fetch bounds and client, mirroring the adapter's limits so an oversized
// or slow attachment is rejected before it reaches the model.
const maxMediaBytes = 8 << 20

var mediaHTTPClient = &http.Client{Timeout: 15 * time.Second}

// NewConn builds and connects a WeCom AI-bot long-connection client. It never
// blocks: the SDK dials in the background and reconnects on its own schedule.
func NewConn(botID, secret string) *Conn {
	c := &Conn{
		events:       make(chan []byte, 64),
		done:         make(chan struct{}),
		pending:      make(map[string][]*pendingStream),
		placeholders: newSlotLimiter(placeholderSlots),
	}
	c.client = aibot.NewWSClient(aibot.WSClientOptions{
		BotID:  botID,
		Secret: secret,
	})
	c.client.OnMessageText(func(frame *aibot.WsFrame) { c.onMessage(frame) })
	// Non-text messages are no longer dropped on the floor: each one becomes a
	// bus message carrying an attachment descriptor (image/file) or the voice
	// transcript, so the user gets an answer or an explicit receipt.
	c.client.OnMessageImage(func(frame *aibot.WsFrame) { c.onMessage(frame) })
	c.client.OnMessageFile(func(frame *aibot.WsFrame) { c.onMessage(frame) })
	c.client.OnMessageVoice(func(frame *aibot.WsFrame) { c.onMessage(frame) })
	c.client.OnMessageVideo(func(frame *aibot.WsFrame) { c.onMessage(frame) })
	c.client.OnMessageMixed(func(frame *aibot.WsFrame) { c.onMessage(frame) })
	c.client.Connect()
	return c
}

// onMessage normalizes one inbound frame (text or attachment), opens the stream
// placeholder WeCom expects within its callback timeout, and hands the
// translated message to the adapter.
func (c *Conn) onMessage(frame *aibot.WsFrame) {
	raw, target, err := translateText(frame)
	if err != nil || raw == nil || target == "" {
		return // malformed or undeduplicatable: nothing to surface
	}
	// Open a stream placeholder immediately: this is the reply WeCom expects
	// within the callback timeout (the SDK does not auto-ack), so redelivery
	// stops and the client shows an in-progress message. The real reply later
	// finishes this stream in place. Done in a goroutine because ReplyStream
	// blocks waiting for the ack of our own reply.
	ps := &pendingStream{frame: frame, streamID: fmt.Sprintf("s_%s", frame.Headers.ReqID)}
	c.mu.Lock()
	c.pending[target] = append(c.pending[target], ps)
	c.mu.Unlock()
	if c.placeholders.tryAcquire() {
		go func() {
			defer c.placeholders.release()
			_, _ = c.client.ReplyStream(frame, ps.streamID, thinkingMsg, false, nil, nil)
		}()
	} else {
		// Every slot is stuck on an unacknowledged send: skip the "processing"
		// hint instead of adding another parked goroutine. The reply itself
		// still reaches the user via Send.
		slog.Warn("wecom: placeholder slots exhausted, skipping the in-progress hint")
	}
	select {
	case c.events <- raw:
	case <-c.done:
	default:
		// Adapter not draining: drop rather than block the SDK reader. A
		// dropped inbound is a lost user message, so make it loud.
		slog.Warn("wecom: inbound buffer full, dropping message")
	}
}

// translateText converts one aibot message frame (text or attachment) into the
// XML Message shape the Adapter normalizes. It returns (nil, "", nil) for frames
// that carry no dedup key (msgid), so malformed pushes are skipped. target is
// the reply address the later outbound message will carry: chatid for group
// chat, the sender's userid for single chat — the same value ToInbound keys on.
func translateText(frame *aibot.WsFrame) ([]byte, string, error) {
	// Text first, then the attachment kinds: each branch fills the same Message
	// and the media descriptor list, so ToInbound stays type-agnostic.
	var tm aibot.TextMessage
	if err := aibot.ParseMessageBody(frame, &tm); err == nil && tm.MsgID != "" {
		if tm.MsgType == "text" || tm.Text.Content != "" {
			m := Message{
				FromUserName: tm.From.UserID,
				CreateTime:   tm.CreateTime,
				MsgType:      tm.MsgType,
				Content:      tm.Text.Content,
				MsgId:        tm.MsgID,
				ChatId:       tm.ChatID,
			}
			raw, err := xml.Marshal(m)
			return raw, targetOf(tm.ChatID, tm.From.UserID), err
		}
	}
	return translateMedia(frame)
}

// translateMedia handles the non-text frames (image / file / voice / video /
// mixed). Voice arrives already transcribed by WeCom, so it becomes text with
// the transcript as the body.
func translateMedia(frame *aibot.WsFrame) ([]byte, string, error) {
	var base aibot.BaseMessage
	if err := aibot.ParseMessageBody(frame, &base); err != nil {
		return nil, "", err
	}
	if base.MsgID == "" {
		return nil, "", nil // undeduplicatable
	}
	m := Message{
		FromUserName: base.From.UserID,
		CreateTime:   base.CreateTime,
		MsgType:      base.MsgType,
		MsgId:        base.MsgID,
		ChatId:       base.ChatID,
	}
	switch base.MsgType {
	case "image":
		var im aibot.ImageMessage
		if err := aibot.ParseMessageBody(frame, &im); err != nil {
			return nil, "", err
		}
		m.Media = append(m.Media, MediaPart{Kind: channels.MediaImage, URL: im.Image.URL, AesKey: im.Image.AesKey})
	case "file":
		var fm aibot.FileMessage
		if err := aibot.ParseMessageBody(frame, &fm); err != nil {
			return nil, "", err
		}
		m.Media = append(m.Media, MediaPart{Kind: channels.MediaFile, URL: fm.File.URL, AesKey: fm.File.AesKey})
	case "voice":
		var vm aibot.VoiceMessage
		if err := aibot.ParseMessageBody(frame, &vm); err != nil {
			return nil, "", err
		}
		// WeCom transcribes voice for us: surface the text instead of an audio
		// blob the model cannot consume.
		m.Content = vm.Voice.Content
	case "video":
		var vm aibot.VideoMessage
		if err := aibot.ParseMessageBody(frame, &vm); err != nil {
			return nil, "", err
		}
		m.Media = append(m.Media, MediaPart{Kind: channels.MediaFile, URL: vm.Video.URL, AesKey: vm.Video.AesKey, Name: "video"})
	case "mixed":
		var mm aibot.MixedMessage
		if err := aibot.ParseMessageBody(frame, &mm); err != nil {
			return nil, "", err
		}
		// A mixed message interleaves text and images; keep the text as the body
		// and each image as an attachment.
		for _, item := range mm.Mixed.MsgItem {
			if item.Text != nil && item.Text.Content != "" {
				if m.Content != "" {
					m.Content += "\n"
				}
				m.Content += item.Text.Content
			}
			if item.Image != nil && item.Image.URL != "" {
				m.Media = append(m.Media, MediaPart{Kind: channels.MediaImage, URL: item.Image.URL, AesKey: item.Image.AesKey})
			}
		}
	default:
		// An unhandled kind still becomes a message: the user gets a receipt
		// rather than silence.
		m.Media = append(m.Media, MediaPart{Kind: channels.MediaFile, Name: base.MsgType})
	}
	raw, err := xml.Marshal(m)
	return raw, targetOf(base.ChatID, base.From.UserID), err
}

// targetOf picks the reply address: group chat id when present, else the sender.
func targetOf(chatID, userID string) string {
	if chatID != "" {
		return chatID
	}
	return userID
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

// DownloadMedia fetches one WeCom attachment: the platform hands out a download
// URL plus the AES key that protects the payload, so the bytes are fetched and
// then decrypted. The returned mime type is inferred from the attachment kind
// (WeCom does not send one).
func (c *Conn) DownloadMedia(ctx context.Context, att channels.MediaAttachment) ([]byte, string, error) {
	if att.URL == "" {
		return nil, "", errors.New("wecom: attachment has no download url")
	}
	data, mime, err := fetchURL(ctx, att.URL)
	if err != nil {
		return nil, "", err
	}
	if att.AesKey != "" {
		plain, err := DecryptMedia(att.AesKey, data)
		if err != nil {
			return nil, "", fmt.Errorf("wecom: decrypt media: %w", err)
		}
		data = plain
	}
	if mime == "" || mime == "application/octet-stream" {
		mime = mimeForKind(att.Kind)
	}
	return data, mime, nil
}

// mimeForKind is the fallback mime type when the platform sends none.
func mimeForKind(kind string) string {
	switch kind {
	case channels.MediaImage:
		return "image/png"
	case channels.MediaVoice:
		return "audio/amr"
	default:
		return "application/octet-stream"
	}
}

// fetchURL downloads a bounded attachment over HTTP.
func fetchURL(ctx context.Context, url string) ([]byte, string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, "", err
	}
	resp, err := mediaHTTPClient.Do(req)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("wecom: download %s: status %d", url, resp.StatusCode)
	}
	// One byte over the cap is enough to detect an oversized attachment without
	// buffering the whole thing.
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxMediaBytes+1))
	if err != nil {
		return nil, "", err
	}
	if len(data) > maxMediaBytes {
		return nil, "", fmt.Errorf("wecom: attachment exceeds %d bytes", maxMediaBytes)
	}
	return data, resp.Header.Get("Content-Type"), nil
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
