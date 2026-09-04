// Package feishu adapts the Feishu (Lark) platform into the unified channels
// contract. It provides event signature verification, normalization of inbound
// im.message.receive_v1 events, and the Adapter that assembles those pieces on
// top of a channels.Conn.
package feishu

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/channels"
)

// Name is the stable channel identifier used by this adapter.
const Name = "feishu"

// ---------------------------------------------------------------- protocol --

// VerifySignature checks a Feishu event-subscription signature:
//
//	signature = sha256(timestamp + nonce + encrypt_key)
//
// Note this is concatenation (not sorted), unlike WeCom.
func VerifySignature(encryptKey, timestamp, nonce, signature string) bool {
	sum := sha256.Sum256([]byte(timestamp + nonce + encryptKey))
	return hex.EncodeToString(sum[:]) == signature
}

// Event is a Feishu im.message.receive_v1 event (schema 2.0).
type Event struct {
	Header Header `json:"header"`
	Event  Body   `json:"event"`
}

// Header carries the event metadata, including the dedup key.
type Header struct {
	EventID   string `json:"event_id"`
	EventType string `json:"event_type"`
}

// Body is the event payload.
type Body struct {
	Sender  Sender  `json:"sender"`
	Message Message `json:"message"`
}

// Sender identifies who sent the message.
type Sender struct {
	SenderID SenderID `json:"sender_id"`
}

// SenderID holds Feishu user identifiers; OpenID is preferred.
type SenderID struct {
	OpenID string `json:"open_id"`
	UserID string `json:"user_id"`
}

// Message is the Feishu message payload.
type Message struct {
	MessageID   string    `json:"message_id"`
	ChatID      string    `json:"chat_id"`
	ChatType    string    `json:"chat_type"` // p2p | group
	MessageType string    `json:"message_type"`
	Content     string    `json:"content"` // JSON string for text/post
	Mentions    []Mention `json:"mentions"`
}

// MentionID holds the identity of a mentioned user/bot. Which field is set
// depends on the app's permissions; open_id is preferred for the bot.
type MentionID struct {
	OpenID  string `json:"open_id"`
	UserID  string `json:"user_id"`
	UnionID string `json:"union_id"`
}

// Mention references a user or bot mentioned in a group message. Key is a
// positional placeholder ("@_user_1"); the real identity is in Id.
type Mention struct {
	Key  string    `json:"key"`
	Name string    `json:"name"`
	Id   MentionID `json:"id"`
}

// ToInbound converts a Feishu event into a normalized InboundMessage.
// botOpenID is the bot's own open_id, used to enforce the group-chat rule that
// the bot only responds when explicitly @-mentioned. Returns nil when a group
// message does not mention the bot.
//
// The reply target is always msg.ChatID: Feishu p2p messages also carry a
// stable chat_id (oc_xxx_p2p), so replies can uniformly use
// receive_id_type=chat_id and never depend on the sender's open_id permission.
func ToInbound(tenantID string, ev *Event, botOpenID string) *channels.InboundMessage {
	if ev == nil {
		return nil
	}
	msg := ev.Event.Message
	chatType := chatTypeOf(msg.ChatType)
	if chatType == channels.ChatTypeGroup && !mentioned(msg.Mentions, botOpenID) {
		return nil
	}
	chatID := msg.ChatID
	userID := ev.Event.Sender.SenderID.OpenID
	if userID == "" {
		userID = ev.Event.Sender.SenderID.UserID
	}
	return &channels.InboundMessage{
		PlatformMsgID: ev.Header.EventID, // event_id is the dedup key
		TenantID:      tenantID,
		SessionID:     channels.BuildSessionID(tenantID, Name, chatType, chatID),
		UserID:        userID,
		ChatType:      chatType,
		ChatID:        chatID,
		Content:       extractText(msg.MessageType, msg.Content),
		MsgType:       normalizeMsgType(msg.MessageType),
	}
}

func chatTypeOf(t string) string {
	if t == "group" {
		return channels.ChatTypeGroup
	}
	return channels.ChatTypeSingle
}

// mentioned reports whether the bot (identified by open_id) was @-mentioned.
// The mention's Id.OpenID (not Key, which is a positional placeholder) carries
// the real identity. botOpenID empty means no gating is possible, so we accept
// the message rather than silently dropping every group message.
func mentioned(mentions []Mention, botOpenID string) bool {
	if botOpenID == "" {
		return true
	}
	for _, m := range mentions {
		if m.Id.OpenID == botOpenID || m.Id.UnionID == botOpenID {
			return true
		}
	}
	return false
}

// extractText unwraps the JSON content of a text message; non-text content is
// passed through unchanged.
func extractText(msgType, content string) string {
	if msgType != channels.MsgTypeText {
		return content
	}
	var payload struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal([]byte(content), &payload); err != nil {
		return content
	}
	return payload.Text
}

func normalizeMsgType(t string) string {
	switch t {
	case channels.MsgTypeText, channels.MsgTypeImage, channels.MsgTypeFile:
		return t
	default:
		return channels.MsgTypeEvent
	}
}

// ----------------------------------------------------------------- adapter --

// Adapter implements channels.Adapter for Feishu on top of a Conn. The Conn
// owns connection establishment and verification; Adapter normalizes and dedups
// inbound events.
type Adapter struct {
	tenantID  string
	botOpenID string
	conn      channels.Conn
	inbound   chan *channels.InboundMessage

	mu   sync.Mutex
	seen map[string]struct{}
}

// New returns a Feishu adapter bound to the given tenant and bot open_id.
func New(tenantID, botOpenID string, conn channels.Conn) *Adapter {
	return &Adapter{
		tenantID:  tenantID,
		botOpenID: botOpenID,
		conn:      conn,
		inbound:   make(chan *channels.InboundMessage, 64),
		seen:      make(map[string]struct{}),
	}
}

// Name returns the stable channel identifier.
func (a *Adapter) Name() string { return Name }

// Inbound returns the channel of normalized inbound messages.
func (a *Adapter) Inbound() <-chan *channels.InboundMessage { return a.inbound }

// Start consumes raw events from the Conn until ctx is done.
func (a *Adapter) Start(ctx context.Context) error {
	for {
		raw, err := a.conn.Recv(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // graceful shutdown
			}
			return err
		}
		var ev Event
		if err := json.Unmarshal(raw, &ev); err != nil || ev.Header.EventID == "" {
			continue // skip malformed or undeduplicatable events
		}
		if a.isDup(ev.Header.EventID) {
			continue
		}
		in := ToInbound(a.tenantID, &ev, a.botOpenID)
		if in == nil {
			continue // filtered: group message without @bot
		}
		select {
		case a.inbound <- in:
		case <-ctx.Done():
			return nil
		}
	}
}

// Send delivers a normalized outbound message back to the platform.
func (a *Adapter) Send(ctx context.Context, msg *channels.OutboundMessage) error {
	if msg == nil || msg.Inbound == nil {
		return fmt.Errorf("feishu: outbound requires inbound context")
	}
	return a.conn.Send(ctx, msg.Inbound.ChatID, msg.Inbound.ChatType, msg.Text())
}

// Stop closes the underlying connection.
func (a *Adapter) Stop(_ context.Context) error {
	return a.conn.Close()
}

// isDup records and reports whether an event id was already seen.
// ponytail: in-memory set; swap for Redis idem:{msg_key} in phase 6.
func (a *Adapter) isDup(id string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.seen[id]; ok {
		return true
	}
	a.seen[id] = struct{}{}
	return false
}
