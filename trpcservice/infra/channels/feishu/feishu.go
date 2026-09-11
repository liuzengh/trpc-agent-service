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
		Media:         extractMedia(msg),
	}
}

// extractMedia reads the attachment descriptor out of an image/file message.
// Feishu sends a key, not a URL: the bytes are fetched later through the
// message-resource API, which needs the message id too.
func extractMedia(msg Message) []channels.MediaAttachment {
	switch msg.MessageType {
	case channels.MsgTypeImage:
		var payload struct {
			ImageKey string `json:"image_key"`
		}
		if err := json.Unmarshal([]byte(msg.Content), &payload); err != nil || payload.ImageKey == "" {
			return nil
		}
		return []channels.MediaAttachment{{
			Kind: channels.MediaImage, FileKey: payload.ImageKey, MessageID: msg.MessageID,
		}}
	case channels.MsgTypeFile:
		var payload struct {
			FileKey  string `json:"file_key"`
			FileName string `json:"file_name"`
		}
		if err := json.Unmarshal([]byte(msg.Content), &payload); err != nil || payload.FileKey == "" {
			return nil
		}
		return []channels.MediaAttachment{{
			Kind: channels.MediaFile, FileKey: payload.FileKey, MessageID: msg.MessageID, Name: payload.FileName,
		}}
	case "audio":
		var payload struct {
			FileKey  string `json:"file_key"`
			Duration int    `json:"duration"`
		}
		if err := json.Unmarshal([]byte(msg.Content), &payload); err != nil || payload.FileKey == "" {
			return nil
		}
		return []channels.MediaAttachment{{
			Kind: channels.MediaVoice, FileKey: payload.FileKey, MessageID: msg.MessageID,
		}}
	default:
		return nil
	}
}

func chatTypeOf(t string) string {
	if t == "group" {
		return channels.ChatTypeGroup
	}
	return channels.ChatTypeSingle
}

// cardActionEnvelope is how an interactive-card button click enters the
// adapter's event stream: the Conn re-encodes the SDK callback into this shape
// (see conn.go), and the adapter decoder turns it into a normal inbound
// message. Going through the same stream means the decision inherits the
// adapter's dedup, the bus idempotency key, the worker's approval resolution
// and the audit trail for free.
type cardActionEnvelope struct {
	CardAction *cardActionPayload `json:"card_action,omitempty"`
}

// cardActionPayload carries what the adapter needs from a button click.
type cardActionPayload struct {
	EventID   string            `json:"event_id"`
	OpenID    string            `json:"open_id"`
	UserID    string            `json:"user_id,omitempty"`
	MessageID string            `json:"message_id,omitempty"`
	Value     map[string]string `json:"value"`
}

// CardActionToInbound maps a button click onto the inbound message the approval
// flow already understands: the click becomes the user's 批准/拒绝 reply to the
// session the card was sent to. Returns nil when the payload lacks the session
// (nothing to decide) or the decision is unknown.
func CardActionToInbound(tenantID string, env *cardActionEnvelope) *channels.InboundMessage {
	if env == nil || env.CardAction == nil {
		return nil
	}
	act := env.CardAction
	sessionID := act.Value["session_id"]
	text := approvalReplyText(act.Value["decision"])
	if sessionID == "" || text == "" {
		return nil
	}
	userID := act.UserID
	if userID == "" {
		userID = act.OpenID
	}
	// The dedup key must be unique per click and stable across redeliveries:
	// the platform event id is exactly that (the message id alone would collide
	// when both buttons of one card are pressed).
	id := act.EventID
	if id == "" {
		id = act.MessageID + ":" + act.Value["decision"]
	}
	return &channels.InboundMessage{
		PlatformMsgID: id,
		TenantID:      tenantID,
		SessionID:     sessionID,
		UserID:        userID,
		ChatType:      channels.ChatTypeSingle,
		ChatID:        act.Value["chat_id"],
		Content:       text,
		MsgType:       "text",
	}
}

// approvalReplyText maps a card button decision onto the reply wording the
// worker's approval classifier already recognizes.
func approvalReplyText(decision string) string {
	switch decision {
	case "approve":
		return "批准"
	case "deny":
		return "拒绝"
	default:
		return ""
	}
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
// owns connection establishment and verification; the adapter only decodes and
// normalizes inbound events. The shared pump, dedup and send plumbing lives in
// channels.BaseAdapter.
type Adapter struct {
	tenantID  string
	botOpenID string
	conn      channels.Conn
	*channels.BaseAdapter
}

// New returns a Feishu adapter bound to the given tenant and bot open_id.
func New(tenantID, botOpenID string, conn channels.Conn) *Adapter {
	return &Adapter{tenantID: tenantID, botOpenID: botOpenID, conn: conn, BaseAdapter: channels.NewBaseAdapter(Name, conn)}
}

// Feed pushes one authenticated HTTP callback payload into the adapter's event
// stream, making the adapter satisfy channels.WebhookAdapter when it was built
// on a webhook Conn.
//
// A long-connection Conn receives events from the platform, so feeding it would
// bypass the SDK's own dispatch: that is reported as "not accepted" rather than
// silently injecting an event.
func (a *Adapter) Feed(payload []byte) bool {
	f, ok := a.conn.(interface{ Feed([]byte) bool })
	if !ok {
		return false
	}
	return f.Feed(payload)
}

// Start consumes raw events from the Conn until ctx is done.
func (a *Adapter) Start(ctx context.Context) error {
	return a.BaseAdapter.Run(ctx, func(raw []byte) (*channels.InboundMessage, bool) {
		// A card button click first: it arrives on the same stream as message
		// events but in a different envelope.
		var env cardActionEnvelope
		if err := json.Unmarshal(raw, &env); err == nil && env.CardAction != nil {
			in := CardActionToInbound(a.tenantID, &env)
			return in, in != nil
		}
		var ev Event
		if err := json.Unmarshal(raw, &ev); err != nil || ev.Header.EventID == "" {
			return nil, false // skip malformed or undeduplicatable events
		}
		in := ToInbound(a.tenantID, &ev, a.botOpenID)
		if in == nil {
			return nil, false // filtered: group message without @bot
		}
		return in, true
	})
}
