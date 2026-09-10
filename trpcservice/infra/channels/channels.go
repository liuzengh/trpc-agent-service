// Package channels adapts IM platforms (WeCom, WeChat, Telegram, etc.)
// into tRPC-Agent-Go Runner inputs, following the OpenClaw Channel model.
//
// The Adapter interface is the single contract every IM platform implements.
// It normalizes heterogeneous inbound events into InboundMessage and delivers
// normalized OutboundMessage replies back to the platform.
package channels

import "context"

// ChatType values for InboundMessage.ChatType.
const (
	ChatTypeSingle = "single"
	ChatTypeGroup  = "group"
)

// MsgType values for InboundMessage.MsgType.
const (
	MsgTypeText  = "text"
	MsgTypeImage = "image"
	MsgTypeFile  = "file"
	MsgTypeEvent = "event" // recall / failure / other platform events
)

// Outbound kind values for OutboundMessage.Kind.
const (
	KindText   = "text"
	KindStream = "stream"
	KindCard   = "card"
)

// Adapter is the unified IM adapter contract for WeCom and Feishu.
type Adapter interface {
	// Name returns a stable channel identifier (e.g. "wecom", "feishu").
	Name() string
	// Start blocks until ctx is done or an unrecoverable error happens.
	Start(ctx context.Context) error
	// Stop gracefully shuts the adapter down.
	Stop(ctx context.Context) error
	// Inbound returns the channel of normalized inbound messages.
	Inbound() <-chan *InboundMessage
	// Send pushes an outbound message (text/card/stream) to the IM platform.
	Send(ctx context.Context, msg *OutboundMessage) error
}

// Conn abstracts a live IM connection (WSS for WeCom, ws.Client for Feishu).
// The real implementation owns connection establishment and verification /
// decryption, then surfaces plaintext events via Recv. Tests use a mock that
// feeds canned events, so the adapters' normalization and dedup logic can be
// exercised without a live platform.
type Conn interface {
	// Recv blocks until the next plaintext event arrives, or returns an error
	// when the connection closes.
	Recv(ctx context.Context) ([]byte, error)
	// Send delivers a text reply to target (group chat id for group chats,
	// user id for single chats). chatType is ChatTypeSingle / ChatTypeGroup
	// and lets the conn pick the right receive-id type.
	Send(ctx context.Context, target, chatType, text string) error
	// SendCard delivers a rich card message to target.
	SendCard(ctx context.Context, target, chatType string, card Card) error
	// SendStream delivers a streamed reply to target.
	SendStream(ctx context.Context, target, chatType string, stream <-chan string) error
	// Close releases the underlying connection.
	Close() error
}

// InboundMessage is a normalized message from an IM platform.
type InboundMessage struct {
	PlatformMsgID string // original platform message id, for dedup
	TenantID      string
	AgentID       string
	SessionID     string
	UserID        string
	ChatType      string // single | group
	ChatID        string
	Content       string
	MsgType       string // text | image | file | event
}

// OutboundMessage is a normalized reply (text / stream / card).
type OutboundMessage struct {
	Inbound  *InboundMessage
	Kind     string // text | stream | card
	Segments []Segment
	// Stream is set when Kind == stream; consumer drains the channel.
	Stream <-chan string
}

// Segment is one piece of a structured outbound message.
type Segment struct {
	Type string // text | image | file
	Text string
	URL  string
}

// Card represents a rich card message payload.
type Card struct {
	Title   string // card title
	Content string // markdown or JSON content
}

// Text returns the concatenated plain text of the message's segments.
func (m *OutboundMessage) Text() string {
	var b []byte
	for _, s := range m.Segments {
		b = append(b, s.Text...)
	}
	return string(b)
}

// BuildSessionID derives the stable, tenant-scoped session id for an IM
// conversation. It embeds tenant + channel + conversation object so that
// sessions across tenants, channels, and chat types can never collide.
//
// Rules (see detailed design §8.2):
//   - single chat: {tenant}:{channel}:user:{openID}
//   - group chat:  {tenant}:{channel}:group:{chatID}
func BuildSessionID(tenantID, channel, chatType, id string) string {
	if chatType == ChatTypeGroup {
		return tenantID + ":" + channel + ":group:" + id
	}
	return tenantID + ":" + channel + ":user:" + id
}
