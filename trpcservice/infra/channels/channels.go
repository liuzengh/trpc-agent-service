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
	// Media carries the non-text attachments of the message (see
	// MediaAttachment); empty for a plain text message.
	Media []MediaAttachment
}

// Media kinds. An unknown kind is reported as MediaFile so the user still gets a
// receipt instead of silence.
const (
	MediaImage = "image"
	MediaFile  = "file"
	MediaVoice = "voice"
)

// MediaAttachment is one non-text attachment of an inbound message. The adapter
// fills the descriptor (URL / FileKey); the bytes are fetched through
// MediaDownloader into Data so the agent can actually see the content instead of
// the platform silently dropping the message.
type MediaAttachment struct {
	Kind     string // image | file | voice
	Name     string
	MimeType string
	// URL is a direct download link (WeCom hands one out with every media message).
	URL string
	// AesKey decrypts a WeCom media download (WeCom encrypts media at rest).
	AesKey string
	// FileKey + MessageID identify a Feishu resource (that API needs both).
	FileKey   string
	MessageID string
	// Data is the fetched payload; nil means the fetch did not happen or failed.
	Data []byte
	// FetchError explains a missing payload (empty when fetched).
	FetchError string
}

// MediaDownloader is the optional capability of a Conn to fetch an inbound
// attachment's bytes (per-platform API, sometimes with decryption). A Conn that
// does not implement it leaves the attachment unfetched, and the platform then
// tells the user the attachment could not be read instead of ignoring it.
type MediaDownloader interface {
	DownloadMedia(ctx context.Context, att MediaAttachment) ([]byte, string, error)
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
	// Actions carries interactive buttons (see CardAction).
	Actions []CardAction
}

// CardAction is one button on an interactive card. Value is the payload the
// platform hands back when the button is pressed, so a decision (approve/deny)
// can arrive as a callback instead of a typed reply.
type CardAction struct {
	Text  string
	Value map[string]string
}

// Card represents a rich card message payload.
type Card struct {
	Title   string // card title
	Content string // markdown or JSON content
	Actions []CardAction
}

// CardActionCapable reports whether a channel can deliver interactive buttons
// and return their callbacks. WeCom's AI-bot long connection has no button
// callback in our adapter, so there the approval notice stays a text prompt and
// the user replies 批准/拒绝.
func CardActionCapable(channel string) bool {
	return channel == ChannelFeishu
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
