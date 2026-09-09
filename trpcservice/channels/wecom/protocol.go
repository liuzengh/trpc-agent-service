package wecom

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// defaultEndpoint is the address this adapter dials, and the only one.
//
// It is a constant rather than a configuration field on purpose. An endpoint
// knob is a place to point the subscribe frame — which carries the bot Secret
// in cleartext inside the TLS session — at a server of someone else's
// choosing. Tests redirect the dial through an unexported seam instead, so
// nothing an operator or a caller can set moves the Secret off this host.
//
// Private deployments do use their own address; adding that later means adding
// an explicitly trusted, validated wss:// value, not a free-form string.
const defaultEndpoint = "wss://openws.work.weixin.qq.com"

// Protocol commands, developer to platform and back. The set is the whole of
// what this slice speaks: welcome messages, template cards, media, active
// sends and uploads are other commands and are not implemented.
const (
	cmdSubscribe     = "aibot_subscribe"
	cmdPing          = "ping"
	cmdRespond       = "aibot_respond_msg"
	cmdMsgCallback   = "aibot_msg_callback"
	cmdEventCallback = "aibot_event_callback"
)

// Frame field values this adapter accepts or sends.
const (
	chatTypeSingle    = "single"
	msgTypeText       = "text"
	msgTypeStream     = "stream"
	eventDisconnected = "disconnected_event"
)

const (
	// maxReplyTextBytes is the stream reply limit the official SDK documents.
	maxReplyTextBytes = 20480

	// maxInboundTextBytes bounds accepted inbound text. The platform's own
	// inbound limit is not documented, so this is this adapter's bound and is
	// not a claim about what the platform will send.
	maxInboundTextBytes = 20480

	// maxExternalIDBytes bounds every external identifier — bot id, msgid,
	// userid, req_id — before it is compared, hashed or carried.
	maxExternalIDBytes = 256

	// maxFrameBytes bounds one WebSocket message. Without it a hostile or
	// broken peer sizes this process's memory.
	maxFrameBytes = 1 << 20

	// maxMissedHeartbeats is how many unanswered pings end a connection.
	maxMissedHeartbeats = 2
)

// frame is the envelope every WebSocket message uses in both directions.
//
// errmsg is deliberately absent. The platform sends one, and it is the kind of
// free text that ends up concatenated into an error and then into a log; a
// field that is never decoded cannot be propagated by accident. errcode is a
// pointer because "explicitly zero" and "absent" are different answers: a
// receipt with no errcode is not a success, and decoding it into an int would
// make it look like one.
type frame struct {
	Cmd     string          `json:"cmd,omitempty"`
	Headers headers         `json:"headers"`
	Body    json.RawMessage `json:"body,omitempty"`
	ErrCode *int            `json:"errcode,omitempty"`
}

type headers struct {
	ReqID string `json:"req_id"`
}

// receipt is the answer to one frame this adapter sent.
//
// It keeps errcode as the platform sent it, including "not sent at all",
// because the three cases are three different facts and collapsing them loses
// the one that matters: an explicit zero is a success, an explicit non-zero is
// a refusal the platform stands behind, and a missing errcode is a receipt
// this adapter cannot read — which is neither.
type receipt struct {
	code *int
}

func (r receipt) ok() bool       { return r.code != nil && *r.code == 0 }
func (r receipt) refused() bool  { return r.code != nil && *r.code != 0 }
func (r receipt) readable() bool { return r.code != nil }

// subscribeBody authenticates the connection. The Secret appears here and
// nowhere else in the process: not in a struct that outlives the write, not in
// a String method, not in an error.
type subscribeBody struct {
	BotID  string `json:"bot_id"`
	Secret string `json:"secret"`
}

// respondBody is a final stream reply. finish is always true: this adapter
// sends one terminal answer per message and no incremental frames.
type respondBody struct {
	MsgType string      `json:"msgtype"`
	Stream  streamReply `json:"stream"`
}

type streamReply struct {
	ID      string `json:"id"`
	Content string `json:"content"`
	Finish  bool   `json:"finish"`
}

// callbackBody is the subset of an inbound message this adapter reads. Fields
// it does not implement — quote, media urls and aeskeys, response_url — are
// not decoded: a message carrying them is rejected by its msgtype before the
// question arises.
type callbackBody struct {
	MsgID    string `json:"msgid"`
	AIBotID  string `json:"aibotid"`
	ChatID   string `json:"chatid"`
	ChatType string `json:"chattype"`
	MsgType  string `json:"msgtype"`
	From     struct {
		UserID string `json:"userid"`
	} `json:"from"`
	Text *struct {
		Content string `json:"content"`
	} `json:"text"`
}

// eventBody is the subset of an event callback this adapter reads: only the
// event type, and only to recognize a takeover.
type eventBody struct {
	Event struct {
		EventType string `json:"eventtype"`
	} `json:"event"`
}

// newID returns a random hex identifier. It is the source of both req_ids and
// connection generations, so both are unguessable and neither repeats across
// process restarts.
func newID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("wecom: random source: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

// newReqID mints a request id for a frame this adapter originates.
//
// The command prefix follows the official SDK's convention, which is useful
// when reading a capture, but it is decoration: receipts are matched by exact
// req_id equality, never by prefix, so a server that echoes a differently
// shaped id matches nothing rather than matching the wrong waiter.
func newReqID(cmd string) (string, error) {
	id, err := newID()
	if err != nil {
		return "", err
	}
	return cmd + "-" + id, nil
}

// encodeFrame renders one outbound frame. body may be nil for commands that
// carry none, such as ping.
func encodeFrame(cmd, reqID string, body any) ([]byte, error) {
	out := frame{Cmd: cmd, Headers: headers{ReqID: reqID}}
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			// No error here repeats the body: a subscribe body is a Secret.
			return nil, fmt.Errorf("wecom: encode %s body", cmd)
		}
		out.Body = encoded
	}
	payload, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("wecom: encode %s frame", cmd)
	}
	return payload, nil
}

// isTakeover reports the platform's disconnected_event: a newer connection
// has subscribed with this bot's credential and this one is about to be cut.
func isTakeover(in frame) bool {
	var body eventBody
	if err := json.Unmarshal(in.Body, &body); err != nil {
		return false
	}
	return body.Event.EventType == eventDisconnected
}

// decodeFrame reads one WebSocket message as a frame. The error names nothing
// from the message: an unreadable frame is exactly where a body would be least
// welcome in a message someone might print.
func decodeFrame(data []byte) (frame, error) {
	var in frame
	if err := json.Unmarshal(data, &in); err != nil {
		return frame{}, fmt.Errorf("wecom: frame is not an object")
	}
	return in, nil
}

// normalizeDirectText turns one inbound callback into a DirectText, or refuses
// it.
//
// Everything this adapter supports has to be true at once: the frame addresses
// the bound bot, it is a single chat, it is text, and every identifier it
// carries is present, bounded and valid UTF-8. Anything else — a group, an
// image, a mixed message, an event, a message for another bot — is refused
// here rather than partially understood downstream.
//
// Tenant, app and binding come from b. The frame contributes content and
// external identifiers, never routing.
//
// Refusals name the field that failed and never its value: a rejected callback
// is exactly the case where the value is most likely to be something that must
// not be written down.
func normalizeDirectText(b Binding, in frame, generation string, now time.Time) (DirectText, error) {
	if err := checkExternalID("req_id", in.Headers.ReqID); err != nil {
		return DirectText{}, err
	}
	var body callbackBody
	if err := json.Unmarshal(in.Body, &body); err != nil {
		return DirectText{}, fmt.Errorf("%w: body is not an object", errRejected)
	}
	// The bot check is first among the body rules. A frame for another bot is
	// not this binding's message, so nothing else about it is worth reading.
	if body.AIBotID != b.BotID {
		return DirectText{}, fmt.Errorf("%w: aibotid is not the bound bot", errRejected)
	}
	if body.ChatType != chatTypeSingle {
		return DirectText{}, fmt.Errorf("%w: chattype is not %q", errRejected, chatTypeSingle)
	}
	// A single chat carries no chatid. One that does is contradicting itself,
	// and the safe reading of a contradiction about whether a room is private
	// is to refuse it.
	if body.ChatID != "" {
		return DirectText{}, fmt.Errorf("%w: single chat carries a chatid", errRejected)
	}
	if body.MsgType != msgTypeText {
		return DirectText{}, fmt.Errorf("%w: msgtype is not %q", errRejected, msgTypeText)
	}
	if body.Text == nil {
		return DirectText{}, fmt.Errorf("%w: text is missing", errRejected)
	}
	text := body.Text.Content
	if strings.TrimSpace(text) == "" {
		return DirectText{}, fmt.Errorf("%w: text is blank", errRejected)
	}
	if len(text) > maxInboundTextBytes {
		return DirectText{}, fmt.Errorf("%w: text is longer than %d bytes",
			errRejected, maxInboundTextBytes)
	}
	if !utf8.ValidString(text) {
		return DirectText{}, fmt.Errorf("%w: text is not valid UTF-8", errRejected)
	}
	if err := checkExternalID("msgid", body.MsgID); err != nil {
		return DirectText{}, err
	}
	if err := checkExternalID("from.userid", body.From.UserID); err != nil {
		return DirectText{}, err
	}
	principal := principalID(b.TenantID, b.BindingID, body.From.UserID)
	return DirectText{
		TenantID:          b.TenantID,
		AgentAppID:        b.AgentAppID,
		BindingID:         b.BindingID,
		PrincipalID:       principal,
		SessionID:         directSessionID(b.TenantID, b.AgentAppID, b.BindingID, principal),
		ExternalMessageID: body.MsgID,
		Text:              text,
		ReceivedAt:        now,
		Reply: ReplyTarget{
			generation: generation,
			reqID:      in.Headers.ReqID,
			streamID:   streamID(b.TenantID, b.BindingID, body.MsgID),
		},
	}, nil
}

// checkExternalID rejects an identifier that is empty, oversized or not valid
// UTF-8.
//
// The UTF-8 rule guards digest, whose inputs must survive JSON encoding
// unchanged: invalid bytes become U+FFFD, and two distinct malformed user ids
// would then hash to one principal and share one session. On the inbound path
// encoding/json has already substituted U+FFFD while decoding, so this check
// cannot see that case and does not claim to; it holds the line for any value
// that reaches a digest without passing through encoding/json first.
func checkExternalID(field, value string) error {
	switch {
	case value == "":
		return fmt.Errorf("%w: %s is empty", errRejected, field)
	case len(value) > maxExternalIDBytes:
		return fmt.Errorf("%w: %s is longer than %d bytes", errRejected, field, maxExternalIDBytes)
	case !utf8.ValidString(value):
		return fmt.Errorf("%w: %s is not valid UTF-8", errRejected, field)
	}
	return nil
}

// validateReplyText rejects text this adapter will not put on the wire, before
// anything is written.
//
// Oversize text is refused, never truncated: cutting 20480 bytes out of a
// longer answer either splits a rune or silently ships a different answer than
// the one the caller asked to send.
func validateReplyText(text string) error {
	if len(text) > maxReplyTextBytes {
		return ErrTextTooLong
	}
	if strings.TrimSpace(text) == "" || !utf8.ValidString(text) {
		return ErrTextInvalid
	}
	return nil
}
