package wecomws

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// WeCom aibot WebSocket protocol commands.
const (
	cmdSubscribe     = "aibot_subscribe"      // client → platform credential handshake
	cmdPing          = "ping"                 // client → platform heartbeat
	cmdRespond       = "aibot_respond_msg"    // client → platform reply; headers.req_id must be the callback's req_id verbatim
	cmdMsgCallback   = "aibot_msg_callback"   // platform → client message callback
	cmdEventCallback = "aibot_event_callback" // platform → client event callback
)

// Inbound event types (eventCallback).
const (
	eventDisconnected = "disconnected_event" // this connection was kicked by a newer subscription
	eventEnterChat    = "enter_chat"
	eventTemplateCard = "template_card_event"
)

// envelope is the outer frame shared by every command: a cmd, the headers
// carrying the request id that correlates a response (or a reply) to its
// request (or callback), and the command-specific body.
//
// Cmd is empty on an ack: the platform answers aibot_subscribe with
// {"headers":...,"errcode":0,"errmsg":"ok"} and correlates it by req_id.
// ErrCode/ErrMsg are the ack's top-level result — command bodies nest their
// own errcode, so both shapes have to be read.
type envelope struct {
	Cmd     string          `json:"cmd,omitempty"`
	Headers frameHeaders    `json:"headers,omitempty"`
	ErrCode int             `json:"errcode,omitempty"`
	ErrMsg  string          `json:"errmsg,omitempty"`
	Body    json.RawMessage `json:"body,omitempty"`
}

type frameHeaders struct {
	ReqID string `json:"req_id,omitempty"`
}

// errcodeBody is the platform's ack shape (the subscribe response, among
// others).
type errcodeBody struct {
	ErrCode int    `json:"errcode"`
	ErrMsg  string `json:"errmsg"`
}

// subscribeFrame asks the platform to bind the connection to botID. The
// secret rides this first frame instead of the URL/handshake, keeping it out
// of access logs.
func subscribeFrame(botID, secret, reqID string) (envelope, error) {
	//nolint:gosec // G117: the secret is the subscribe credential, sent to the platform over wss by design
	body, err := json.Marshal(struct {
		BotID  string `json:"bot_id"`
		Secret string `json:"secret"`
	}{BotID: botID, Secret: secret})
	if err != nil {
		return envelope{}, err
	}
	return envelope{Cmd: cmdSubscribe, Headers: frameHeaders{ReqID: reqID}, Body: body}, nil
}

// pingFrame is one heartbeat, carrying a fresh req_id the pong must echo.
func pingFrame(reqID string) envelope {
	return envelope{Cmd: cmdPing, Headers: frameHeaders{ReqID: reqID}}
}

// respondFrame builds one aibot_respond_msg frame replying to one callback:
// the callback's req_id rides the headers verbatim — the platform correlates
// a reply to its question by it and rejects (or drops) mismatches. The
// platform rejects a plain "text" respond with errcode 40008 ("invalid
// message type") — the only text-shaped reply it accepts is a stream segment,
// so every reply rides msgtype "stream". Segments of one reply share
// streamID; only the last sets finish, which is what makes the platform
// render the message as complete.
func respondFrame(reqID, streamID, content string, finish bool) (envelope, error) {
	body, err := json.Marshal(map[string]any{
		"msgtype": "stream",
		"stream": map[string]any{
			"id":      streamID,
			"finish":  finish,
			"content": content,
		},
	})
	if err != nil {
		return envelope{}, err
	}
	return envelope{Cmd: cmdRespond, Headers: frameHeaders{ReqID: reqID}, Body: body}, nil
}

// msgCallback is the body of an inbound message callback.
type msgCallback struct {
	MsgID    string `json:"msgid"`
	AibotID  string `json:"aibot_id"`
	ChatID   string `json:"chatid"`   // group chats only; empty for direct chats
	ChatType string `json:"chattype"` // single / group
	From     struct {
		UserID string `json:"userid"`
	} `json:"from"`
	MsgType string `json:"msgtype"`
	Text    struct {
		Content string `json:"content"`
	} `json:"text"`
}

// mediaLabels names the media msgtypes for the downgrade placeholder.
var mediaLabels = map[string]string{
	"image": "图片",
	"voice": "语音",
	"video": "视频",
	"file":  "文件",
	"mixed": "混合消息",
}

// mediaPlaceholder is the text a media message degrades to (no inbound
// media download/decryption yet); unknown types fall back to their
// raw msgtype.
func mediaPlaceholder(msgType string) string {
	label, ok := mediaLabels[msgType]
	if !ok {
		label = msgType
	}
	return "[" + label + "]（暂不支持媒体消息）"
}

// eventCallback is the body of an inbound event frame. The eventtype rides
// either directly on the body or nested under "event"; both nestings are
// accepted.
type eventCallback struct {
	EventType string `json:"eventtype"`
	Event     struct {
		EventType string `json:"eventtype"`
	} `json:"event"`
}

func (e eventCallback) eventType() string {
	if e.EventType != "" {
		return e.EventType
	}
	return e.Event.EventType
}

// newReqID returns a random 16-byte hex ID for client-initiated frames
// (subscribe, ping).
func newReqID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("new req_id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// heartbeatReqID spells the heartbeat's req_id the way the platform expects,
// {cmd}_{unixmilli}_{random} with the cmd prefix: a heartbeat whose req_id
// does not start with "ping" is never answered at all — not even an error —
// so the connection dies on the client's own missed-pong watchdog. The
// subscribe ack, by contrast, echoes any req_id.
func heartbeatReqID() (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("heartbeat req_id: %w", err)
	}
	return fmt.Sprintf("ping_%d_%s", time.Now().UnixMilli(), hex.EncodeToString(b[:])), nil
}

// newStreamID names the stream one reply rides; the platform groups the
// frames of a reply by it, so uniqueness is the only requirement.
func newStreamID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("new stream id: %w", err)
	}
	return fmt.Sprintf("stream_%d_%s", time.Now().UnixMilli(), hex.EncodeToString(b[:])), nil
}

// scopedReplyToken couples the callback's req_id with the epoch of the
// connection that received it: "<epoch>:<req_id>". The platform correlates a
// reply by req_id on the connection that asked, so after a reconnect the old
// req_id is undeliverable — the token has to carry which connection earned it
// or a successor connection would accept the write while the platform drops
// the frame as uncorrelatable.
func scopedReplyToken(epoch uint64, reqID string) string {
	return strconv.FormatUint(epoch, 10) + ":" + reqID
}

// parseReplyToken splits a scoped reply token back into its epoch and req_id.
// An epoch of 0 means the token carries no epoch (a message in flight across
// a rolling deploy): it is sent as-is, since its owning connection cannot be
// identified anymore.
func parseReplyToken(token string) (epoch uint64, reqID string) {
	i := strings.IndexByte(token, ':')
	if i < 0 {
		return 0, token
	}
	e, err := strconv.ParseUint(token[:i], 10, 64)
	if err != nil {
		return 0, token
	}
	return e, token[i+1:]
}

// newEpoch returns a random non-zero connection epoch. It must be unique
// across processes — a new leader redials with fresh epochs, and a plain
// per-connection counter would collide with the predecessor's counter and
// bless a dead predecessor's tokens — so 64 random bits instead; zero stays
// reserved for "unscoped".
func newEpoch() (uint64, error) {
	var b [8]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			return 0, fmt.Errorf("new epoch: %w", err)
		}
		if e := binary.BigEndian.Uint64(b[:]); e != 0 {
			return e, nil
		}
	}
}
