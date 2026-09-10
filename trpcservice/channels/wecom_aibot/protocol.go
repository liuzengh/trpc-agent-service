package wecom_aibot

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	cmdSubscribe     = "aibot_subscribe"
	cmdPing          = "ping"
	cmdCallback      = "aibot_msg_callback"
	cmdEventCallback = "aibot_event_callback"
	cmdRespond       = "aibot_respond_msg"
)

var (
	// ErrInvalid reports an invalid AI Bot configuration or constructor input.
	ErrInvalid = errors.New("invalid wecom ai bot configuration")
	// ErrMalformedFrame reports a frame that does not match the AI Bot protocol.
	ErrMalformedFrame = errors.New("malformed wecom ai bot frame")
	// ErrAuthentication reports a rejected AI Bot connection authentication.
	ErrAuthentication = errors.New("wecom ai bot authentication failed")
	// ErrQueueFull reports that an outbound frame cannot be buffered safely.
	ErrQueueFull = errors.New("wecom ai bot reply queue is full")
	// ErrNotReady reports that a connection has not completed authentication.
	ErrNotReady = errors.New("wecom ai bot is not ready")
	// ErrAcknowledgementTimeout reports an outbound reply that WeCom did not acknowledge in time.
	ErrAcknowledgementTimeout = errors.New("wecom ai bot reply acknowledgement timed out")
	// ErrClosed reports that the manager or its active connection has closed.
	ErrClosed = errors.New("wecom ai bot is closed")
)

// Frame is one JSON envelope exchanged over the AI Bot WebSocket.
type Frame struct {
	Cmd     string          `json:"cmd,omitempty"`
	Headers Headers         `json:"headers"`
	Body    json.RawMessage `json:"body,omitempty"`
	ErrCode *int            `json:"errcode,omitempty"`
	ErrMsg  string          `json:"errmsg,omitempty"`
}

// Headers contains request-scoped protocol metadata.
type Headers struct {
	ReqID string `json:"req_id"`
}

// Message is the text message payload of an AI Bot callback.
type Message struct {
	MsgID    string `json:"msgid"`
	AIBotID  string `json:"aibotid"`
	ChatID   string `json:"chatid,omitempty"`
	ChatType string `json:"chattype"`
	From     struct {
		UserID string `json:"userid"`
	} `json:"from"`
	MsgType string `json:"msgtype"`
	Text    struct {
		Content string `json:"content"`
	} `json:"text"`
}

// Event is the event callback payload reserved by the AI Bot protocol.
type Event struct {
	MsgID    string `json:"msgid"`
	AIBotID  string `json:"aibotid"`
	ChatID   string `json:"chatid,omitempty"`
	ChatType string `json:"chattype,omitempty"`
	From     struct {
		UserID string `json:"userid"`
	} `json:"from"`
	MsgType string `json:"msgtype"`
	Event   struct {
		EventType string `json:"eventtype"`
	} `json:"event"`
}

// StreamReply is an incremental or final text reply sent to WeCom.
type StreamReply struct {
	MsgType string `json:"msgtype"`
	Stream  struct {
		ID      string `json:"id"`
		Finish  bool   `json:"finish"`
		Content string `json:"content,omitempty"`
	} `json:"stream"`
}

func decodeFrame(data []byte) (Frame, error) {
	if len(data) == 0 || len(data) > 1<<20 {
		return Frame{}, ErrMalformedFrame
	}
	var frame Frame
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&frame) != nil || strings.TrimSpace(frame.Headers.ReqID) == "" {
		return Frame{}, ErrMalformedFrame
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return Frame{}, ErrMalformedFrame
	}
	if frame.Cmd != "" && frame.Cmd != cmdSubscribe && frame.Cmd != cmdPing && frame.Cmd != cmdCallback && frame.Cmd != cmdEventCallback && frame.Cmd != cmdRespond {
		return Frame{}, ErrMalformedFrame
	}
	return frame, nil
}

func encodeFrame(frame Frame) ([]byte, error) {
	if strings.TrimSpace(frame.Headers.ReqID) == "" || len([]rune(frame.Headers.ReqID)) > 256 {
		return nil, fmt.Errorf("%w: req_id is required", ErrMalformedFrame)
	}
	return json.Marshal(frame)
}
