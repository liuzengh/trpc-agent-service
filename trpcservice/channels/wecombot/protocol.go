// Package wecombot implements the WeCom AI-bot WebSocket long-connection
// channel (智能机器人长连接), per:
// https://developer.work.weixin.qq.com/document/path/101463
//
// This is the required WeCom IM (智能机器人长连接). Package wecom is the
// self-built app callback and is not one of the two real IM acceptance paths.
package wecombot

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
)

// WebSocket frame commands defined by the long-connection protocol.
const (
	cmdSubscribe   = "aibot_subscribe"
	cmdPing        = "ping"
	cmdMsgCallback = "aibot_msg_callback"
	cmdRespondMsg  = "aibot_respond_msg"
)

// inboundFrame is one server-pushed JSON frame. Request acknowledgements only
// carry headers plus errcode/errmsg; callbacks additionally carry cmd and body.
type inboundFrame struct {
	Cmd     string `json:"cmd"`
	ReqID   string `json:"-"`
	ErrCode int    `json:"errcode"`
	ErrMsg  string `json:"errmsg"`
	Body    frameBody
}

// frameBody holds every optional body field used by inbound frames.
type frameBody struct {
	MsgID    string `json:"msgid"`
	AIBotID  string `json:"aibotid"`
	ChatID   string `json:"chatid"`
	ChatType string `json:"chattype"`
	MsgType  string `json:"msgtype"`
	Text     struct {
		Content string `json:"content"`
	} `json:"text"`
	From struct {
		UserID string `json:"userid"`
	} `json:"from"`
	Event struct {
		EventType string `json:"eventtype"`
	} `json:"event"`
}

// UnmarshalJSON decodes a protocol frame with req_id nested under headers.
func (f *inboundFrame) UnmarshalJSON(data []byte) error {
	var raw struct {
		Cmd     string `json:"cmd"`
		Headers struct {
			ReqID string `json:"req_id"`
		} `json:"headers"`
		ErrCode int    `json:"errcode"`
		ErrMsg  string `json:"errmsg"`
		Body    frameBody
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	*f = inboundFrame{
		Cmd:     raw.Cmd,
		ReqID:   raw.Headers.ReqID,
		ErrCode: raw.ErrCode,
		ErrMsg:  raw.ErrMsg,
		Body:    raw.Body,
	}
	return nil
}

// outboundFrame is one client-sent JSON frame; body shape depends on cmd and
// req_id is nested under headers as required by the protocol.
type outboundFrame struct {
	Cmd     string       `json:"cmd,omitempty"`
	Headers frameHeaders `json:"headers"`
	ErrCode int          `json:"errcode,omitempty"`
	ErrMsg  string       `json:"errmsg,omitempty"`
	Body    any          `json:"body,omitempty"`
}

type frameHeaders struct {
	ReqID string `json:"req_id"`
}

// subscribeBody authenticates one bot after the connection is established.
type subscribeBody struct {
	BotID  string `json:"bot_id"`
	Secret string `json:"secret"`
}

// streamBody carries one streaming reply segment. All replies for one
// callback must transmit the callback req_id; the stream id correlates the
// updates of a single streamed message and finish ends it.
type streamBody struct {
	MsgType string        `json:"msgtype"`
	Stream  streamPayload `json:"stream"`
}

type streamPayload struct {
	ID      string `json:"id"`
	Finish  bool   `json:"finish"`
	Content string `json:"content"`
}

// subscribeFrame builds the aibot_subscribe authentication request.
func subscribeFrame(reqID, botID, secret string) outboundFrame {
	return outboundFrame{
		Cmd:     cmdSubscribe,
		Headers: frameHeaders{ReqID: reqID},
		Body:    subscribeBody{BotID: botID, Secret: secret},
	}
}

// streamFrame builds one aibot_respond_msg streaming reply.
func streamFrame(reqID, streamID, content string, finish bool) outboundFrame {
	return outboundFrame{
		Cmd:     cmdRespondMsg,
		Headers: frameHeaders{ReqID: reqID},
		Body: streamBody{
			MsgType: "stream",
			Stream: streamPayload{
				ID:      streamID,
				Finish:  finish,
				Content: content,
			},
		},
	}
}

// pingFrame builds one heartbeat request.
func pingFrame(reqID string) outboundFrame {
	return outboundFrame{Cmd: cmdPing, Headers: frameHeaders{ReqID: reqID}}
}

func encodeFrame(frame outboundFrame) ([]byte, error) {
	encoded, err := json.Marshal(frame)
	if err != nil {
		return nil, fmt.Errorf("encode wecombot frame: %w", err)
	}
	return encoded, nil
}

// newRequestID returns a random request identifier.
func newRequestID() (string, error) {
	raw := make([]byte, 12)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate wecombot request id: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

// errSubscribeRejected means the server refused the aibot_subscribe request.
var errSubscribeRejected = errors.New("wecombot subscribe rejected")
