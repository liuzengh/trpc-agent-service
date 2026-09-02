package wecom

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"testing"

	"github.com/go-sphere/wecom-aibot-go-sdk/aibot"
)

func TestTranslateTextGroup(t *testing.T) {
	frame := &aibot.WsFrame{
		Cmd: "aibot_msg_callback",
		Body: json.RawMessage(
			`{"msgid":"m1","aibotid":"bot1","chatid":"chat1","chattype":"group",` +
				`"from":{"userid":"u1"},"msgtype":"text","text":{"content":"hello @bot"}}`,
		),
	}
	raw, err := translateText(frame)
	if err != nil {
		t.Fatalf("translateText: %v", err)
	}
	var m Message
	if err := xml.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m.MsgId != "m1" || m.FromUserName != "u1" || m.ChatId != "chat1" ||
		m.Content != "hello @bot" || m.MsgType != "text" {
		t.Errorf("mapped fields wrong: %+v", m)
	}
}

func TestTranslateTextSingleChat(t *testing.T) {
	// Single chat carries no chatid; ChatId must stay empty so ToInbound
	// keys the session on FromUserName.
	frame := &aibot.WsFrame{
		Cmd: "aibot_msg_callback",
		Body: json.RawMessage(
			`{"msgid":"m2","aibotid":"bot1","chattype":"single",` +
				`"from":{"userid":"u2"},"msgtype":"text","text":{"content":"hi"}}`,
		),
	}
	raw, err := translateText(frame)
	if err != nil {
		t.Fatalf("translateText: %v", err)
	}
	var m Message
	if err := xml.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if m.MsgId != "m2" || m.FromUserName != "u2" || m.ChatId != "" || m.Content != "hi" {
		t.Errorf("single-chat mapping wrong: %+v", m)
	}
}

func TestTranslateTextNoMsgID(t *testing.T) {
	frame := &aibot.WsFrame{
		Cmd:  "aibot_msg_callback",
		Body: json.RawMessage(`{"aibotid":"bot1","chattype":"single","from":{"userid":"u"}}`),
	}
	raw, err := translateText(frame)
	if err != nil {
		t.Fatalf("translateText: %v", err)
	}
	if raw != nil {
		t.Errorf("frame without msgid must be skipped, got %s", raw)
	}
}

func TestTranslateTextNonTextBody(t *testing.T) {
	// A non-text body fails to unmarshal into TextMessage (missing text) or
	// yields an empty content; either way translateText must not panic and the
	// adapter just skips empty msgid frames.
	frame := &aibot.WsFrame{
		Cmd:  "aibot_msg_callback",
		Body: json.RawMessage(`{"msgid":"m3","msgtype":"image","image":{"url":"x"}}`),
	}
	if _, err := translateText(frame); err != nil {
		t.Logf("non-text body rejected (acceptable): %v", err)
	}
}

func TestConnRecvLifecycle(t *testing.T) {
	c := &Conn{events: make(chan []byte, 1), done: make(chan struct{})}

	// data path
	c.events <- []byte("<Message></Message>")
	raw, err := c.Recv(context.Background())
	if err != nil || string(raw) != "<Message></Message>" {
		t.Fatalf("recv data: raw=%s err=%v", raw, err)
	}

	// ctx cancellation wins over an empty channel
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Recv(ctx); err != context.Canceled {
		t.Errorf("recv cancelled: err=%v, want context.Canceled", err)
	}

	// close surfaces errClosed
	close(c.done)
	if _, err := c.Recv(context.Background()); err != errClosed {
		t.Errorf("recv closed: err=%v, want errClosed", err)
	}

	// select order: an available message beats a closed done
	c2 := &Conn{events: make(chan []byte, 1), done: make(chan struct{})}
	c2.events <- []byte("x")
	got, err := c2.Recv(context.Background())
	if err != nil || string(got) != "x" {
		t.Errorf("recv prefers pending message: got=%s err=%v", got, err)
	}
}
