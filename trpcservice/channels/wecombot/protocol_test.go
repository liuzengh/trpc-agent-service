package wecombot

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestSubscribeFrameJSON(t *testing.T) {
	encoded, err := encodeFrame(subscribeFrame("req-1", "bot-1", "secret-1"))
	if err != nil {
		t.Fatalf("encodeFrame() error = %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	if decoded["cmd"] != "aibot_subscribe" {
		t.Errorf("cmd = %v, want aibot_subscribe", decoded["cmd"])
	}
	headers, ok := decoded["headers"].(map[string]any)
	if !ok || headers["req_id"] != "req-1" {
		t.Errorf("headers = %v, want req_id req-1", decoded["headers"])
	}
	body, ok := decoded["body"].(map[string]any)
	if !ok || body["bot_id"] != "bot-1" || body["secret"] != "secret-1" {
		t.Errorf("body = %v, want bot_id and secret", decoded["body"])
	}
}

func TestStreamFrameJSON(t *testing.T) {
	encoded, err := encodeFrame(streamFrame("req-9", "stream-1", "hello", true))
	if err != nil {
		t.Fatalf("encodeFrame() error = %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	if decoded["cmd"] != "aibot_respond_msg" {
		t.Errorf("cmd = %v, want aibot_respond_msg", decoded["cmd"])
	}
	body, _ := decoded["body"].(map[string]any)
	if body["msgtype"] != "stream" {
		t.Errorf("msgtype = %v, want stream", body["msgtype"])
	}
	stream, _ := body["stream"].(map[string]any)
	if stream["id"] != "stream-1" || stream["finish"] != true || stream["content"] != "hello" {
		t.Errorf("stream = %v, want id/finish/content", stream)
	}
	if _, exists := decoded["errcode"]; exists {
		t.Errorf("errcode must be omitted, got %v", decoded["errcode"])
	}
}

func TestInboundFrameUnmarshal(t *testing.T) {
	payload := `{
		"cmd": "aibot_msg_callback",
		"headers": {"req_id": "req-2"},
		"body": {
			"msgid": "msg-2",
			"aibotid": "bot-1",
			"chatid": "chat-1",
			"chattype": "group",
			"from": {"userid": "user-1"},
			"msgtype": "text",
			"text": {"content": "@RobotA hello"}
		}
	}`
	var frame inboundFrame
	if err := frame.UnmarshalJSON([]byte(payload)); err != nil {
		t.Fatalf("UnmarshalJSON() error = %v", err)
	}
	if frame.Cmd != "aibot_msg_callback" || frame.ReqID != "req-2" {
		t.Errorf("cmd/req_id = %s/%s", frame.Cmd, frame.ReqID)
	}
	if frame.Body.MsgID != "msg-2" || frame.Body.ChatType != "group" {
		t.Errorf("body = %+v", frame.Body)
	}
	if frame.Body.From.UserID != "user-1" || frame.Body.Text.Content != "@RobotA hello" {
		t.Errorf("body = %+v", frame.Body)
	}
}

func TestInboundAckFrameUnmarshal(t *testing.T) {
	payload := `{"headers": {"req_id": "req-3"}, "errcode": 0, "errmsg": "ok"}`
	var frame inboundFrame
	if err := frame.UnmarshalJSON([]byte(payload)); err != nil {
		t.Fatalf("UnmarshalJSON() error = %v", err)
	}
	if frame.ReqID != "req-3" || frame.ErrCode != 0 {
		t.Errorf("frame = %+v", frame)
	}
}

func TestPingFrameJSON(t *testing.T) {
	encoded, err := encodeFrame(pingFrame("req-4"))
	if err != nil {
		t.Fatalf("encodeFrame() error = %v", err)
	}
	if !strings.Contains(string(encoded), `"cmd":"ping"`) {
		t.Errorf("encoded = %s, want ping cmd", encoded)
	}
	if strings.Contains(string(encoded), `"body"`) {
		t.Errorf("encoded = %s, ping must not carry a body", encoded)
	}
}

func TestNewRequestIDUnique(t *testing.T) {
	first, err := newRequestID()
	if err != nil {
		t.Fatalf("newRequestID() error = %v", err)
	}
	second, err := newRequestID()
	if err != nil {
		t.Fatalf("newRequestID() error = %v", err)
	}
	if first == second || first == "" {
		t.Errorf("ids not unique: %q vs %q", first, second)
	}
}
