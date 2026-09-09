package wecom

import (
	"encoding/base64"
	"errors"
	"strings"
	"testing"
	"time"
)

func testBinding(t *testing.T) Binding {
	t.Helper()
	key := strings.TrimSuffix(base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef")), "=")
	crypt, err := NewCrypt("token", key, "ww-corp")
	if err != nil {
		t.Fatal(err)
	}
	return Binding{TenantID: "tenant-a", AppID: "assistant", BindingID: "wecom-a", CorpID: "ww-corp", AgentID: "1000002", ConfigVersion: 3, Crypt: crypt}
}

func TestNormalizeMediaAndGroupScope(t *testing.T) {
	message := CallbackMessage{ToUserName: "ww-corp", FromUserName: "alice", CreateTime: 100, MsgType: "file", MsgID: "message-1", AgentID: "1000002", ChatID: "room-1", FileName: "report.pdf", FileSize: 42, MediaID: "media-1"}
	inbound, err := normalize(testBinding(t), message, time.Unix(200, 0))
	if err != nil {
		t.Fatal(err)
	}
	if inbound.UserID != "wecom/wecom-a/alice" || inbound.SessionID != "group/wecom-a/room-1" || inbound.ConversationID != "room-1" {
		t.Fatalf("identity=%+v", inbound)
	}
	if inbound.Text != "[WeCom file: report.pdf 42 bytes]" {
		t.Fatalf("text=%q", inbound.Text)
	}
	if inbound.Media == nil || inbound.Media.Kind != "file" || inbound.Media.Key != "media-1" || strings.Contains(inbound.Text, "media-1") {
		t.Fatalf("media reference was not isolated from text: %+v", inbound)
	}
	if inbound.TraceID == "" || !inbound.ReceivedAt.Equal(time.Unix(200, 0).UTC()) {
		t.Fatalf("trace/time=%+v", inbound)
	}
}

func TestNormalizeRejectsCrossBindingAndStabilizesEventID(t *testing.T) {
	binding := testBinding(t)
	if _, err := normalize(binding, CallbackMessage{ToUserName: "other", FromUserName: "alice", MsgType: "text", Content: "hello"}, time.Now()); !errors.Is(err, ErrBindingMismatch) {
		t.Fatalf("binding error=%v", err)
	}
	event := CallbackMessage{ToUserName: "ww-corp", FromUserName: "alice", CreateTime: 100, MsgType: "event", Event: "enter_agent", EventKey: "key", AgentID: "1000002"}
	if eventMessageID(event) != eventMessageID(event) {
		t.Fatal("event message ID is not stable")
	}
	if _, err := normalize(binding, event, time.Now()); !errors.Is(err, ErrUnsupportedMessage) {
		t.Fatalf("event error=%v", err)
	}
}
