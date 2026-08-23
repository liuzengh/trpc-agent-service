package wecom

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/DocJlm/trpc-agent-service/trpcservice/channels"
	"github.com/go-sphere/wecom-aibot-go-sdk/aibot"
)

type testProvider struct {
	values []string
	err    error
}

func (p testProvider) Resolve(context.Context, string) ([]string, error) { return p.values, p.err }

func TestNormalizeTextOfficialFrameShape(t *testing.T) {
	body := map[string]any{
		"msgid": "msg-1", "aibotid": "bot-1", "chatid": "chat-1",
		"chattype": "group", "from": map[string]string{"userid": "user-1"},
		"create_time": int64(1700000000), "msgtype": "text",
		"text": map[string]string{"content": "hello"},
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	message, err := NormalizeText("tenant-1", "binding-1", &aibot.WsFrame{
		Headers: aibot.WsFrameHeaders{ReqID: "request-1"}, Body: raw,
	})
	if err != nil {
		t.Fatalf("NormalizeText: %v", err)
	}
	if message.Channel != ChannelName || message.ConversationType != channels.ConversationGroup {
		t.Fatalf("unexpected normalized channel: %+v", message)
	}
	if message.ExternalMessageID != "msg-1" || message.ExternalConversationID != "chat-1" || message.ReplyToken != "request-1" {
		t.Fatalf("unexpected identifiers: %+v", message)
	}
}

func TestNormalizeTextRejectsInvalidFrame(t *testing.T) {
	if _, err := NormalizeText("tenant", "binding", nil); err == nil {
		t.Fatal("nil frame should fail")
	}
}

func TestAdapterValidationAndHealth(t *testing.T) {
	adapter := New("tenant", "binding", "credential", nil, nil)
	if adapter.ID() != "binding" || adapter.Health().State != "created" {
		t.Fatalf("adapter=%+v health=%+v", adapter, adapter.Health())
	}
	if err := adapter.Run(context.Background()); err == nil {
		t.Fatal("missing dependencies should fail")
	}
	adapter = New("tenant", "binding", "credential", testProvider{values: []string{"only-one"}}, func(context.Context, channels.InboundEnvelope) error { return nil })
	if err := adapter.Run(context.Background()); err == nil {
		t.Fatal("invalid credential count should fail")
	}
	adapter = New("tenant", "binding", "credential", testProvider{err: errors.New("unavailable")}, func(context.Context, channels.InboundEnvelope) error { return nil })
	if err := adapter.Run(context.Background()); err == nil || adapter.Health().State != "credential_error" {
		t.Fatalf("credential error=%v health=%+v", err, adapter.Health())
	}
	adapter = New("tenant", "binding", "credential", testProvider{values: []string{"bot", "secret"}}, func(context.Context, channels.InboundEnvelope) error { return nil })
	t.Setenv("WECOM_CREDENTIAL_ORDER", "invalid")
	if err := adapter.Run(context.Background()); err == nil {
		t.Fatal("invalid credential order should fail")
	}
	if err := adapter.Send(context.Background(), channels.OutboundEnvelope{}); err == nil {
		t.Fatal("wrong outbound binding should fail")
	}
	if err := adapter.Send(context.Background(), channels.OutboundEnvelope{BindingID: "binding", Channel: ChannelName}); err == nil {
		t.Fatal("send before start should fail")
	}
}
