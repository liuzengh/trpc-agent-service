package feishu

import (
	"context"
	"errors"
	"testing"

	"github.com/DocJlm/trpc-agent-service/trpcservice/channels"
	larkevent "github.com/larksuite/oapi-sdk-go/v3/event"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

type testProvider struct {
	values []string
	err    error
}

func (p testProvider) Resolve(context.Context, string) ([]string, error) { return p.values, p.err }

func stringPointer(value string) *string { return &value }

func TestNormalizeMessage(t *testing.T) {
	event := &larkim.P2MessageReceiveV1{
		EventV2Base: &larkevent.EventV2Base{Header: &larkevent.EventHeader{EventID: "event-1"}},
		Event: &larkim.P2MessageReceiveV1Data{
			Sender: &larkim.EventSender{SenderId: &larkim.UserId{OpenId: stringPointer("open-1")}},
			Message: &larkim.EventMessage{
				MessageId: stringPointer("message-1"), ChatId: stringPointer("chat-1"),
				ChatType: stringPointer("group"), MessageType: stringPointer("text"),
				Content:  stringPointer(`{"text":"@_user_1 hello"}`),
				Mentions: []*larkim.MentionEvent{{Key: stringPointer("@_user_1"), Name: stringPointer("robot")}},
			},
		},
	}
	message, mentionAll, err := NormalizeMessage("tenant", "binding", event)
	if err != nil {
		t.Fatalf("NormalizeMessage: %v", err)
	}
	if mentionAll || !message.MentionedBot || message.Content != "hello" {
		t.Fatalf("unexpected mention normalization: %+v mentionAll=%v", message, mentionAll)
	}
	if message.ExternalMessageID != "event-1" || message.ReplyToken != "message-1" {
		t.Fatalf("unexpected ids: %+v", message)
	}
	if message.ConversationType != channels.ConversationGroup {
		t.Fatalf("conversation=%q", message.ConversationType)
	}
}

func TestNormalizeMessageRejectsMentionAll(t *testing.T) {
	event := &larkim.P2MessageReceiveV1{
		Event: &larkim.P2MessageReceiveV1Data{
			Sender: &larkim.EventSender{SenderId: &larkim.UserId{OpenId: stringPointer("open-1")}},
			Message: &larkim.EventMessage{
				MessageId: stringPointer("message-1"), ChatId: stringPointer("chat-1"),
				ChatType: stringPointer("group"), MessageType: stringPointer("text"),
				Content:  stringPointer(`{"text":"@_all hello"}`),
				Mentions: []*larkim.MentionEvent{{Key: stringPointer("@_all"), Name: stringPointer("所有人")}},
			},
		},
	}
	message, mentionAll, err := NormalizeMessage("tenant", "binding", event)
	if err != nil {
		t.Fatal(err)
	}
	if !mentionAll || channels.ShouldHandleGroup(message.ConversationType, message.MentionedBot, mentionAll) {
		t.Fatal("@all group message should be ignored")
	}
}

func TestAdapterValidationAndHealth(t *testing.T) {
	adapter := New("tenant", "binding", "credential", nil, nil)
	if adapter.ID() != "binding" || adapter.Health().State != "created" {
		t.Fatalf("health=%+v", adapter.Health())
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
	if err := adapter.Send(context.Background(), channels.OutboundEnvelope{}); err == nil {
		t.Fatal("wrong outbound binding should fail")
	}
	if err := adapter.Send(context.Background(), channels.OutboundEnvelope{BindingID: "binding", Channel: ChannelName, ReplyToMessageID: "message"}); err == nil {
		t.Fatal("send before start should fail")
	}
}

func TestNormalizeMessageRejectsMalformedEvents(t *testing.T) {
	if _, _, err := NormalizeMessage("tenant", "binding", nil); err == nil {
		t.Fatal("nil event should fail")
	}
	event := &larkim.P2MessageReceiveV1{Event: &larkim.P2MessageReceiveV1Data{
		Sender:  &larkim.EventSender{SenderId: &larkim.UserId{OpenId: stringPointer("user")}},
		Message: &larkim.EventMessage{MessageId: stringPointer("message"), MessageType: stringPointer("image"), Content: stringPointer(`{}`)},
	}}
	if _, _, err := NormalizeMessage("tenant", "binding", event); err == nil {
		t.Fatal("unsupported message should fail")
	}
}
