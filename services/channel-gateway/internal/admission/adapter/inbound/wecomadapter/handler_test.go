package wecomadapter_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	wire "github.com/liuzengh/trpc-agent-service/api/events/execution/v1"
	"github.com/liuzengh/trpc-agent-service/platform/im/wecom"
	adapter "github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/adapter/inbound/wecomadapter"
	"github.com/liuzengh/trpc-agent-service/services/channel-gateway/internal/admission/domain"
)

type capture struct {
	inputs []domain.Inbound
	err    error
}

func (c *capture) AcceptInbound(_ context.Context, in domain.Inbound) (domain.Receipt, error) {
	c.inputs = append(c.inputs, in)
	return domain.Receipt{Decision: "ignore"}, c.err
}
func fence() domain.ConnectionFence {
	return domain.ConnectionFence{InstanceID: "instance-1", Epoch: 7, Revision: 3}
}
func event() wecom.Event {
	return wecom.Event{Kind: wecom.EventText, MessageID: "message-1", BotID: "bot-1", RequestID: "request-1", Generation: 1, ChatType: "single", SenderID: "user-1", Text: "你好\n keep exact whitespace. ", BodyDigest: strings.Repeat("a", 64)}
}

func TestTextUsesStableMessageAndSchemaReplyAddress(t *testing.T) {
	for _, chatType := range []string{"single", "group"} {
		t.Run(chatType, func(t *testing.T) {
			dst := &capture{}
			h, err := adapter.NewHandler("account-1", "bot-1", fence(), dst, adapter.RetryPolicy{MaxAttempts: 1})
			if err != nil {
				t.Fatal(err)
			}
			e := event()
			e.ChatType = chatType
			wantConversation := "user-1"
			if chatType == "group" {
				e.ChatID = "group-1"
				wantConversation = "group-1"
			}
			if err = h.Handle(context.Background(), e); err != nil {
				t.Fatal(err)
			}
			if len(dst.inputs) != 1 {
				t.Fatalf("inputs=%d", len(dst.inputs))
			}
			in := dst.inputs[0]
			if in.Key != (domain.EventKey{Provider: "wecom", AccountID: "account-1", EventID: "message-1"}) || in.Kind != "text" || in.ConversationID != wantConversation || in.SenderID != "user-1" || in.Text != e.Text {
				t.Fatalf("normalized input=%+v", in)
			}
			if in.ConnectionFence == nil || *in.ConnectionFence != fence() {
				t.Fatal("trusted fence missing")
			}
			reply, err := wire.DecodeReplyContext("wecom", in.ReplyContext)
			if err != nil {
				t.Fatal(err)
			}
			if reply.ChatType != chatType || reply.ChatIDOrUserID != wantConversation || reply.CallbackReqID != "request-1" || reply.ReceivedAt != in.ReceivedAt.Format("2006-01-02T15:04:05.999999999Z07:00") {
				t.Fatalf("reply=%+v", reply)
			}
			raw, err := json.Marshal(in)
			if err != nil {
				t.Fatal(err)
			}
			for _, forbidden := range []string{"ConnectionFence", "connection_fence", "instance-1", "Epoch", "Revision", "source_message_id"} {
				if strings.Contains(string(raw), forbidden) {
					t.Fatalf("local fence/invalid address leaked: %s", raw)
				}
			}
		})
	}
}

func TestSemanticDigestExcludesDeliveryAndOwnerMetadata(t *testing.T) {
	firstSink, secondSink := &capture{}, &capture{}
	first, _ := adapter.NewHandler("account-1", "bot-1", fence(), firstSink)
	otherFence := domain.ConnectionFence{InstanceID: "instance-2", Epoch: 100, Revision: 4}
	second, _ := adapter.NewHandler("account-1", "bot-1", otherFence, secondSink)
	e := event()
	if err := first.Handle(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	e.RequestID = "redelivery-req"
	e.Generation = 100
	if err := second.Handle(context.Background(), e); err != nil {
		t.Fatal(err)
	}
	a, b := firstSink.inputs[0], secondSink.inputs[0]
	if a.Key != b.Key || a.SourceDigest != b.SourceDigest {
		t.Fatal("transport/owner change altered semantic identity")
	}
	if string(a.ReplyContext) == string(b.ReplyContext) || a.ConnectionFence.Epoch == b.ConnectionFence.Epoch {
		t.Fatal("fixture did not change local delivery metadata")
	}
	for name, mutate := range map[string]func(*wecom.Event){
		"unknown body": func(e *wecom.Event) { e.BodyDigest = strings.Repeat("b", 64) },
		"text":         func(e *wecom.Event) { e.Text = "changed" },
		"sender":       func(e *wecom.Event) { e.SenderID = "another-user" },
		"chat":         func(e *wecom.Event) { e.ChatType = "group"; e.ChatID = "group-1" },
	} {
		t.Run(name, func(t *testing.T) {
			altered := event()
			mutate(&altered)
			if err := second.Handle(context.Background(), altered); err != nil {
				t.Fatal(err)
			}
			if got := secondSink.inputs[len(secondSink.inputs)-1]; got.Key != a.Key || got.SourceDigest == a.SourceDigest {
				t.Fatal("changed semantic body missed identity conflict")
			}
		})
	}
}

func TestNonTextNeverBecomesPromptAndReplacementStaysControl(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		kind                  wecom.EventKind
		eventType, text, want string
	}{
		{"whitespace", wecom.EventText, "", " \n\t ", "ignore"},
		{"button", wecom.EventNotice, "template_card_event", "untrusted prompt-looking value", "interaction"},
		{"feedback", wecom.EventNotice, "feedback_event", "", "interaction"},
		{"enter", wecom.EventNotice, "enter_chat", "", "interaction"},
		{"image", wecom.EventUnsupported, "image", "never prompt this", "ignore"},
		{"future event", wecom.EventUnsupported, "future_event", "", "ignore"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dst := &capture{}
			h, _ := adapter.NewHandler("account-1", "bot-1", fence(), dst)
			e := event()
			e.Kind = tc.kind
			e.EventType = tc.eventType
			e.Text = tc.text
			if err := h.Handle(context.Background(), e); err != nil {
				t.Fatal(err)
			}
			if len(dst.inputs) != 1 || dst.inputs[0].Kind != tc.want || dst.inputs[0].Text != "" {
				t.Fatalf("nontext prompted: %+v", dst.inputs)
			}
		})
	}
	dst := &capture{}
	h, _ := adapter.NewHandler("account-1", "bot-1", fence(), dst)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := h.Handle(ctx, wecom.Event{Kind: wecom.EventNotice, BotID: "bot-1", EventType: "disconnected_event"}); err != nil {
		t.Fatal(err)
	}
	if len(dst.inputs) != 0 {
		t.Fatal("replacement entered Admission")
	}
}

func TestInvalidCallbacksNeverReachAdmission(t *testing.T) {
	for name, mutate := range map[string]func(*wecom.Event){
		"wrong bot":           func(e *wecom.Event) { e.BotID = "other" },
		"zero generation":     func(e *wecom.Event) { e.Generation = 0 },
		"empty message":       func(e *wecom.Event) { e.MessageID = "" },
		"long message":        func(e *wecom.Event) { e.MessageID = strings.Repeat("a", 257) },
		"long callback":       func(e *wecom.Event) { e.RequestID = strings.Repeat("a", 257) },
		"long sender":         func(e *wecom.Event) { e.SenderID = strings.Repeat("a", 257) },
		"long group":          func(e *wecom.Event) { e.ChatType = "group"; e.ChatID = strings.Repeat("a", 257) },
		"missing group":       func(e *wecom.Event) { e.ChatType = "group" },
		"unknown chat":        func(e *wecom.Event) { e.ChatType = "channel" },
		"invalid utf8":        func(e *wecom.Event) { e.Text = string([]byte{0xff}) },
		"huge text":           func(e *wecom.Event) { e.Text = strings.Repeat("a", 65537) },
		"bad message":         func(e *wecom.Event) { e.MessageID = "bad\nidentity" },
		"bad callback":        func(e *wecom.Event) { e.RequestID = "bad identity" },
		"missing body digest": func(e *wecom.Event) { e.BodyDigest = "" },
		"uppercase digest":    func(e *wecom.Event) { e.BodyDigest = strings.Repeat("A", 64) },
		"unknown kind":        func(e *wecom.Event) { e.Kind = "invented" },
	} {
		t.Run(name, func(t *testing.T) {
			dst := &capture{}
			h, _ := adapter.NewHandler("account-1", "bot-1", fence(), dst)
			e := event()
			mutate(&e)
			if err := h.Handle(context.Background(), e); !errors.Is(err, domain.ErrInvalidInput) {
				t.Fatalf("invalid callback accepted: %v", err)
			}
			if len(dst.inputs) != 0 {
				t.Fatal("invalid callback reached Admission")
			}
		})
	}
}

func TestConfigurationAndAcceptorFailure(t *testing.T) {
	dst := &capture{err: domain.ErrUnavailable}
	for _, tc := range []struct {
		account, bot string
		f            domain.ConnectionFence
		acceptor     adapter.Acceptor
	}{
		{"bad account", "bot-1", fence(), dst}, {"account-1", "", fence(), dst}, {"account-1", "bot-1", domain.ConnectionFence{}, dst}, {"account-1", "bot-1", fence(), nil},
	} {
		if _, err := adapter.NewHandler(tc.account, tc.bot, tc.f, tc.acceptor); !errors.Is(err, domain.ErrInvalidInput) {
			t.Fatal("bad configuration accepted")
		}
	}
	h, err := adapter.NewHandler("account-1", "bot-1", fence(), dst, adapter.RetryPolicy{MaxAttempts: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = h.Handle(context.Background(), event()); !errors.Is(err, domain.ErrUnavailable) {
		t.Fatal("durability failure hidden")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err = h.Handle(ctx, event()); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled callback admitted")
	}
	if len(dst.inputs) != 1 {
		t.Fatal("canceled callback reached Admission")
	}
}

func TestNoticeWithoutChatDoesNotInventReplyAddress(t *testing.T) {
	for _, tc := range []struct {
		kind            wecom.EventKind
		eventType, want string
	}{
		{wecom.EventNotice, "enter_chat", "interaction"},
		{wecom.EventNotice, "template_card_event", "interaction"},
		{wecom.EventNotice, "feedback_event", "interaction"},
		{wecom.EventUnsupported, "future_event", "ignore"},
	} {
		t.Run(tc.eventType, func(t *testing.T) {
			dst := &capture{}
			h, _ := adapter.NewHandler("account-1", "bot-1", fence(), dst)
			e := event()
			e.Kind = tc.kind
			e.EventType = tc.eventType
			e.ChatType = ""
			e.ChatID = ""
			e.Text = ""
			if err := h.Handle(context.Background(), e); err != nil {
				t.Fatalf("valid chatless callback rejected: %v", err)
			}
			if len(dst.inputs) != 1 {
				t.Fatal("notice was not recorded")
			}
			in := dst.inputs[0]
			if in.Kind != tc.want || in.Text != "" || in.ConversationID != "" || len(in.ReplyContext) != 0 || in.SenderID != "user-1" || in.ConnectionFence == nil {
				t.Fatalf("invented address or lost audit fields: %+v", in)
			}
		})
	}
}
