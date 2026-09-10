package feishu

import (
	"context"
	"testing"

	channeltypes "github.com/larksuite/channel-sdk-go/types"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

type fakeRuntimeChannel struct {
	handler    func(context.Context, *channeltypes.NormalizedMessage) error
	action     func(context.Context, *channeltypes.CardActionEvent) error
	message    *channeltypes.NormalizedMessage
	cardAction *channeltypes.CardActionEvent
	files      map[string][]byte
	lastSend   *channeltypes.SendInput
}

func (f *fakeRuntimeChannel) OnMessage(handler func(context.Context, *channeltypes.NormalizedMessage) error) {
	f.handler = handler
}

func (f *fakeRuntimeChannel) OnCardAction(handler func(context.Context, *channeltypes.CardActionEvent) error) {
	f.action = handler
}

func (f *fakeRuntimeChannel) DownloadFile(_ context.Context, key, _ string) ([]byte, error) {
	return f.files[key], nil
}

func (f *fakeRuntimeChannel) Send(_ context.Context, input *channeltypes.SendInput) (*channeltypes.SendResult, error) {
	f.lastSend = input
	return &channeltypes.SendResult{MessageID: "om-test"}, nil
}

func (*fakeRuntimeChannel) OnReady(func())        {}
func (*fakeRuntimeChannel) OnError(func(error))   {}
func (*fakeRuntimeChannel) OnReconnecting(func()) {}
func (*fakeRuntimeChannel) OnReconnected(func())  {}
func (*fakeRuntimeChannel) OnDisconnected(func()) {}

func (f *fakeRuntimeChannel) Start(ctx context.Context) error {
	if f.handler != nil && f.message != nil {
		if err := f.handler(ctx, f.message); err != nil {
			return err
		}
	}
	if f.action != nil && f.cardAction != nil {
		return f.action(ctx, f.cardAction)
	}
	return nil
}

func (f *fakeRuntimeChannel) Stop(context.Context) error { return nil }

func TestConnectorNormalizesDirectMessage(t *testing.T) {
	fake := &fakeRuntimeChannel{message: &channeltypes.NormalizedMessage{
		EventID: "evt-1", MessageID: "om-1", ChatID: "oc-1", ChatType: "p2p",
		UserID: "ou-user", Content: "你好", CreateTimeMs: 1710000000000,
	}}
	connector := newConnectorWithChannel(fake)
	var got channels.InboundMessage
	if err := connector.Run(context.Background(), func(_ context.Context, message channels.InboundMessage) error {
		got = message
		return nil
	}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got.Channel != channels.Feishu || got.MessageID != "om-1" || got.ProviderRequestID != "evt-1" || got.ConversationID != "oc-1" || got.SenderID != "ou-user" || got.Text != "你好" {
		t.Fatalf("unexpected normalized message: %+v", got)
	}
	if got.ConversationScope != channels.ConversationDirect {
		t.Fatalf("scope = %q, want direct", got.ConversationScope)
	}
	if got.TriggerType != channels.TriggerDirect {
		t.Fatalf("trigger = %q, want direct", got.TriggerType)
	}
}

func TestConnectorRequiresBotMentionInGroup(t *testing.T) {
	fake := &fakeRuntimeChannel{message: &channeltypes.NormalizedMessage{
		EventID: "evt-2", MessageID: "om-2", ChatID: "oc-group", ChatType: "group",
		UserID: "ou-user", Content: "@_bot @_alice 帮忙看一下", MentionedBot: false,
		Mentions: []channeltypes.Mention{
			{Key: "@_bot", Name: "机器人", IsBot: true},
			{Key: "@_alice", Name: "Alice"},
		},
	}}
	connector := newConnectorWithChannel(fake)
	called := false
	if err := connector.Run(context.Background(), func(context.Context, channels.InboundMessage) error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if called {
		t.Fatal("group message without bot mention must be ignored")
	}

	fake.message.MentionedBot = true
	if err := connector.Run(context.Background(), func(_ context.Context, message channels.InboundMessage) error {
		called = true
		if message.ConversationScope != channels.ConversationGroup {
			t.Fatalf("scope = %q, want group", message.ConversationScope)
		}
		if message.TriggerType != channels.TriggerMention {
			t.Fatalf("trigger = %q, want mention", message.TriggerType)
		}
		if message.Text != "Alice 帮忙看一下" {
			t.Fatalf("text = %q, want bot mention removed and human mention normalized", message.Text)
		}
		return nil
	}); err != nil {
		t.Fatalf("Run(mentioned) error = %v", err)
	}
	if !called {
		t.Fatal("group message mentioning bot must be delivered")
	}
}

func TestConnectorDownloadsFileResources(t *testing.T) {
	fake := &fakeRuntimeChannel{
		files: map[string][]byte{"file-key": []byte("document bytes")},
		message: &channeltypes.NormalizedMessage{
			EventID: "evt-file", MessageID: "om-file", ChatID: "oc-1", ChatType: "p2p", UserID: "ou-user",
			Resources: []channeltypes.Resource{{Type: "file", FileKey: "file-key", FileName: "report.pdf"}},
		},
	}
	connector := newConnectorWithChannel(fake)
	var got channels.InboundMessage
	if err := connector.Run(context.Background(), func(_ context.Context, message channels.InboundMessage) error {
		got = message
		return nil
	}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(got.ReceivedFiles) != 1 || got.ReceivedFiles[0].Name != "report.pdf" || string(got.ReceivedFiles[0].Data) != "document bytes" {
		t.Fatalf("received files = %#v", got.ReceivedFiles)
	}
}

func TestConnectorNormalizesCardAction(t *testing.T) {
	fake := &fakeRuntimeChannel{cardAction: &channeltypes.CardActionEvent{
		EventID: "evt-card", MessageID: "om-card", ChatID: "oc-group",
		Operator: channeltypes.CardActionOperator{OpenID: "ou-user"},
		Action: channeltypes.CardActionPayload{Value: map[string]any{
			"action_id": "approval:approve:token-1", "conversation_scope": "group",
		}},
	}}
	connector := newConnectorWithChannel(fake)
	var got channels.InboundMessage
	if err := connector.Run(context.Background(), func(_ context.Context, message channels.InboundMessage) error {
		got = message
		return nil
	}); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if got.TriggerType != channels.TriggerAction || got.Action == nil {
		t.Fatalf("card action = %#v, want normalized action", got)
	}
	if got.Action.ActionID != "approval:approve:token-1" || got.Action.OriginMessageID != "om-card" {
		t.Fatalf("card action = %#v", got.Action)
	}
	if got.ConversationScope != channels.ConversationGroup || got.SenderID != "ou-user" {
		t.Fatalf("card action route = %#v", got)
	}
}
