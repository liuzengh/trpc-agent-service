package channels

import (
	"context"
	"errors"
	"testing"
)

func TestInboundMessageGroupClassificationAndSubjects(t *testing.T) {
	t.Parallel()
	if (InboundMessage{ConversationScope: ConversationDirect}).IsGroupConversation() {
		t.Fatal("direct message classified as group")
	}
	if !(InboundMessage{ConversationScope: ConversationGroup}).IsGroupConversation() {
		t.Fatal("group message not classified as group")
	}

	group, err := GroupSubjectID(Feishu, " support-bot ", " group-42 ")
	if err != nil || group != "group:feishu:support-bot:group-42" {
		t.Fatalf("GroupSubjectID() = %q, %v", group, err)
	}
	external, err := ExternalSubjectID(Telegram, "support-bot", "customer-1")
	if err != nil || external != "external:telegram:support-bot:customer-1" {
		t.Fatalf("ExternalSubjectID() = %q, %v", external, err)
	}
	for _, test := range []struct {
		name         string
		channel      Channel
		bindingID    string
		conversation string
	}{
		{name: "web", channel: Web, bindingID: "web", conversation: "chat"},
		{name: "unknown", channel: Channel("unknown"), bindingID: "bot", conversation: "chat"},
		{name: "binding", channel: WeCom, conversation: "chat"},
		{name: "conversation", channel: WeCom, bindingID: "bot"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := GroupSubjectID(test.channel, test.bindingID, test.conversation); err == nil {
				t.Fatal("GroupSubjectID() error = nil")
			}
		})
	}
}

func TestOutboundMessageOpenClawProjectionDoesNotAliasFiles(t *testing.T) {
	t.Parallel()
	message := OutboundMessage{
		Text:            "已生成报告",
		Files:           []OutboundFile{{Name: "report.txt", Path: "/tmp/report.txt"}},
		Card:            &InteractiveCard{Title: "报告", Body: "查看结果"},
		UpdateMessageID: "progress-1", IdempotencyKey: "request-1",
	}
	projected := message.OpenClawMessage()
	if projected.Text != message.Text || len(projected.Files) != 1 || projected.Files[0].Name != "report.txt" {
		t.Fatalf("OpenClawMessage() = %#v", projected)
	}
	projected.Files[0].Name = "mutated.txt"
	if message.Files[0].Name != "report.txt" {
		t.Fatal("OpenClawMessage() aliases caller file slice")
	}
}

func TestBoundChannelValidatesAndDelegatesLifecycle(t *testing.T) {
	t.Parallel()
	if _, err := NewBoundChannel("", func(context.Context) error { return nil }); err == nil {
		t.Fatal("NewBoundChannel(empty ID) error = nil")
	}
	if _, err := NewBoundChannel("support", nil); err == nil {
		t.Fatal("NewBoundChannel(nil runner) error = nil")
	}
	wantErr := errors.New("receiver stopped")
	called := false
	bound, err := NewBoundChannel(" support-main ", func(ctx context.Context) error {
		called = true
		if ctx == nil {
			t.Fatal("runner context = nil")
		}
		return wantErr
	})
	if err != nil {
		t.Fatal(err)
	}
	if bound.ID() != "support-main" {
		t.Fatalf("ID() = %q", bound.ID())
	}
	if err := bound.Run(context.Background()); !errors.Is(err, wantErr) || !called {
		t.Fatalf("Run() called=%v error=%v", called, err)
	}
}
