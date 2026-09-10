package wecom

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func TestOutboundClientSendsDurableStreamFramesAndCard(t *testing.T) {
	binding := testWeComBinding("tenant-a", "support", "binding-a", "bot-a", "bot-secret")
	sender := &richWeComSender{}
	client, err := NewOutboundClient(
		context.Background(),
		testWeComSecrets{key: binding, value: "bot-secret"},
		binding.Snapshot(),
		WithMessageSender(sender),
	)
	if err != nil {
		t.Fatal(err)
	}
	target, err := encodeProviderTarget("chat-a", "callback-a")
	if err != nil {
		t.Fatal(err)
	}
	base := channels.Reply{
		TenantID:        binding.TenantID,
		AppID:           binding.AppID,
		RequestID:       "request-a",
		SourceEventID:   "event-a",
		Channel:         channels.ChannelWeCom,
		BindingID:       binding.BindingID,
		BindingRevision: binding.BindingRevision,
		Revision:        1,
		Target:          channels.ReplyTarget{Kind: channels.TargetKindConversation, InternalEntityID: "conversation-a"},
		Kind:            channels.ReplyKindStream,
		StreamID:        "stream-a",
	}
	frames := []channels.Reply{
		base,
		base,
		base,
	}
	frames[0].ReplyID = "reply-start"
	frames[0].StreamPhase = channels.StreamPhaseStart
	frames[0].StreamSequence = 1
	frames[0].Text = "hello"
	frames[1].ReplyID = "reply-update"
	frames[1].StreamPhase = channels.StreamPhaseUpdate
	frames[1].StreamSequence = 2
	frames[1].Text = "hello world"
	frames[2].ReplyID = "reply-end"
	frames[2].StreamPhase = channels.StreamPhaseEnd
	frames[2].StreamSequence = 3
	frames[2].Text = "hello world!"

	for index, frame := range frames {
		receipt, err := client.SendStream(context.Background(), frame, target, "provider-stream")
		if err != nil {
			t.Fatalf("send stream frame %d: %v", index, err)
		}
		if receipt.ProviderMessageID != "provider-stream" {
			t.Fatalf("stream receipt %d = %#v", index, receipt)
		}
	}
	if len(sender.streams) != 3 {
		t.Fatalf("stream calls = %#v", sender.streams)
	}
	if sender.streams[0].destination != "chat-a" || sender.streams[0].callbackRequestID != "callback-a" ||
		sender.streams[0].streamID != "stream-a" || sender.streams[0].content != "hello" ||
		sender.streams[0].finish || sender.streams[0].update {
		t.Fatalf("start stream call = %#v", sender.streams[0])
	}
	if !sender.streams[1].update || sender.streams[1].finish || sender.streams[1].content != "hello world" {
		t.Fatalf("update stream call = %#v", sender.streams[1])
	}
	if !sender.streams[2].update || !sender.streams[2].finish || sender.streams[2].content != "hello world!" {
		t.Fatalf("end stream call = %#v", sender.streams[2])
	}
	abort := frames[0]
	abort.ReplyID = "reply-abort"
	abort.StreamPhase = channels.StreamPhaseAbort
	abort.StreamSequence = 1
	abort.Text = "执行失败"
	if _, err := client.SendStream(context.Background(), abort, target, ""); err != nil {
		t.Fatalf("send initial abort: %v", err)
	}
	if len(sender.streams) != 4 || sender.streams[3].update || !sender.streams[3].finish || sender.streams[3].content != "执行失败" {
		t.Fatalf("initial abort stream call = %#v", sender.streams)
	}

	cardReply := base
	cardReply.Kind = channels.ReplyKindCard
	cardReply.StreamID = ""
	cardReply.Card = &channels.ReplyCard{
		Title:  "Agent 回复",
		Body:   "done",
		Status: "SUCCEEDED",
	}
	cardReply.ReplyID = "reply-card"
	cardReply.StreamPhase = ""
	cardReply.StreamSequence = 0
	if _, err := client.SendCard(context.Background(), cardReply, target, ""); err != nil {
		t.Fatalf("send card: %v", err)
	}
	if sender.cardTarget != "chat-a" || sender.card["card_type"] != "text_notice" {
		t.Fatalf("card call target=%q payload=%#v", sender.cardTarget, sender.card)
	}
	if _, ok := sender.card["horizontal_content_list"]; ok {
		t.Fatalf("unsupported card actions were sent: %#v", sender.card)
	}
}

type richWeComSender struct {
	streams    []wecomStreamCall
	cardTarget string
	card       map[string]any
}

type wecomStreamCall struct {
	destination       string
	callbackRequestID string
	streamID          string
	content           string
	finish            bool
	update            bool
}

func (s *richWeComSender) SendMessage(context.Context, string, string) (string, error) {
	return "provider-text", nil
}

func (s *richWeComSender) SendStream(
	_ context.Context,
	destination, callbackRequestID, streamID, content string,
	finish, update bool,
) (string, error) {
	s.streams = append(s.streams, wecomStreamCall{
		destination:       destination,
		callbackRequestID: callbackRequestID,
		streamID:          streamID,
		content:           content,
		finish:            finish,
		update:            update,
	})
	return "provider-stream", nil
}

func (s *richWeComSender) SendCard(_ context.Context, destination string, card map[string]any) (string, error) {
	s.cardTarget = destination
	s.card = card
	return "provider-card", nil
}
