package channels_test

import (
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func TestReplyValidateRichKinds(t *testing.T) {
	base := channels.Reply{
		TenantID:        "tenant-1",
		AppID:           "app-1",
		RequestID:       "request-1",
		SourceEventID:   "event-1",
		Channel:         channels.ChannelFeishu,
		BindingID:       "binding-1",
		BindingRevision: 1,
		ReplyID:         "reply-1",
		Revision:        1,
		Target: channels.ReplyTarget{
			Kind:             channels.TargetKindUser,
			InternalEntityID: "user-1",
		},
	}
	stream := base
	stream.Kind = channels.ReplyKindStream
	stream.StreamID = "request-1"
	stream.StreamPhase = channels.StreamPhaseStart
	stream.StreamSequence = 1
	stream.Text = "partial"
	if err := stream.Validate(); err != nil {
		t.Fatalf("validate stream reply: %v", err)
	}
	card := base
	card.Kind = channels.ReplyKindCard
	card.Card = &channels.ReplyCard{
		Title: "Agent",
		Body:  "answer",
	}
	if err := card.Validate(); err != nil {
		t.Fatalf("validate card reply: %v", err)
	}
	if stream.StableID() == card.StableID() {
		t.Fatal("stream and card replies share an identity")
	}
}

func TestReplyTextZeroValueRemainsCompatible(t *testing.T) {
	reply := channels.Reply{
		TenantID:        "tenant-1",
		AppID:           "app-1",
		RequestID:       "request-1",
		SourceEventID:   "event-1",
		Channel:         channels.ChannelFeishu,
		BindingID:       "binding-1",
		BindingRevision: 1,
		ReplyID:         "reply-1",
		Revision:        1,
		Target:          channels.ReplyTarget{Kind: channels.TargetKindUser, InternalEntityID: "user-1"},
		Text:            "answer",
	}
	if reply.ReplyKind() != channels.ReplyKindText {
		t.Fatalf("zero reply kind = %q", reply.ReplyKind())
	}
	if err := reply.Validate(); err != nil {
		t.Fatalf("validate zero-kind text reply: %v", err)
	}
}

func TestReplyCardRejectsActionsWithoutCallbackBoundary(t *testing.T) {
	card := channels.ReplyCard{
		Title: "Agent 回复",
		Body:  "done",
		Actions: []channels.CardAction{{
			ID: "retry", Label: "Retry", Value: "request-a",
		}},
	}
	if !errors.Is(card.Validate(), channels.ErrReplyCardActionsUnsupported) {
		t.Fatal("action-bearing card was accepted without an authenticated callback boundary")
	}
}
