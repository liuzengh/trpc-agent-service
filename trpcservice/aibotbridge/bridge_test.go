package aibotbridge

import (
	"testing"

	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
)

func TestNormalizeTextDirectMessage(t *testing.T) {
	message, err := normalize(callbackBody{
		MsgID: "msg-1", AIBotID: "bot-1", ChatType: "single", MsgType: "text",
		From: callbackFrom{UserID: "user-1"}, Text: textContent{Content: "hello"},
	}, "binding-1", "bot-1")
	if err != nil {
		t.Fatal(err)
	}
	if message.Scope != domain.ScopeDirect || message.ReplyTarget != "user-1" || message.Text != "hello" {
		t.Fatalf("unexpected normalized message: %+v", message)
	}
}

func TestNormalizeMixedGroupMessageAndRejectsWrongBot(t *testing.T) {
	body := callbackBody{
		MsgID: "msg-2", AIBotID: "bot-1", ChatID: "chat-1", ChatType: "group", MsgType: "mixed",
		From: callbackFrom{UserID: "user-1"},
		Mixed: mixedContent{Items: []mixedItem{
			{MsgType: "text", Text: textContent{Content: "first"}},
			{MsgType: "image", Image: mediaContent{URL: "https://encrypted.invalid/image"}},
			{MsgType: "text", Text: textContent{Content: "second"}},
		}},
	}
	message, err := normalize(body, "binding-1", "bot-1")
	if err != nil {
		t.Fatal(err)
	}
	if message.Scope != domain.ScopeGroup || message.ReplyTarget != "chat-1" || message.Text != "first\nsecond" || len(message.Attachments) != 1 {
		t.Fatalf("unexpected normalized message: %+v", message)
	}
	body.AIBotID = "another-bot"
	if _, err := normalize(body, "binding-1", "bot-1"); err == nil {
		t.Fatal("callback for another bot was accepted")
	}
}
