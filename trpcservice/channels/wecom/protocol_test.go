package wecom

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func TestNormalizeMessageAcceptsGroupFile(t *testing.T) {
	binding := testWeComBinding("tenant-a", "support", "binding-a", "bot-a", "bot-secret")
	envelope, err := normalizeMessage(binding.Snapshot(), Message{
		MessageID:   "message-file-group",
		AIBotID:     "bot-a",
		ChatID:      "chat-a",
		ChatType:    "group",
		From:        MessageFrom{UserID: "user-a"},
		MessageType: "file",
		File: MessageMedia{
			URL:    "https://wework.qpic.cn/attachment/file-a",
			AESKey: "encrypted-key",
		},
	})
	if err != nil {
		t.Fatalf("normalize group file: %v", err)
	}
	if envelope.ConversationKind != channels.ConversationGroup || envelope.ChatID != "chat-a" {
		t.Fatalf("conversation = %#v", envelope)
	}
	if envelope.MessageType != channels.MessageTypeFile || len(envelope.Media) != 1 {
		t.Fatalf("media = %#v", envelope.Media)
	}
}
