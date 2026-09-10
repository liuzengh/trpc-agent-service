package channels_test

import (
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func TestNewChannelInputValidatesMappingShape(t *testing.T) {
	base := channels.ChannelInput{
		TenantID:          "tenant-a",
		AppID:             "support",
		Channel:           channels.ChannelWeCom,
		BindingID:         "binding-1",
		BindingRevision:   1,
		ExternalMessageID: "message-1",
		Conversation: channels.ChannelConversation{
			Kind: channels.ConversationGroup,
		},
		MessageType: channels.MessageTypeText,
		Text:        "hello",
	}

	if _, err := channels.NewChannelInput(base, channels.ChannelMappingInput{
		ExternalSenderID:           "user-1",
		ExternalChatID:             "chat-1",
		ProviderSenderTarget:       "user-target-1",
		ProviderConversationTarget: "chat-target-1",
	}); err != nil {
		t.Fatalf("new group channel input: %v", err)
	}

	base.Conversation.Kind = channels.ConversationDirect
	if _, err := channels.NewChannelInput(base, channels.ChannelMappingInput{
		ExternalSenderID:     "user-1",
		ExternalChatID:       "chat-1",
		ProviderSenderTarget: "user-target-1",
	}); err == nil {
		t.Fatal("direct channel input accepted conversation mapping fields")
	}
}

func TestChannelInputCloneCopiesMappingAndArtifacts(t *testing.T) {
	input, err := channels.NewChannelInput(
		channels.ChannelInput{
			TenantID:          "tenant-a",
			AppID:             "support",
			Channel:           channels.ChannelFeishu,
			BindingID:         "binding-1",
			BindingRevision:   1,
			ExternalMessageID: "message-1",
			Conversation: channels.ChannelConversation{
				Kind: channels.ConversationDirect,
			},
			MessageType:  channels.MessageTypeText,
			Text:         "hello",
			ArtifactRefs: []string{"artifact://one"},
		},
		channels.ChannelMappingInput{
			ExternalSenderID:     "user-1",
			ProviderSenderTarget: "user-target-1",
		},
	)
	if err != nil {
		t.Fatalf("new channel input: %v", err)
	}

	clone := input.Clone()
	clone.ArtifactRefs[0] = "artifact://two"
	cloneMapping, ok := clone.MappingInput()
	if !ok {
		t.Fatal("cloned channel input lost mapping")
	}
	cloneMapping.ExternalSenderID = "user-2"
	originalMapping, ok := input.MappingInput()
	if !ok {
		t.Fatal("channel input lost mapping")
	}
	if input.ArtifactRefs[0] != "artifact://one" || originalMapping.ExternalSenderID != "user-1" {
		t.Fatalf("clone mutated original input: %#v %#v", input, originalMapping)
	}
}

func TestProviderMediaRefRejectsTextAndEmptyReferences(t *testing.T) {
	if err := (channels.ProviderMediaRef{Kind: channels.MessageTypeText, Reference: "text"}).Validate(); err == nil {
		t.Fatal("text provider media ref passed validation")
	}
	if err := (channels.ProviderMediaRef{Kind: channels.MessageTypeFile}).Validate(); err == nil {
		t.Fatal("empty provider media ref passed validation")
	}
}
