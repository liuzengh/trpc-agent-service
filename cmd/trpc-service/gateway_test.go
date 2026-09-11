package main

import (
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/channels"
)

// TestWebhookSelectionRejectsUnsupportedChannel: an operator who lists a channel
// the platform cannot call back must be told at startup. Silently ignoring it
// would leave a configured-but-dead callback URL, and the bot would look
// connected while receiving nothing.
func TestWebhookSelectionRejectsUnsupportedChannel(t *testing.T) {
	verifiers, builders := webhookChannels()

	if _, err := webhookSelection([]string{channels.ChannelWeCom}, verifiers, builders); err == nil {
		t.Fatal("WeCom has no HTTP callback mode and must be rejected")
	} else if !strings.Contains(err.Error(), channels.ChannelWeCom) {
		t.Errorf("error should name the channel: %v", err)
	}
	if _, err := webhookSelection([]string{"telegram"}, verifiers, builders); err == nil {
		t.Fatal("an unknown channel must be rejected")
	}

	served, err := webhookSelection([]string{channels.ChannelFeishu, ""}, verifiers, builders)
	if err != nil {
		t.Fatalf("feishu must be servable: %v", err)
	}
	if !served[channels.ChannelFeishu] || len(served) != 1 {
		t.Errorf("served = %v, want exactly feishu", served)
	}
}
