package channels_test

import (
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/telegram"
	"github.com/XnLemon/trpc-agent-service/trpcservice/channels/wecom"
)

func TestAdapterContractsSeparateProtocolTransportFromLifecycle(t *testing.T) {
	var polling channels.PollingAdapter = (*telegram.Adapter)(nil)
	var webhook channels.WebhookAdapter = (*wecom.Handler)(nil)
	if polling.Channel() != channels.ChannelTelegram {
		t.Fatalf("polling channel = %q", polling.Channel())
	}
	if webhook.Channel() != channels.ChannelWeCom {
		t.Fatalf("webhook channel = %q", webhook.Channel())
	}
	webhook.BeginShutdown()
	if err := polling.Close(); err != nil {
		t.Fatalf("polling Close() = %v", err)
	}
	if err := webhook.Close(); err != nil {
		t.Fatalf("webhook Close() = %v", err)
	}
}
