package telegram

import (
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
	"github.com/go-telegram/bot/models"
)

func TestAdapterLifecycleAccessorsExposeSafeState(t *testing.T) {
	target := newTrustedTarget(t, channels.ChannelTelegram, "accessors", "12345")
	adapter := newTestAdapter(t, target, &dispatchStub{}, &fakeBot{me: &models.User{ID: 12345, IsBot: true, Username: "accessor_bot"}})
	if adapter.Channel() != channels.ChannelTelegram {
		t.Fatalf("adapter channel = %s", adapter.Channel())
	}
	if adapter.Username() != "accessor_bot" || !adapter.Ready() {
		t.Fatalf("adapter identity state: username=%q ready=%v", adapter.Username(), adapter.Ready())
	}
	if err := adapter.Close(); err != nil {
		t.Fatal(err)
	}
	if adapter.Ready() {
		t.Fatal("closed adapter remained ready")
	}

	var nilAdapter *Adapter
	if nilAdapter.Channel() != channels.ChannelTelegram || nilAdapter.Username() != "" || nilAdapter.Ready() {
		t.Fatalf("nil adapter accessors: channel=%s username=%q ready=%v", nilAdapter.Channel(), nilAdapter.Username(), nilAdapter.Ready())
	}
	if err := nilAdapter.Close(); err != nil {
		t.Fatal(err)
	}
}
