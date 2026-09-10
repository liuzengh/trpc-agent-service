package postgres

import (
	"errors"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/channels"
)

func TestChannelPostgresProtocolCodec(t *testing.T) {
	encoded, err := encodeProtocol(channels.ProtocolConfiguration{Telegram: &channels.TelegramProtocolConfiguration{WebhookPath: "/telegram"}})
	if err != nil {
		t.Fatal(err)
	}
	var decoded channels.ProtocolConfiguration
	if err := decodeProtocol(encoded, &decoded); err != nil || decoded.Telegram == nil || decoded.Telegram.WebhookPath != "/telegram" {
		t.Fatalf("protocol decode = %+v, err=%v", decoded, err)
	}
	if err := decodeProtocol([]byte("not-json"), &channels.ProtocolConfiguration{}); !errors.Is(err, ErrStorage) {
		t.Fatalf("malformed protocol error = %v", err)
	}
}
