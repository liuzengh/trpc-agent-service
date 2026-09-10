package redis

import (
	"encoding/json"
	"errors"
	"testing"

	channel "github.com/liuzengh/trpc-agent-service/trpcservice/channels/contract"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	redisclient "github.com/redis/go-redis/v9"
)

func TestDecodeReplyBindsEveryRoutingField(t *testing.T) {
	destination := channel.ReplyDestination{TenantID: "tenant", Channel: "fake", ChannelBindingID: "binding", ExternalAccountID: "account", ConfigVersion: 1}
	event := channel.ReplyEvent{SchemaVersion: 1, TenantID: "tenant", RequestID: "request", ChannelBindingID: "binding", DeliveryKey: "reply", ConfigVersion: 1, ContentRef: "result://request", Target: channel.DeliveryTarget{Channel: "fake", ExternalAccountID: "account", ExternalMessageID: "message"}, Final: true}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]any{"schema_version": "1", "event": payload, "tenant_id": "tenant", "channel": "fake", "binding_id": "binding", "config_version": "1", "external_account_id": "account", "delivery_key": "reply"}
	delivery, err := decodeReply(destination, redisclient.XMessage{ID: "1-0", Values: values})
	if err != nil || delivery.Event != event || delivery.Destination != destination {
		t.Fatalf("delivery=%#v err=%v", delivery, err)
	}
	for _, field := range []string{"tenant_id", "channel", "binding_id", "config_version", "external_account_id", "delivery_key"} {
		changed := make(map[string]any, len(values))
		for key, value := range values {
			changed[key] = value
		}
		changed[field] = "spoofed"
		if _, err := decodeReply(destination, redisclient.XMessage{ID: "1-0", Values: changed}); !errors.Is(err, runtime.ErrVersionMismatch) {
			t.Fatalf("field=%s err=%v", field, err)
		}
	}
}
