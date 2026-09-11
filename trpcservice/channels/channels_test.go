package channels

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

func TestRegistryAndTestAdapter(t *testing.T) {
	adapter := NewTestAdapter()
	registry, err := NewRegistry(adapter)
	if err != nil {
		t.Fatalf("new registry: %v", err)
	}
	resolved, err := registry.Get("http")
	if err != nil {
		t.Fatalf("get adapter: %v", err)
	}
	binding := controlplane.DefaultBootstrapData().ChannelBindings[0]
	receipt, err := resolved.Send(context.Background(), binding, OutboundMessage{
		OutboundID: "out-1",
		RequestID:  "request-1",
		Text:       "hello",
	})
	if err != nil || receipt.ProviderMessageID == "" || len(adapter.Deliveries()) != 1 {
		t.Fatalf("receipt=%+v deliveries=%+v err=%v", receipt, adapter.Deliveries(), err)
	}
}
