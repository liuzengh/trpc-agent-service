package replies

import (
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
)

func TestRenderAggregatesTextAndFallsBackDeterministically(t *testing.T) {
	if got := Render([]gateway.DispatchEvent{{Type: gateway.DispatchEventStatus}, {Type: gateway.DispatchEventMessage, Text: "a"}, {Type: gateway.DispatchEventMessage, Text: "b"}}); got != (Reply{Kind: KindText, Text: "ab"}) {
		t.Fatalf("rendered text = %#v", got)
	}
	if got := Render([]gateway.DispatchEvent{{Type: gateway.DispatchEventError, Error: "provider detail"}}); got != (Reply{Kind: KindFallback, Text: StableFallback}) {
		t.Fatalf("error fallback = %#v", got)
	}
	if got := Render(nil); got != (Reply{Kind: KindFallback, Text: StableFallback}) {
		t.Fatalf("empty fallback = %#v", got)
	}
	if got := Render([]gateway.DispatchEvent{{Type: gateway.DispatchEventType("card"), Text: "partial"}}); got != (Reply{Kind: KindFallback, Text: StableFallback}) {
		t.Fatalf("unknown event fallback = %#v", got)
	}
}
