// Package replies converts protocol-neutral dispatch events into a safe
// protocol reply. It belongs to the Gateway event boundary and deliberately
// has no channel provider dependencies.
package replies

import (
	"strings"

	"github.com/XnLemon/trpc-agent-service/trpcservice/gateway"
)

const (
	// KindText identifies a rendered text reply.
	KindText = "text"
	// KindFallback identifies a deterministic safe fallback reply.
	KindFallback = "fallback"

	// StableFallback is used when a stream has no renderable text or contains a
	// structured event that the destination cannot represent safely.
	StableFallback = "Sorry, I couldn't process that message."
)

// Reply is the provider-neutral result of rendering a dispatch stream.
type Reply struct {
	Kind string
	Text string
}

// Render consumes all events and returns one deterministic reply. Status and
// Done events are control signals; partial status updates are never emitted as
// premature replies. Any execution error wins over accumulated text.
func Render(events []gateway.DispatchEvent) Reply {
	var b strings.Builder
	for _, event := range events {
		switch event.Type {
		case gateway.DispatchEventError:
			return Reply{Kind: KindFallback, Text: StableFallback}
		case gateway.DispatchEventMessage:
			b.WriteString(event.Text)
		case gateway.DispatchEventStatus, gateway.DispatchEventDone:
		default:
			return Reply{Kind: KindFallback, Text: StableFallback}
		}
	}
	if text := strings.TrimSpace(b.String()); text != "" {
		return Reply{Kind: KindText, Text: text}
	}
	return Reply{Kind: KindFallback, Text: StableFallback}
}
