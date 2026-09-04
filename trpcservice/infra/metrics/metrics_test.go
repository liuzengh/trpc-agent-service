package metrics

import (
	"context"
	"testing"
	"time"
)

// Init must be safe to call even without a real meter provider wired (the
// framework's GetMeterProvider returns a noop provider then), so recording
// never panics.
func TestInitAndRecord(t *testing.T) {
	Init()
	ctx := context.Background()

	InboundMessage(ctx, "t1", "wecom")
	OutboundMessage(ctx, "t1", "wecom")
	AgentRun(ctx, "t1", "a1", 120*time.Millisecond)
	AgentError(ctx, "t1", "a1")
	TokenUsage(ctx, "t1", 42)
	IMDelivery(ctx, "wecom", true)
	IMDelivery(ctx, "wecom", false)
	TenantCost(ctx, "t1", 0.0042)
	SessionLatency(ctx, "t1", 3*time.Millisecond)

	// Re-init is idempotent (instruments are re-registered against the noop
	// provider without error).
	Init()
}
