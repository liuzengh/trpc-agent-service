// Package channels adapts IM platforms (WeCom, Feishu, etc.)
// into tRPC-Agent-Go Runner inputs, following the OpenClaw Channel model.
package channels

import (
	"context"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

// TextSender sends one logical Outbox reply. Platform implementations own
// provider-specific UTF-8 splitting and token refresh.
type TextSender interface {
	SendText(context.Context, gateway.OutboundMessage) error
}

// SendLimiter is shared by channel senders that expand one logical reply into
// multiple provider API requests.
type SendLimiter interface {
	Wait(context.Context, gateway.OutboundMessage) error
}

// RateLimitAware accepts the process-wide distributed limiter.
type RateLimitAware interface {
	SetDeliveryLimiter(SendLimiter)
}

// RetryClassifier exposes a provider's definitive retry decision.
type RetryClassifier interface {
	DeliveryRetryable() bool
}

// OutcomeClassifier marks errors for which the provider may already have
// accepted the message. Such messages must not be blindly retried.
type OutcomeClassifier interface {
	DeliveryOutcomeUncertain() bool
}

// UncertainError wraps a transport result that may have reached the provider.
type UncertainError struct{ Cause error }

func (err *UncertainError) Error() string {
	if err == nil || err.Cause == nil {
		return "channel: delivery outcome uncertain"
	}
	return fmt.Sprintf("channel: delivery outcome uncertain: %v", err.Cause)
}

func (err *UncertainError) Unwrap() error { return err.Cause }

func (err *UncertainError) DeliveryOutcomeUncertain() bool { return true }
