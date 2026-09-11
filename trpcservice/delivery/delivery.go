// Package delivery defines the operation-level contract shared by channel
// adapters, the durable Outbox, and its persistence ledger.
package delivery

import (
	"fmt"
	"time"

	"github.com/cyl6/trpc-agent-service/trpcservice/domain"
)

// Outcome describes what is known about one provider request. Unknown is
// deliberately distinct from a retryable rejection: callers must never retry
// it automatically because the provider may already have accepted the message.
type Outcome string

const (
	Confirmed         Outcome = "confirmed"
	RetryableNotSent  Outcome = "retryable_not_sent"
	PermanentRejected Outcome = "permanent_rejected"
	Unknown           Outcome = "unknown"
)

// Part is one deterministic provider request in a delivery plan.
type Part struct {
	Message domain.OutboundMessage
	Index   int
	Total   int
}

// Request carries the stable operation identity and the fresh attempt number
// for exactly one provider request. OperationKey remains unchanged on retry.
type Request struct {
	OperationKey string
	AttemptNo    int
	Message      domain.OutboundMessage
}

// Result contains only bounded, low-cardinality provider metadata suitable
// for a durable ledger. Raw response bodies and provider error descriptions
// must never be stored here.
type Result struct {
	Outcome           Outcome
	ErrorType         string
	ProviderCode      string
	HTTPStatus        int
	ProviderMessageID string
	ProviderRequestID string
	RetryAfter        time.Duration
	ResponseHash      string
	// Err is process-local diagnostic context. Persistence implementations
	// ignore it and callers must not expose it through an API or metric label.
	Err error `json:"-"`
}

// Validate rejects an adapter result whose scheduling meaning is ambiguous.
func (r Result) Validate() error {
	switch r.Outcome {
	case Confirmed, RetryableNotSent, PermanentRejected, Unknown:
		return nil
	default:
		return fmt.Errorf("delivery: invalid outcome %q", r.Outcome)
	}
}

// Failure returns a redacted error for the legacy synchronous path. Durable
// scheduling must inspect Outcome directly instead of parsing this string.
func (r Result) Failure() error {
	if r.Outcome == Confirmed {
		return nil
	}
	category := r.ErrorType
	if category == "" {
		category = "delivery_failed"
	}
	return &FailureError{
		Outcome: r.Outcome, Category: category,
		RetryWholeTask: r.Outcome == RetryableNotSent,
	}
}

// FailureError lets the legacy in-process queue retry only outcomes that are
// explicitly proven safe for the whole task. A retryable failure of a later
// part is not whole-task safe because earlier parts may already be confirmed.
// It intentionally contains no provider response.
type FailureError struct {
	Outcome        Outcome
	Category       string
	RetryWholeTask bool
}

func (e *FailureError) Error() string {
	if e == nil {
		return "delivery failed"
	}
	return fmt.Sprintf("delivery %s: %s", e.Outcome, e.Category)
}
