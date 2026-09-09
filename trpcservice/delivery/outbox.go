// Package delivery owns durable outbound-message claims and channel delivery.
package delivery

import (
	"context"
	"errors"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

// ErrClaimOwner means that an Outbox claim was completed by another worker.
var ErrClaimOwner = errors.New("delivery: outbox claim owned by another worker")

// Status is the durable Outbox delivery state.
type Status string

const (
	StatusPending Status = "pending"
	StatusClaimed Status = "claimed"
	StatusSending Status = "sending"
	StatusRetry   Status = "retry"
	StatusSent    Status = "sent"
	StatusDLQ     Status = "dlq"
	// StatusUncertain means the provider may have accepted a request but the
	// worker could not durably confirm it. It requires operator reconciliation.
	StatusUncertain Status = "uncertain"
)

// BindingKey scopes a sender to one tenant-owned channel binding.
type BindingKey struct {
	TenantID  string
	BindingID string
}

// Claim is one exact Outbox delivery attempt.
type Claim struct {
	Message     gateway.OutboundMessage
	Owner       string
	ClaimToken  string
	Attempt     int
	Status      Status
	LeaseUntil  time.Time
	LastFailure string
}

// Store is the durable contract used by Delivery Workers.
type Store interface {
	ClaimReady(context.Context, []BindingKey, string, time.Duration, int) ([]Claim, error)
	Renew(context.Context, Claim, time.Duration) (Claim, error)
	BeginSend(context.Context, Claim) error
	MarkSent(context.Context, Claim) error
	Fail(context.Context, Claim, error, time.Time, bool) (Status, error)
	MarkUncertain(context.Context, Claim, error) error
}
