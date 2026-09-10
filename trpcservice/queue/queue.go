// Package queue defines dispatch, execution lease, and stream delivery contracts.
package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/execution"
)

var (
	// ErrLeaseLost means a worker no longer owns a claimed execution.
	ErrLeaseLost = errors.New("execution lease is lost")
)

// Dispatch identifies one execution published to a worker stream.
type Dispatch struct {
	OutboxID  int64
	TenantID  string
	AppID     string
	RequestID string
	// TraceParent and TraceState carry the W3C Trace Context through the
	// durable Redis boundary. They are metadata only and never contain
	// request payloads.
	TraceParent string `json:"traceparent,omitempty"`
	TraceState  string `json:"tracestate,omitempty"`
}

// Validate checks the persistent execution identity carried by a stream entry.
func (d Dispatch) Validate() error {
	if d.OutboxID <= 0 {
		return errors.New("dispatch outbox id must be positive")
	}
	if d.TenantID == "" || d.AppID == "" || d.RequestID == "" {
		return errors.New("dispatch scope is required")
	}
	return nil
}

// Delivery is one stream message assigned to a consumer group member.
type Delivery struct {
	ID       string
	Dispatch Dispatch
}

// Validate checks one received stream delivery.
func (d Delivery) Validate() error {
	if d.ID == "" {
		return errors.New("stream delivery id is required")
	}
	return d.Dispatch.Validate()
}

// Publisher writes durable dispatches into a shared worker stream.
type Publisher interface {
	Publish(context.Context, Dispatch) error
}

// Stream provides Consumer Group delivery, local ownership release,
// acknowledgement, and dead-lettering.
type Stream interface {
	Receive(context.Context, string, time.Duration) (Delivery, error)
	// Release removes only this process's local ownership marker. The durable
	// stream entry remains pending for a later reclaim.
	Release(Delivery)
	Ack(context.Context, Delivery) error
	Dead(context.Context, Delivery, error) error
}

// ClaimRequest identifies a worker and requested execution lease duration.
type ClaimRequest struct {
	Owner         string
	LeaseDuration time.Duration
}

// Validate checks claim inputs required by the authoritative execution store.
func (r ClaimRequest) Validate() error {
	if r.Owner == "" {
		return errors.New("lease owner is required")
	}
	if r.LeaseDuration <= 0 {
		return errors.New("lease duration must be positive")
	}
	return nil
}

// Lease identifies one time-bounded execution owner token.
type Lease struct {
	Owner string
	Token string
	Until time.Time
}

// Validate checks a lease returned by the execution store.
func (l Lease) Validate() error {
	if l.Owner == "" || l.Token == "" {
		return errors.New("lease owner and token are required")
	}
	if l.Until.IsZero() {
		return errors.New("lease until is required")
	}
	return nil
}

// Claim is an execution currently owned by one worker lease.
type Claim struct {
	Job     execution.Job
	TurnSeq int64
	Lease   Lease
	Attempt int
	// FinalAttempt is true when another retry is not available for this claim.
	FinalAttempt bool
}

// Validate checks the durable execution identity and lease ownership in a claim.
func (c Claim) Validate() error {
	if err := c.Job.Validate(); err != nil {
		return fmt.Errorf("job: %w", err)
	}
	if c.TurnSeq <= 0 {
		return errors.New("turn_seq must be positive")
	}
	if err := c.Lease.Validate(); err != nil {
		return fmt.Errorf("lease: %w", err)
	}
	if c.Attempt <= 0 {
		return errors.New("attempt must be positive")
	}
	return nil
}

// CompletionStatus is a terminal execution status written by its current owner.
type CompletionStatus string

const (
	// CompletionSucceeded means the worker completed the execution successfully.
	CompletionSucceeded CompletionStatus = "SUCCEEDED"
	// CompletionFailed means the worker exhausted retry attempts.
	CompletionFailed CompletionStatus = "FAILED"
	// CompletionUncertain means an external side effect may have happened and
	// the execution must not be retried automatically.
	CompletionUncertain CompletionStatus = "UNCERTAIN"
)

// Validate checks whether a status is valid for completing a claimed execution.
func (s CompletionStatus) Validate() error {
	switch s {
	case CompletionSucceeded, CompletionFailed, CompletionUncertain:
		return nil
	default:
		return errors.New("completion status is invalid")
	}
}
