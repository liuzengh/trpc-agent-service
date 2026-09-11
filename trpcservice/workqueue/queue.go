package workqueue

import (
	"context"
	"errors"
)

// ErrNoMessage means the blocking receive interval ended without work.
var ErrNoMessage = errors.New("work queue has no message")

// Queue is an at-least-once Agent task transport.
type Queue interface {
	Publish(ctx context.Context, task AgentTask) error
	Receive(ctx context.Context) (Delivery, error)
	Ready(ctx context.Context) error
	Close() error
}

// Delivery owns one queue message until Ack or Retry succeeds.
type Delivery interface {
	Task() AgentTask
	Ack(ctx context.Context) error
	Retry(ctx context.Context) error
}

// LeasedDelivery cancels its context when transport ownership is lost. Close
// stops renewal without acknowledging; a later consumer may reclaim the task.
type LeasedDelivery interface {
	Delivery
	Context() context.Context
	Close()
}
