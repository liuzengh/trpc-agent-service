package queue

import (
	"context"
	"time"
)

// JobQueue is the transport-neutral queue contract consumed by later stages.
// Implementations must not transfer a caller's context across the queue.
type JobQueue interface {
	Enqueue(context.Context, AgentJob) (QueueReceipt, error)
	Receive(context.Context, time.Duration) (Delivery, error)
	Ack(context.Context, Delivery) error
	Nack(context.Context, Delivery, NackOptions) error
	ExtendVisibility(context.Context, Delivery, time.Duration) error
	Close() error
}
