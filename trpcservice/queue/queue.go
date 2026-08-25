// Package queue contains the Redis Stream work queue and fencing leases.
package queue

import (
	"context"
	"errors"
	"time"

	"github.com/DocJlm/trpc-agent-service/trpcservice/store"
)

var ErrLeaseBusy = errors.New("lease is held by another worker")

type Delivery struct {
	ID   string
	Task store.DispatchTask
}

type Queue interface {
	Publish(context.Context, store.DispatchTask) error
	Receive(context.Context, string, time.Duration) (Delivery, error)
	Ack(context.Context, Delivery) error
	Retry(context.Context, Delivery) error
	Dead(context.Context, Delivery, error) error
	Close() error
}

type Lease interface {
	Fence() int64
	Renew(context.Context, time.Duration) error
	Release(context.Context) error
}

type Locker interface {
	Acquire(context.Context, string, time.Duration) (Lease, error)
}
