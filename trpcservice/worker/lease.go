package worker

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
)

type jobLeaseContextKey struct{}
type finalAttemptContextKey struct{}

// ContextWithJobLease attaches the current durable queue lease to a worker
// context. The authoritative event journal revalidates this lease before it
// accepts a write.
func ContextWithJobLease(ctx context.Context, lease queue.Lease) (context.Context, error) {
	if err := lease.Validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, jobLeaseContextKey{}, lease), nil
}

// JobLeaseFromContext returns the durable lease attached by a Consumer.
func JobLeaseFromContext(ctx context.Context) (queue.Lease, bool) {
	if ctx == nil {
		return queue.Lease{}, false
	}
	lease, ok := ctx.Value(jobLeaseContextKey{}).(queue.Lease)
	if !ok || lease.Validate() != nil {
		return queue.Lease{}, false
	}
	return lease, true
}

// ContextWithFinalAttempt tells an executor whether its current durable claim
// has another retry available.
func ContextWithFinalAttempt(ctx context.Context, final bool) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, finalAttemptContextKey{}, final)
}

// FinalAttemptFromContext reports whether the current durable claim is final.
func FinalAttemptFromContext(ctx context.Context) bool {
	return ctx != nil && ctx.Value(finalAttemptContextKey{}) == true
}
