package task

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/vector"
)

// Repository is the durable vector-task state-machine boundary. Implementations
// must make all state transitions conditional and tenant scoped.
type Repository interface {
	Enqueue(context.Context, tenant.TenantContext, vector.VectorDocumentRef, time.Time) (EnqueueOutcome, error)
	Get(context.Context, tenant.TenantContext, string) (Task, error)
	Candidate(context.Context, time.Time) (Candidate, error)
	Claim(context.Context, Candidate, LeaseRef, time.Time) (Task, error)
	ExtendLease(context.Context, Candidate, LeaseRef, time.Time) error
	Complete(context.Context, Candidate, LeaseRef, time.Time) (Task, error)
	Fail(context.Context, Candidate, LeaseRef, Failure, time.Time) (Task, error)
	Cancel(context.Context, tenant.TenantContext, string, time.Time) (Task, error)
	Head(context.Context, string, string) (HeadKey, bool, error)
	Counts(context.Context) (map[State]int64, error)
}
