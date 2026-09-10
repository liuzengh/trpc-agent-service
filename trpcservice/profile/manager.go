package profile

import (
	"context"

	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// RuntimeBundle owns an immutable Agent graph and creates a request runner
// only with an explicitly injected tenant-scoped session service.
type RuntimeBundle interface {
	NewRunner(session.Service) (runner.Runner, error)
}

type BundleLease interface {
	Bundle() RuntimeBundle
	Release()
}

type RuntimeBundleManager interface {
	Acquire(context.Context, ExecutionProfileKey) (BundleLease, error)
	Retire(ExecutionProfileKey)
	Close(context.Context) error
}

// TenantBundleRetirer is the narrow invalidation capability used when a
// credential generation changes. It never closes an active lease.
type TenantBundleRetirer interface {
	RetireTenant(string)
}
