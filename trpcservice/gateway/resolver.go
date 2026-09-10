package gateway

import (
	"context"
	"errors"
	"fmt"

	"github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
)

// PlanResolver adapts a trusted Gateway Principal to the neutral runtime
// PlanRequest. Authentication remains Gateway-owned; runtime owns repository
// lookup and immutable execution-plan construction.
type PlanResolver struct {
	resolver *runtime.PlanResolver
}

// NewPlanResolver constructs the runtime resolver behind the Gateway adapter.
func NewPlanResolver(config runtime.PlanResolverConfig) (*PlanResolver, error) {
	resolver, err := runtime.NewPlanResolver(config)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	return &PlanResolver{resolver: resolver}, nil
}

// Ready reports whether the wrapped runtime resolver can accept requests.
func (resolver *PlanResolver) Ready() bool {
	return resolver != nil && resolver.resolver != nil && resolver.resolver.Ready()
}

// ResolveAuthenticatedAPI converts an authenticator-issued proof into the
// trusted API principal before handing the neutral identity to runtime.
func (resolver *PlanResolver) ResolveAuthenticatedAPI(ctx context.Context, authenticated AuthenticatedAPI) (runtime.ExecutionPlan, error) {
	principal, err := newAPIPrincipal(authenticated)
	if err != nil {
		return runtime.ExecutionPlan{}, err
	}
	return resolver.Resolve(ctx, principal)
}

// Resolve preserves the Gateway contract while delegating plan resolution to
// runtime. Principal proof and route validation are intentionally performed
// before any repository is consulted.
func (resolver *PlanResolver) Resolve(ctx context.Context, principal Principal) (runtime.ExecutionPlan, error) {
	if ctx == nil {
		return runtime.ExecutionPlan{}, fmt.Errorf("%w: context is required", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return runtime.ExecutionPlan{}, err
	}
	if !resolver.Ready() {
		return runtime.ExecutionPlan{}, ErrNotReady
	}
	if err := principal.Validate(); err != nil {
		return runtime.ExecutionPlan{}, ErrUnauthenticated
	}
	plan, err := resolver.resolver.Resolve(ctx, runtime.PlanRequest{TenantID: principal.TenantID(), AppID: principal.AppID()})
	if err != nil {
		return runtime.ExecutionPlan{}, mapPlanResolverError(err)
	}
	return plan, nil
}

func mapPlanResolverError(err error) error {
	if IsContextCancellation(err) {
		return err
	}
	if errors.Is(err, runtime.ErrPlanResolverNotReady) {
		return ErrNotReady
	}
	if errors.Is(err, runtime.ErrInvalidPlanRequest) {
		return ErrInvalid
	}
	return ErrPlanUnavailable
}

// IsContextCancellation reports whether an error is a caller cancellation.
func IsContextCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
