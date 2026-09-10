package governance

import (
	"context"
	"fmt"
	"strings"
)

// Invocation is the request-scoped governance state made available to every
// governed tool through the Runner call context. It must not be stored in a
// cached tenant Runner because its budget and trace identity change per call.
type Invocation struct {
	Execution ExecutionContext
	Budget    *CallBudget
	Units     *UnitBudget
}

// Validate prevents a tool from running with an incomplete tenant identity.
func (i Invocation) Validate() error {
	if strings.TrimSpace(i.Execution.TenantID) == "" ||
		strings.TrimSpace(i.Execution.Role) == "" ||
		strings.TrimSpace(i.Execution.TraceID) == "" ||
		strings.TrimSpace(i.Execution.PolicyVersion) == "" {
		return ErrInvalidExecutionContext
	}
	if i.Budget == nil {
		return fmt.Errorf("%w: tool budget is required", ErrBudgetExceeded)
	}
	return nil
}

type invocationContextKey struct{}

// WithInvocation attaches immutable execution identity and its one shared
// budget to a single Runner call. The returned context must only live for the
// duration of that invocation.
func WithInvocation(ctx context.Context, invocation Invocation) context.Context {
	return context.WithValue(ctx, invocationContextKey{}, invocation)
}

// InvocationFromContext retrieves the request-scoped governance state. A
// missing or malformed value is deliberately indistinguishable from no value
// so callers can fail closed.
func InvocationFromContext(ctx context.Context) (Invocation, bool) {
	if ctx == nil {
		return Invocation{}, false
	}
	invocation, ok := ctx.Value(invocationContextKey{}).(Invocation)
	if !ok || invocation.Validate() != nil {
		return Invocation{}, false
	}
	return invocation, true
}
