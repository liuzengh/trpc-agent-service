package inmemory

import (
	"context"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type BindingResolver struct {
	mu       sync.RWMutex
	bindings map[string]tenant.ExecutionBinding
}

func NewBindingResolver() *BindingResolver {
	return &BindingResolver{bindings: make(map[string]tenant.ExecutionBinding)}
}
func (r *BindingResolver) Put(tenantID, appID string, binding tenant.ExecutionBinding) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bindings[tenantID+"\x00"+appID] = binding
}
func (r *BindingResolver) ResolveExecutionBinding(ctx context.Context, tc tenant.Context) (tenant.ExecutionBinding, error) {
	if err := ctx.Err(); err != nil {
		return tenant.ExecutionBinding{}, err
	}
	if err := tc.Validate(); err != nil {
		return tenant.ExecutionBinding{}, err
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	b, ok := r.bindings[tc.TenantID+"\x00"+tc.AgentAppID]
	if !ok {
		return tenant.ExecutionBinding{}, runtime.ErrNotFound
	}
	return b, b.Validate()
}
