package runtime

import (
	"context"
	"errors"
	"strings"
	"sync"
)

var (
	// ErrInvalidTenantRuntime reports invalid tenant runtime registry input.
	ErrInvalidTenantRuntime = errors.New("invalid tenant runtime")
	// ErrTenantRuntimeClosed reports that the tenant runtime registry is closed.
	ErrTenantRuntimeClosed = errors.New("tenant runtime registry is closed")
)

// TenantRuntime materializes and owns one tenant's runtime resources.
type TenantRuntime interface {
	Ensure(context.Context, string) error
}

// TenantRuntimeInvalidator invalidates tenant-scoped runtime resources.
type TenantRuntimeInvalidator interface{ InvalidateTenant(string) }

// TenantRuntimeMaterializer constructs a tenant runtime on demand.
type TenantRuntimeMaterializer func(context.Context, string) error

type tenantRuntimeState struct {
	done        chan struct{}
	ready       bool
	invalidated bool
	err         error
}

// TenantRuntimeRegistry lazily materializes and tracks tenant runtimes.
type TenantRuntimeRegistry struct {
	mu          sync.Mutex
	materialize TenantRuntimeMaterializer
	states      map[string]*tenantRuntimeState
	closed      bool
}

// NewTenantRuntimeRegistry creates a tenant runtime registry.
func NewTenantRuntimeRegistry(materialize TenantRuntimeMaterializer) (*TenantRuntimeRegistry, error) {
	if materialize == nil {
		return nil, ErrInvalidTenantRuntime
	}
	return &TenantRuntimeRegistry{materialize: materialize, states: make(map[string]*tenantRuntimeState)}, nil
}

// Ensure materializes tenantID if it is not already active.
func (registry *TenantRuntimeRegistry) Ensure(ctx context.Context, tenantID string) error {
	if registry == nil || ctx == nil || strings.TrimSpace(tenantID) == "" {
		return ErrInvalidTenantRuntime
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for {
		registry.mu.Lock()
		if registry.closed {
			registry.mu.Unlock()
			return ErrTenantRuntimeClosed
		}
		state := registry.states[tenantID]
		if state == nil {
			state = &tenantRuntimeState{done: make(chan struct{})}
			registry.states[tenantID] = state
			registry.mu.Unlock()
			return registry.materializeOne(ctx, tenantID, state)
		}
		if state.ready {
			registry.mu.Unlock()
			return nil
		}
		done := state.done
		registry.mu.Unlock()
		select {
		case <-done:
			if err := ctx.Err(); err != nil {
				return err
			}
			if state.err != nil {
				return state.err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (registry *TenantRuntimeRegistry) materializeOne(ctx context.Context, tenantID string, state *tenantRuntimeState) error {
	err := registry.materialize(ctx, tenantID)
	registry.mu.Lock()
	if current := registry.states[tenantID]; current == state {
		if registry.closed {
			state.err = ErrTenantRuntimeClosed
			delete(registry.states, tenantID)
		} else if state.invalidated && err == nil {
			delete(registry.states, tenantID)
		} else if err == nil {
			state.ready = true
		} else {
			state.err = err
			delete(registry.states, tenantID)
		}
		close(state.done)
	}
	invalidated := state.invalidated && err == nil
	stateErr := state.err
	registry.mu.Unlock()
	if invalidated {
		return registry.Ensure(ctx, tenantID)
	}
	return stateErr
}

// InvalidateTenant removes tenantID from the active registry.
func (registry *TenantRuntimeRegistry) InvalidateTenant(tenantID string) {
	if registry == nil || strings.TrimSpace(tenantID) == "" {
		return
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return
	}
	if state := registry.states[tenantID]; state != nil {
		if state.ready {
			delete(registry.states, tenantID)
		} else {
			state.invalidated = true
		}
	}
}

// Close stops all active tenant runtimes and releases registry resources.
func (registry *TenantRuntimeRegistry) Close() error {
	if registry == nil {
		return nil
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.closed {
		return nil
	}
	registry.closed = true
	for tenantID, state := range registry.states {
		if state.ready {
			delete(registry.states, tenantID)
		} else {
			state.invalidated = true
		}
	}
	return nil
}

var _ TenantRuntime = (*TenantRuntimeRegistry)(nil)
var _ TenantRuntimeInvalidator = (*TenantRuntimeRegistry)(nil)
