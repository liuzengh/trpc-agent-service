// Package routing resolves untrusted channel keys into trusted runtime scopes.
package routing

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
)

var (
	ErrBindingNotFound = errors.New("channel binding not found")
	ErrRouteDisabled   = errors.New("channel route is disabled")
)

// Resolver derives tenant and Agent application identity from a callback key.
type Resolver interface {
	Resolve(ctx context.Context, callbackKey string) (runtimecontext.Scope, error)
}

// ControlPlaneResolver validates a binding and all of its owning objects.
type ControlPlaneResolver struct {
	repository controlplane.Repository
}

// NewControlPlaneResolver creates a resolver backed by the control plane.
func NewControlPlaneResolver(repository controlplane.Repository) (*ControlPlaneResolver, error) {
	if repository == nil {
		return nil, fmt.Errorf("routing control-plane repository is required")
	}
	return &ControlPlaneResolver{repository: repository}, nil
}

// Resolve never accepts tenant or app identity from the request itself.
func (r *ControlPlaneResolver) Resolve(
	ctx context.Context,
	callbackKey string,
) (runtimecontext.Scope, error) {
	binding, err := r.repository.GetChannelBindingByCallbackKey(ctx, callbackKey)
	if err != nil {
		if errors.Is(err, controlplane.ErrNotFound) {
			return runtimecontext.Scope{}, ErrBindingNotFound
		}
		return runtimecontext.Scope{}, fmt.Errorf("resolve channel binding: %w", err)
	}
	if binding.Status != controlplane.StatusActive {
		return runtimecontext.Scope{}, ErrRouteDisabled
	}
	tenant, err := r.repository.GetTenant(ctx, binding.TenantID)
	if err != nil {
		return runtimecontext.Scope{}, fmt.Errorf("resolve binding tenant: %w", err)
	}
	if tenant.Status != controlplane.StatusActive {
		return runtimecontext.Scope{}, ErrRouteDisabled
	}
	app, err := r.repository.GetAgentApp(ctx, binding.TenantID, binding.AppID)
	if err != nil {
		return runtimecontext.Scope{}, fmt.Errorf("resolve binding app: %w", err)
	}
	if app.Status != controlplane.StatusActive || app.StableRevisionID == "" {
		return runtimecontext.Scope{}, ErrRouteDisabled
	}
	revision, err := r.repository.GetStableRevision(ctx, binding.TenantID, binding.AppID)
	if err != nil {
		return runtimecontext.Scope{}, fmt.Errorf("resolve stable revision: %w", err)
	}
	if revision.AppID != binding.AppID || revision.TenantID != binding.TenantID {
		return runtimecontext.Scope{}, fmt.Errorf("stable revision scope mismatch")
	}
	scope, err := runtimecontext.NewScope(
		binding.TenantID,
		binding.AppID,
		revision.ID,
		binding.ChannelType,
		binding.ID,
	)
	if err != nil {
		return runtimecontext.Scope{}, fmt.Errorf("build runtime scope: %w", err)
	}
	return scope, nil
}

var _ Resolver = (*ControlPlaneResolver)(nil)
