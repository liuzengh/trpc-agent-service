// Package routing resolves untrusted channel keys into trusted runtime scopes.
package routing

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtimecontext"
)

var (
	ErrBindingNotFound = errors.New("channel binding not found")
	ErrRouteDisabled   = errors.New("channel route is disabled")
	ErrBindingChanged  = errors.New("channel authorization changed while request was pending")
)

// Resolver derives tenant and Agent application identity from a callback key.
type Resolver interface {
	Resolve(ctx context.Context, callbackKey string) (runtimecontext.Scope, error)
}

type RequestResolver interface {
	ResolveFor(ctx context.Context, callbackKey string, routingKey string) (runtimecontext.Scope, error)
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
	return r.ResolveFor(ctx, callbackKey, "")
}

func (r *ControlPlaneResolver) ResolveFor(
	ctx context.Context,
	callbackKey string,
	routingKey string,
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
	revisionID, err := selectRevisionID(app, routingKey)
	if err != nil {
		return runtimecontext.Scope{}, fmt.Errorf("select rollout revision: %w", err)
	}
	revision, err := r.repository.GetRevision(ctx, binding.TenantID, revisionID)
	if err != nil {
		return runtimecontext.Scope{}, fmt.Errorf("resolve selected revision: %w", err)
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
	scope.BindingVersion = binding.Version
	return scope, nil
}

// Revalidate reuses route validation without repinning a conversation to the
// latest revision. Changed channel authorization requires a new user decision.
func (r *ControlPlaneResolver) Revalidate(ctx context.Context, scope runtimecontext.Scope) error {
	b, err := r.repository.GetChannelBinding(ctx, scope.TenantID, scope.ChannelBindingID)
	if err != nil {
		return err
	}
	current, err := r.Resolve(ctx, b.CallbackKey)
	if err != nil {
		return err
	}
	if current.TenantID != scope.TenantID || current.AppID != scope.AppID || current.ChannelType != scope.ChannelType || (scope.BindingVersion != 0 && current.BindingVersion != scope.BindingVersion) {
		return ErrBindingChanged
	}
	return nil
}

type rolloutPolicy struct {
	Mode             string `json:"mode"`
	CanaryRevisionID string `json:"canary_revision_id"`
	CanaryPercent    int    `json:"canary_percent"`
	Salt             string `json:"salt"`
}

func selectRevisionID(app controlplane.AgentApp, routingKey string) (string, error) {
	if len(app.RolloutPolicy) == 0 {
		return app.StableRevisionID, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(app.RolloutPolicy))
	decoder.DisallowUnknownFields()
	var policy rolloutPolicy
	if err := decoder.Decode(&policy); err != nil {
		return "", err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return "", errors.New("rollout policy must contain one JSON value")
	}
	if policy.CanaryPercent < 0 || policy.CanaryPercent > 100 {
		return "", errors.New("canary_percent must be between 0 and 100")
	}
	if policy.CanaryPercent == 0 || routingKey == "" {
		return app.StableRevisionID, nil
	}
	if policy.CanaryRevisionID == "" {
		return "", errors.New("canary_revision_id is required")
	}
	digest := sha256.Sum256([]byte(app.TenantID + "\x00" + app.ID + "\x00" + policy.Salt + "\x00" + routingKey))
	bucket := binary.BigEndian.Uint64(digest[:8]) % 100
	if int(bucket) < policy.CanaryPercent {
		return policy.CanaryRevisionID, nil
	}
	return app.StableRevisionID, nil
}

var _ Resolver = (*ControlPlaneResolver)(nil)
var _ RequestResolver = (*ControlPlaneResolver)(nil)
