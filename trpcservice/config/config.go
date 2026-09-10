// Package config loads tenant, model, channel, and storage backend settings.
package config

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// Resolver loads one immutable application config version in tenant scope.
type Resolver interface {
	// ResolveAppConfig returns a validated, caller-owned copy for an exact
	// tenant, application, and version match.
	ResolveAppConfig(ctx context.Context, tenantID, appID, version string) (tenant.AppConfig, error)
}

// BindingResolver resolves channel bindings in tenant application scope.
type BindingResolver interface {
	// ResolveBinding returns a validated binding for an exact tenant, application,
	// and binding ID match.
	ResolveBinding(ctx context.Context, tenantID, appID, bindingID string) (channels.Binding, error)
}

// StaticResolver stores immutable application configs for tests and local wiring.
// Its zero value is an empty resolver. Constructed resolvers are safe for concurrent reads.
type StaticResolver struct {
	configs map[configKey]tenant.AppConfig
}

// StaticBindingResolver stores immutable channel bindings for tests and local wiring.
// Its zero value is an empty resolver. Constructed resolvers are safe for concurrent reads.
type StaticBindingResolver struct {
	bindings map[bindingKey]channels.Binding
	routes   map[string]channels.BindingSnapshot
}

type configKey struct {
	tenantID string
	appID    string
	version  string
}

type bindingKey struct {
	tenantID  string
	appID     string
	bindingID string
}

// NewStaticResolver validates and copies application configs. It rejects duplicate scopes.
func NewStaticResolver(configs ...tenant.AppConfig) (*StaticResolver, error) {
	return newStaticResolver(nil, configs...)
}

// NewStaticResolverWithBindings validates application configs against channel
// bindings and rejects duplicate scopes.
func NewStaticResolverWithBindings(
	bindings []channels.Binding,
	configs ...tenant.AppConfig,
) (*StaticResolver, error) {
	bindingResolver, err := NewStaticBindingResolver(bindings...)
	if err != nil {
		return nil, err
	}
	return newStaticResolver(bindingResolver, configs...)
}

func newStaticResolver(bindingResolver BindingResolver, configs ...tenant.AppConfig) (*StaticResolver, error) {
	resolver := &StaticResolver{
		configs: make(map[configKey]tenant.AppConfig, len(configs)),
	}
	for i, cfg := range configs {
		if err := cfg.Validate(); err != nil {
			return nil, fmt.Errorf("app config %d: %w", i, err)
		}
		if err := ValidateAppConfigBindings(context.Background(), cfg, bindingResolver); err != nil {
			return nil, fmt.Errorf("app config %d: %w", i, err)
		}
		key := configKey{
			tenantID: cfg.TenantID,
			appID:    cfg.AppID,
			version:  cfg.Version,
		}
		if _, exists := resolver.configs[key]; exists {
			return nil, fmt.Errorf(
				"app config for tenant_id %q app_id %q version %q is duplicated",
				key.tenantID,
				key.appID,
				key.version,
			)
		}
		resolver.configs[key] = cfg.Clone()
	}
	return resolver, nil
}

// NewStaticBindingResolver validates and copies channel bindings. It rejects
// duplicate tenant application binding IDs and duplicate PublicRouteID values.
func NewStaticBindingResolver(bindings ...channels.Binding) (*StaticBindingResolver, error) {
	resolver := &StaticBindingResolver{
		bindings: make(map[bindingKey]channels.Binding, len(bindings)),
		routes:   make(map[string]channels.BindingSnapshot, len(bindings)),
	}
	for i, binding := range bindings {
		if err := binding.Validate(); err != nil {
			return nil, fmt.Errorf("channel binding %d: %w", i, err)
		}
		key := bindingKey{
			tenantID:  binding.TenantID,
			appID:     binding.AppID,
			bindingID: binding.BindingID,
		}
		if _, exists := resolver.bindings[key]; exists {
			return nil, fmt.Errorf(
				"channel binding for tenant_id %q app_id %q binding_id %q is duplicated",
				key.tenantID,
				key.appID,
				key.bindingID,
			)
		}
		if binding.PublicRouteID != "" {
			if _, exists := resolver.routes[binding.PublicRouteID]; exists {
				return nil, fmt.Errorf("channel binding public route is duplicated")
			}
			resolver.routes[binding.PublicRouteID] = binding.Snapshot()
		}
		resolver.bindings[key] = binding
	}
	return resolver, nil
}

// ResolveAppConfig returns a validated copy of one exact config version.
func (r *StaticResolver) ResolveAppConfig(
	_ context.Context,
	tenantID,
	appID,
	version string,
) (tenant.AppConfig, error) {
	if tenantID == "" {
		return tenant.AppConfig{}, errors.New("tenant_id is required")
	}
	if appID == "" {
		return tenant.AppConfig{}, errors.New("app_id is required")
	}
	if version == "" {
		return tenant.AppConfig{}, errors.New("config version is required")
	}

	key := configKey{tenantID: tenantID, appID: appID, version: version}
	cfg, ok := r.configs[key]
	if !ok {
		return tenant.AppConfig{}, fmt.Errorf(
			"app config not found for tenant_id %q app_id %q version %q",
			tenantID,
			appID,
			version,
		)
	}
	return cfg.Clone(), nil
}

// ResolveBinding returns a copy of one exact channel binding.
func (r *StaticBindingResolver) ResolveBinding(
	_ context.Context,
	tenantID,
	appID,
	bindingID string,
) (channels.Binding, error) {
	if tenantID == "" {
		return channels.Binding{}, errors.New("tenant_id is required")
	}
	if appID == "" {
		return channels.Binding{}, errors.New("app_id is required")
	}
	if bindingID == "" {
		return channels.Binding{}, errors.New("binding_id is required")
	}

	key := bindingKey{tenantID: tenantID, appID: appID, bindingID: bindingID}
	binding, ok := r.bindings[key]
	if !ok {
		return channels.Binding{}, fmt.Errorf(
			"channel binding not found for tenant_id %q app_id %q binding_id %q",
			tenantID,
			appID,
			bindingID,
		)
	}
	return binding, nil
}

// ListActiveChannelBindings returns copies of all active bindings for one
// provider. Long-connection clients are created per returned binding.
func (r *StaticBindingResolver) ListActiveChannelBindings(
	_ context.Context,
	channel channels.Channel,
) ([]channels.Binding, error) {
	if err := channel.Validate(); err != nil {
		return nil, err
	}
	bindings := make([]channels.Binding, 0)
	for _, binding := range r.bindings {
		if binding.Channel == channel && binding.Status == channels.BindingActive {
			bindings = append(bindings, binding)
		}
	}
	slices.SortFunc(bindings, func(left, right channels.Binding) int {
		if left.TenantID != right.TenantID {
			return strings.Compare(left.TenantID, right.TenantID)
		}
		if left.AppID != right.AppID {
			return strings.Compare(left.AppID, right.AppID)
		}
		return strings.Compare(left.BindingID, right.BindingID)
	})
	return bindings, nil
}

// ResolveBindingByPublicRoute returns a binding snapshot located by its
// opaque public route. The channel is checked after route lookup so callers
// cannot use a route from another platform.
func (r *StaticBindingResolver) ResolveBindingByPublicRoute(
	_ context.Context,
	channel channels.Channel,
	publicRouteID string,
) (channels.BindingSnapshot, error) {
	if err := channel.Validate(); err != nil {
		return channels.BindingSnapshot{}, err
	}
	if err := channels.ValidatePublicRouteID(publicRouteID); err != nil {
		return channels.BindingSnapshot{}, err
	}
	snapshot, ok := r.routes[publicRouteID]
	if !ok {
		return channels.BindingSnapshot{}, channels.ErrBindingNotFound
	}
	if snapshot.Channel != channel {
		return channels.BindingSnapshot{}, channels.ErrBindingChannelMismatch
	}
	return snapshot, nil
}

// ValidateAppConfigBindings verifies that each binding referenced by cfg exists
// and belongs to the same tenant application.
func ValidateAppConfigBindings(
	ctx context.Context,
	cfg tenant.AppConfig,
	bindings BindingResolver,
) error {
	if len(cfg.ChannelBinding) == 0 {
		return nil
	}
	if bindings == nil {
		return errors.New("channel binding resolver is required")
	}
	for _, bindingID := range cfg.ChannelBinding {
		binding, err := bindings.ResolveBinding(ctx, cfg.TenantID, cfg.AppID, bindingID)
		if err != nil {
			return err
		}
		if err := binding.Validate(); err != nil {
			return fmt.Errorf("channel binding %q: %w", bindingID, err)
		}
		if binding.TenantID != cfg.TenantID || binding.AppID != cfg.AppID || binding.BindingID != bindingID {
			return fmt.Errorf("channel binding %q does not match app config scope", bindingID)
		}
	}
	return nil
}
