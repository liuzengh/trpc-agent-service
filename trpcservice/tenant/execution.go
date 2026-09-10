package tenant

import (
	"context"
	"fmt"
	"strconv"
)

type contextKey struct{}

// ConfigurationSnapshot is immutable tenant data for one Worker execution.
// It never contains secrets.
type ConfigurationSnapshot struct {
	tenant *Tenant
}

// Tenant returns a defensive copy of the tenant data captured by the snapshot.
func (s ConfigurationSnapshot) Tenant() Tenant {
	if s.tenant == nil {
		return Tenant{}
	}
	return s.tenant.Clone()
}

func cloneTenant(t *Tenant) *Tenant {
	if t == nil {
		return nil
	}
	clone := t.Clone()
	return &clone
}

// NewConfigurationSnapshot creates a defensive copy for one execution.
// A Gateway must use a tenant ID resolved from an authenticated binding.
func NewConfigurationSnapshot(t *Tenant) (ConfigurationSnapshot, error) {
	if t == nil {
		return ConfigurationSnapshot{}, fmt.Errorf("%w: tenant snapshot is required", ErrInvalid)
	}
	if err := t.Validate(); err != nil {
		return ConfigurationSnapshot{}, err
	}
	if !t.CanAcceptExecution() {
		return ConfigurationSnapshot{}, fmt.Errorf("%w: tenant status %q cannot accept execution", ErrInvalid, t.Status)
	}
	return ConfigurationSnapshot{tenant: cloneTenant(t)}, nil
}

// WithConfigurationSnapshot carries a fixed configuration for one execution.
func WithConfigurationSnapshot(ctx context.Context, snapshot ConfigurationSnapshot) context.Context {
	if snapshot.tenant == nil {
		return context.WithValue(ctx, contextKey{}, ConfigurationSnapshot{})
	}
	return context.WithValue(ctx, contextKey{}, ConfigurationSnapshot{tenant: cloneTenant(snapshot.tenant)})
}

// ConfigurationSnapshotFromContext returns a defensive copy.
func ConfigurationSnapshotFromContext(ctx context.Context) (ConfigurationSnapshot, bool) {
	snapshot, ok := ctx.Value(contextKey{}).(ConfigurationSnapshot)
	if !ok || snapshot.tenant == nil {
		return ConfigurationSnapshot{}, false
	}
	return ConfigurationSnapshot{tenant: cloneTenant(snapshot.tenant)}, true
}

// RunnerIdentity contains collision-free identity values for a Runner.
type RunnerIdentity struct {
	UserID    string
	SessionID string
}

// NewRunnerIdentity namespaces external IDs without ambiguous concatenation.
func NewRunnerIdentity(tenantID, externalUserID, externalSessionID string) (RunnerIdentity, error) {
	if err := validateTenantID(tenantID); err != nil {
		return RunnerIdentity{}, err
	}
	userID, err := namespacedID(tenantID, externalUserID)
	if err != nil {
		return RunnerIdentity{}, err
	}
	sessionID, err := namespacedID(tenantID, externalSessionID)
	if err != nil {
		return RunnerIdentity{}, err
	}
	return RunnerIdentity{UserID: userID, SessionID: sessionID}, nil
}

func namespacedID(tenantID, externalID string) (string, error) {
	if externalID == "" {
		return "", fmt.Errorf("%w: external identity is required", ErrInvalid)
	}
	return strconv.Itoa(len(tenantID)) + ":" + tenantID + ":" + strconv.Itoa(len(externalID)) + ":" + externalID, nil
}
