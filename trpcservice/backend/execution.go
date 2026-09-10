package backend

import (
	"context"
	"fmt"

	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

type executionSnapshotContextKey struct{}

// BackendExecutionSnapshot is the sealed, immutable backend selection for one
// Worker execution. It contains no resolved credentials or live clients.
type BackendExecutionSnapshot struct {
	tenant  *tenant.Tenant
	profile *Profile
	catalog *ProviderCatalog
}

// FactoryCacheKey is the comparable identity for one materialized storage set.
type FactoryCacheKey struct {
	TenantID       string
	TenantVersion  int64
	ProfileID      string
	ProfileVersion int64
	ContentDigest  string
}

// StorageFactoryInput is the provider-neutral, secret-free configuration
// boundary consumed by later storage adapters and a trusted Secret Resolver.
type StorageFactoryInput struct {
	TenantID       string
	TenantVersion  int64
	ProfileID      string
	ProfileKey     string
	ProfileVersion int64
	ContentDigest  string
	SchemaVersion  int
	Bindings       []CapabilityBinding
}

// Clone returns a defensive copy of Factory input slices and maps.
func (input StorageFactoryInput) Clone() StorageFactoryInput {
	clone := input
	clone.Bindings = cloneBindings(input.Bindings)
	return clone
}

// NewBackendExecutionSnapshot validates and freezes the Tenant's selected
// active Profile using the same immutable Catalog that accepted it.
func NewBackendExecutionSnapshot(tenantSnapshot tenant.ConfigurationSnapshot, profile *Profile, catalog *ProviderCatalog) (BackendExecutionSnapshot, error) {
	tenantValue := tenantSnapshot.Tenant()
	if err := validateExecutionState(tenantValue, profile, catalog); err != nil {
		return BackendExecutionSnapshot{}, err
	}
	tenantCopy := tenantValue.Clone()
	profileCopy := profile.Clone()
	return BackendExecutionSnapshot{tenant: &tenantCopy, profile: &profileCopy, catalog: catalog}, nil
}

func validateExecutionState(tenantValue tenant.Tenant, profile *Profile, catalog *ProviderCatalog) error {
	if err := tenantValue.Validate(); err != nil {
		return fmt.Errorf("%w: invalid tenant snapshot: %v", ErrInvalid, err)
	}
	if !tenantValue.CanAcceptExecution() {
		return fmt.Errorf("%w: tenant status %q cannot accept execution", ErrInvalid, tenantValue.Status)
	}
	if profile == nil {
		return fmt.Errorf("%w: Backend Profile snapshot is required", ErrInvalid)
	}
	if catalog == nil {
		return fmt.Errorf("%w: Provider Catalog is required", ErrInvalid)
	}
	if err := profile.Validate(catalog); err != nil {
		return fmt.Errorf("%w: invalid Backend Profile snapshot: %v", ErrInvalid, err)
	}
	if tenantValue.TenantID != profile.TenantID {
		return fmt.Errorf("%w: Tenant and Backend Profile scopes must match", ErrInvalid)
	}
	if tenantValue.DefaultBackendProfileID == nil || *tenantValue.DefaultBackendProfileID != profile.ProfileID {
		return fmt.Errorf("%w: Backend Profile is not the Tenant default selection", ErrInvalid)
	}
	if !profile.CanAcceptExecution() {
		return fmt.Errorf("%w: Backend Profile status %q cannot accept execution", ErrInvalid, profile.Status)
	}
	return nil
}

// Tenant returns a defensive copy of the fixed Tenant version.
func (snapshot BackendExecutionSnapshot) Tenant() tenant.Tenant {
	if snapshot.tenant == nil {
		return tenant.Tenant{}
	}
	return snapshot.tenant.Clone()
}

// Profile returns a defensive copy of the fixed Backend Profile.
func (snapshot BackendExecutionSnapshot) Profile() Profile {
	if snapshot.profile == nil {
		return Profile{}
	}
	return snapshot.profile.Clone()
}

// CacheKey returns the stable storage Factory cache identity.
func (snapshot BackendExecutionSnapshot) CacheKey() (FactoryCacheKey, error) {
	if err := snapshot.validate(); err != nil {
		return FactoryCacheKey{}, err
	}
	return FactoryCacheKey{
		TenantID: snapshot.tenant.TenantID, TenantVersion: snapshot.tenant.Version,
		ProfileID: snapshot.profile.ProfileID, ProfileVersion: snapshot.profile.Version,
		ContentDigest: snapshot.profile.ContentDigest,
	}, nil
}

// FactoryInput maps the sealed state into the only allowed later adapter input.
func (snapshot BackendExecutionSnapshot) FactoryInput() (StorageFactoryInput, error) {
	if err := snapshot.validate(); err != nil {
		return StorageFactoryInput{}, err
	}
	return StorageFactoryInput{
		TenantID: snapshot.tenant.TenantID, TenantVersion: snapshot.tenant.Version,
		ProfileID: snapshot.profile.ProfileID, ProfileKey: snapshot.profile.ProfileKey,
		ProfileVersion: snapshot.profile.Version, ContentDigest: snapshot.profile.ContentDigest,
		SchemaVersion: snapshot.profile.SchemaVersion, Bindings: cloneBindings(snapshot.profile.Bindings),
	}, nil
}

// WithBackendExecutionSnapshot carries a validated defensive copy for one
// execution. Invalid or zero snapshots overwrite the key with an empty value.
func WithBackendExecutionSnapshot(ctx context.Context, snapshot BackendExecutionSnapshot) context.Context {
	if err := snapshot.validate(); err != nil {
		return context.WithValue(ctx, executionSnapshotContextKey{}, BackendExecutionSnapshot{})
	}
	return context.WithValue(ctx, executionSnapshotContextKey{}, snapshot.clone())
}

// BackendExecutionSnapshotFromContext returns a validated defensive copy.
func BackendExecutionSnapshotFromContext(ctx context.Context) (BackendExecutionSnapshot, bool) {
	snapshot, ok := ctx.Value(executionSnapshotContextKey{}).(BackendExecutionSnapshot)
	if !ok || snapshot.validate() != nil {
		return BackendExecutionSnapshot{}, false
	}
	return snapshot.clone(), true
}

func (snapshot BackendExecutionSnapshot) validate() error {
	if snapshot.tenant == nil || snapshot.profile == nil || snapshot.catalog == nil {
		return fmt.Errorf("%w: Backend execution snapshot is not initialized", ErrInvalid)
	}
	return validateExecutionState(*snapshot.tenant, snapshot.profile, snapshot.catalog)
}

func (snapshot BackendExecutionSnapshot) clone() BackendExecutionSnapshot {
	tenantCopy := snapshot.tenant.Clone()
	profileCopy := snapshot.profile.Clone()
	return BackendExecutionSnapshot{tenant: &tenantCopy, profile: &profileCopy, catalog: snapshot.catalog}
}
