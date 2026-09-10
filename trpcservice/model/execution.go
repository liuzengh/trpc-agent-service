package model

import (
	"context"
	"fmt"

	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
	trpcmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

type executionSnapshotContextKey struct{}

// FactoryCacheKey is the comparable identity for one materialized model.
// It contains no Secret value or live client.
type FactoryCacheKey struct {
	TenantID       string
	TenantVersion  int64
	ProfileID      string
	ProfileVersion int64
	ContentDigest  string
}

// ModelFactoryInput is the provider-neutral, secret-free boundary consumed by
// a trusted ModelFactory and a SecretResolver.
type ModelFactoryInput struct {
	TenantID       string
	TenantVersion  int64
	ProfileID      string
	ProfileKey     string
	ProfileVersion int64
	ContentDigest  string
	SchemaVersion  int
	Provider       string
	Model          string
	Endpoint       string
	Options        map[string]string
	SecretRef      string
	Generation     GenerationConfig
}

// Clone returns a defensive copy of the factory input.
func (input ModelFactoryInput) Clone() ModelFactoryInput {
	clone := input
	clone.Options = cloneStringMap(input.Options)
	clone.Generation = input.Generation.Clone()
	return clone
}

// ModelExecutionSnapshot is the sealed, immutable Model Profile selection for
// one execution. It contains no credentials or live runtime clients.
type ModelExecutionSnapshot struct {
	tenant  *tenant.Tenant
	profile *Profile
	catalog *ProviderCatalog
}

// NewModelExecutionSnapshot validates and freezes an active Tenant and Model
// Profile in the same trusted provider catalog used for configuration writes.
func NewModelExecutionSnapshot(tenantSnapshot tenant.ConfigurationSnapshot, profile *Profile, catalog *ProviderCatalog) (ModelExecutionSnapshot, error) {
	tenantValue := tenantSnapshot.Tenant()
	if err := validateExecutionState(tenantValue, profile, catalog); err != nil {
		return ModelExecutionSnapshot{}, err
	}
	tenantCopy := tenantValue.Clone()
	profileCopy := profile.Clone()
	return ModelExecutionSnapshot{tenant: &tenantCopy, profile: &profileCopy, catalog: catalog}, nil
}

func validateExecutionState(tenantValue tenant.Tenant, profile *Profile, catalog *ProviderCatalog) error {
	if err := tenantValue.Validate(); err != nil {
		return fmt.Errorf("%w: invalid tenant snapshot", ErrInvalid)
	}
	if !tenantValue.CanAcceptExecution() {
		return fmt.Errorf("%w: tenant cannot accept execution", ErrInvalid)
	}
	if profile == nil {
		return fmt.Errorf("%w: Model Profile snapshot is required", ErrInvalid)
	}
	if catalog == nil {
		return fmt.Errorf("%w: Provider Catalog is required", ErrInvalid)
	}
	if err := profile.Validate(catalog); err != nil {
		return fmt.Errorf("%w: invalid Model Profile snapshot", ErrInvalid)
	}
	if tenantValue.TenantID != profile.TenantID {
		return fmt.Errorf("%w: Tenant and Model Profile scopes must match", ErrInvalid)
	}
	if !profile.CanAcceptExecution() {
		return fmt.Errorf("%w: Model Profile cannot accept execution", ErrInvalid)
	}
	return nil
}

// Tenant returns a defensive copy of the fixed Tenant version.
func (snapshot ModelExecutionSnapshot) Tenant() tenant.Tenant {
	if snapshot.tenant == nil {
		return tenant.Tenant{}
	}
	return snapshot.tenant.Clone()
}

// Profile returns a defensive copy of the fixed Model Profile.
func (snapshot ModelExecutionSnapshot) Profile() Profile {
	if snapshot.profile == nil {
		return Profile{}
	}
	return snapshot.profile.Clone()
}

// CacheKey returns the stable model Factory cache identity.
func (snapshot ModelExecutionSnapshot) CacheKey() (FactoryCacheKey, error) {
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
func (snapshot ModelExecutionSnapshot) FactoryInput() (ModelFactoryInput, error) {
	if err := snapshot.validate(); err != nil {
		return ModelFactoryInput{}, err
	}
	configuration := snapshot.profile.Configuration.Clone()
	return ModelFactoryInput{
		TenantID: snapshot.tenant.TenantID, TenantVersion: snapshot.tenant.Version,
		ProfileID: snapshot.profile.ProfileID, ProfileKey: snapshot.profile.ProfileKey,
		ProfileVersion: snapshot.profile.Version, ContentDigest: snapshot.profile.ContentDigest,
		SchemaVersion: snapshot.profile.SchemaVersion, Provider: configuration.Provider,
		Model: configuration.Model, Endpoint: configuration.Endpoint,
		Options: cloneStringMap(configuration.Options), SecretRef: configuration.SecretRef,
		Generation: configuration.Generation.Clone(),
	}, nil
}

// WithModelExecutionSnapshot carries a validated defensive copy for one
// execution. Invalid or zero snapshots overwrite the context value.
func WithModelExecutionSnapshot(ctx context.Context, snapshot ModelExecutionSnapshot) context.Context {
	if err := snapshot.validate(); err != nil {
		return context.WithValue(ctx, executionSnapshotContextKey{}, ModelExecutionSnapshot{})
	}
	return context.WithValue(ctx, executionSnapshotContextKey{}, snapshot.clone())
}

// ModelExecutionSnapshotFromContext returns a validated defensive copy.
func ModelExecutionSnapshotFromContext(ctx context.Context) (ModelExecutionSnapshot, bool) {
	snapshot, ok := ctx.Value(executionSnapshotContextKey{}).(ModelExecutionSnapshot)
	if !ok || snapshot.validate() != nil {
		return ModelExecutionSnapshot{}, false
	}
	return snapshot.clone(), true
}

func (snapshot ModelExecutionSnapshot) validate() error {
	if snapshot.tenant == nil || snapshot.profile == nil || snapshot.catalog == nil {
		return fmt.Errorf("%w: execution snapshot is not initialized", ErrInvalid)
	}
	return validateExecutionState(*snapshot.tenant, snapshot.profile, snapshot.catalog)
}

func (snapshot ModelExecutionSnapshot) clone() ModelExecutionSnapshot {
	tenantCopy := snapshot.tenant.Clone()
	profileCopy := snapshot.profile.Clone()
	return ModelExecutionSnapshot{tenant: &tenantCopy, profile: &profileCopy, catalog: snapshot.catalog}
}

// SecretScope is the only accepted lookup key for a secret. TenantID is
// always required, even when the underlying provider uses a URI-like ref.
type SecretScope struct {
	TenantID  string
	SecretRef string
}

// Validate checks the explicit tenant and opaque reference boundary.
func (scope SecretScope) Validate() error {
	if err := validateTenantID(scope.TenantID); err != nil {
		return fmt.Errorf("%w: tenant scope is invalid", ErrInvalid)
	}
	if scope.SecretRef == "" {
		return fmt.Errorf("%w: secret reference is required", ErrInvalid)
	}
	if _, err := normalizeSecretRef(scope.SecretRef, FieldRequired); err != nil {
		return fmt.Errorf("%w: secret scope is invalid", ErrInvalid)
	}
	return nil
}

// SecretValue is a temporary credential handed directly to a ModelFactory.
// Its String representation is always redacted.
type SecretValue struct{ value string }

// NewSecretValue creates a fake/provider value for a trusted resolver. The
// value is deliberately not serializable through this type's public API.
func NewSecretValue(value string) (SecretValue, error) {
	if value == "" {
		return SecretValue{}, fmt.Errorf("%w: secret value is empty", ErrInvalid)
	}
	return SecretValue{value: value}, nil
}

// Value returns the underlying value to a ModelFactory. Callers must not
// persist, log, wrap, or place the return value in an execution object.
func (secret SecretValue) Value() string { return secret.value }

// String prevents accidental credential disclosure in formatted errors or
// diagnostics.
func (secret SecretValue) String() string {
	if secret.value == "" {
		return "<empty-secret>"
	}
	return "<redacted-secret>"
}

// SecretResolver resolves one secret inside an explicit tenant scope.
type SecretResolver interface {
	Resolve(context.Context, SecretScope) (SecretValue, error)
}

// ModelFactory constructs an upstream model from secret-free configuration and
// a temporary SecretValue. Production implementations must not retain or log
// the SecretValue.
type ModelFactory interface {
	New(context.Context, ModelFactoryInput, SecretValue) (trpcmodel.Model, error)
}

// Validate checks the provider-neutral ModelFactory input before a runtime
// adapter is allowed to resolve credentials or construct a client.
func (input ModelFactoryInput) Validate() error {
	if err := validateTenantID(input.TenantID); err != nil {
		return err
	}
	if err := validateProfileID(input.ProfileID); err != nil {
		return err
	}
	if input.TenantVersion < 1 || input.ProfileVersion < 1 || input.SchemaVersion != SchemaVersionV1 || input.ContentDigest == "" || input.Provider == "" || input.Model == "" {
		return fmt.Errorf("%w: factory input is incomplete", ErrInvalid)
	}
	if input.SecretRef != "" {
		if _, err := normalizeSecretRef(input.SecretRef, FieldRequired); err != nil {
			return err
		}
	}
	if _, err := normalizeGeneration(input.Generation); err != nil {
		return err
	}
	return nil
}
