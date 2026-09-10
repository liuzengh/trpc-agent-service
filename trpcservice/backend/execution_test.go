package backend

import (
	"context"
	"errors"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestBackendExecutionSnapshotFreezesFactoryInputAndCacheIdentity(t *testing.T) {
	tenantRoot, tenantSnapshot, profile, catalog := backendExecutionFixture(t)
	snapshot, err := NewBackendExecutionSnapshot(tenantSnapshot, profile, catalog)
	if err != nil {
		t.Fatal(err)
	}
	input := assertBackendSnapshotCacheAndInput(t, snapshot, tenantRoot, profile)
	assertBackendSnapshotDefensiveCopies(t, snapshot, tenantRoot, profile, input)
}

func assertBackendSnapshotCacheAndInput(t *testing.T, snapshot BackendExecutionSnapshot, tenantRoot *tenant.Tenant, profile *Profile) StorageFactoryInput {
	t.Helper()
	key, err := snapshot.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	if key.TenantID != tenantRoot.TenantID || key.TenantVersion != tenantRoot.Version ||
		key.ProfileID != profile.ProfileID || key.ProfileVersion != profile.Version || key.ContentDigest != profile.ContentDigest {
		t.Fatalf("unexpected cache identity: %+v", key)
	}
	input, err := snapshot.FactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	if input.TenantID != tenantRoot.TenantID || input.TenantVersion != tenantRoot.Version || input.ProfileID != profile.ProfileID ||
		input.ProfileKey != profile.ProfileKey || input.ProfileVersion != profile.Version || input.ContentDigest != profile.ContentDigest ||
		input.SchemaVersion != profile.SchemaVersion || len(input.Bindings) != 1 || input.Bindings[0].SecretRef != "secret://tenant/database" {
		t.Fatalf("unexpected Factory input: %+v", input)
	}
	return input
}

func assertBackendSnapshotDefensiveCopies(t *testing.T, snapshot BackendExecutionSnapshot, tenantRoot *tenant.Tenant, profile *Profile, input StorageFactoryInput) {
	t.Helper()
	*tenantRoot.DefaultBackendProfileID = "source-tenant-mutation"
	profile.Bindings[0].Options["database"] = "source-profile-mutation"
	if got := snapshot.Tenant().DefaultBackendProfileID; got == nil || *got == "source-tenant-mutation" {
		t.Fatal("snapshot retained mutable Tenant source")
	}
	if snapshot.Profile().Bindings[0].Options["database"] == "source-profile-mutation" {
		t.Fatal("snapshot retained mutable Profile source")
	}

	input.Bindings[0].Options["database"] = "caller-mutation"
	again, err := snapshot.FactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	if again.Bindings[0].Options["database"] == "caller-mutation" {
		t.Fatal("Factory input exposed mutable snapshot state")
	}
	clone := again.Clone()
	clone.Bindings[0].Options["database"] = "clone-mutation"
	if again.Bindings[0].Options["database"] == "clone-mutation" {
		t.Fatal("Factory input Clone leaked nested map mutation")
	}
}

func TestBackendExecutionSnapshotContextIsSealedAndDefensive(t *testing.T) {
	_, tenantSnapshot, profile, catalog := backendExecutionFixture(t)
	snapshot, err := NewBackendExecutionSnapshot(tenantSnapshot, profile, catalog)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithBackendExecutionSnapshot(context.Background(), snapshot)
	fromContext, ok := BackendExecutionSnapshotFromContext(ctx)
	if !ok {
		t.Fatal("valid Backend snapshot was not carried by Context")
	}
	mutable := fromContext.Profile()
	mutable.Bindings[0].Options["database"] = "context-mutation"
	again, ok := BackendExecutionSnapshotFromContext(ctx)
	if !ok || again.Profile().Bindings[0].Options["database"] == "context-mutation" {
		t.Fatal("Context exposed mutable Backend execution state")
	}

	ctx = WithBackendExecutionSnapshot(ctx, BackendExecutionSnapshot{})
	if invalid, ok := BackendExecutionSnapshotFromContext(ctx); ok || invalid.Profile().ProfileID != "" {
		t.Fatalf("zero snapshot entered trusted Context: %+v", invalid)
	}
	if _, ok := BackendExecutionSnapshotFromContext(context.Background()); ok {
		t.Fatal("Context without Backend snapshot was accepted")
	}

	corrupt := snapshot.clone()
	corrupt.profile.ContentDigest = "bad"
	ctx = WithBackendExecutionSnapshot(context.Background(), corrupt)
	if _, ok := BackendExecutionSnapshotFromContext(ctx); ok {
		t.Fatal("corrupt snapshot entered trusted Context")
	}
}

func TestBackendExecutionSnapshotRejectsInvalidAdmissionState(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*tenant.Tenant, *Profile, **ProviderCatalog)
		zero   bool
	}{
		{name: "zero tenant snapshot", zero: true},
		{name: "nil Profile", mutate: func(_ *tenant.Tenant, profile *Profile, _ **ProviderCatalog) { *profile = Profile{} }},
		{name: "nil Catalog", mutate: func(_ *tenant.Tenant, _ *Profile, catalog **ProviderCatalog) { *catalog = nil }},
		{name: "digest mismatch", mutate: func(_ *tenant.Tenant, profile *Profile, _ **ProviderCatalog) { profile.ContentDigest = "bad" }},
		{name: "tenant scope mismatch", mutate: func(_ *tenant.Tenant, profile *Profile, _ **ProviderCatalog) { profile.TenantID = tenantTwoForRuntime }},
		{name: "missing default", mutate: func(root *tenant.Tenant, _ *Profile, _ **ProviderCatalog) { root.DefaultBackendProfileID = nil }},
		{name: "different default", mutate: func(root *tenant.Tenant, _ *Profile, _ **ProviderCatalog) {
			root.DefaultBackendProfileID = stringPointerForRuntime("bp_01J1K9ZQTVE4PAWF1TSB2WMHNP")
		}},
		{name: "suspended Profile", mutate: func(_ *tenant.Tenant, profile *Profile, _ **ProviderCatalog) { profile.Status = StatusSuspended }},
		{name: "disabled Profile", mutate: func(_ *tenant.Tenant, profile *Profile, _ **ProviderCatalog) { profile.Status = StatusDisabled }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root, tenantSnapshot, profile, catalog := backendExecutionFixture(t)
			if test.mutate != nil {
				test.mutate(root, profile, &catalog)
				if root.DefaultBackendProfileID == nil || *root.DefaultBackendProfileID != profile.ProfileID {
					var err error
					tenantSnapshot, err = tenant.NewConfigurationSnapshot(root)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			if test.zero {
				tenantSnapshot = tenant.ConfigurationSnapshot{}
			}
			var candidate *Profile
			if test.name != "nil Profile" {
				candidate = profile
			}
			if _, err := NewBackendExecutionSnapshot(tenantSnapshot, candidate, catalog); !errors.Is(err, ErrInvalid) {
				t.Fatalf("expected admission rejection, got %v", err)
			}
		})
	}
}

func TestBackendExecutionSnapshotRejectsCatalogAndSessionMismatch(t *testing.T) {
	_, tenantSnapshot, profile, _ := backendExecutionFixture(t)
	otherCatalog, err := NewProviderCatalog(ProviderSpec{
		Provider: "inmemory", Capabilities: []Capability{CapabilitySession},
		EndpointPolicy: FieldForbidden, SecretRefPolicy: FieldForbidden,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewBackendExecutionSnapshot(tenantSnapshot, profile, otherCatalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("mismatched Catalog error = %v", err)
	}

	root, _, _, catalog := backendExecutionFixture(t)
	suspended, err := NewProfile(CreateInput{
		TenantID: root.TenantID, ProfileKey: "memory-only", DisplayName: "Memory Only", Status: StatusSuspended,
		Bindings: []CapabilityBinding{{Capability: CapabilityMemory, Provider: "inmemory"}},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	suspended.Status = StatusActive
	suspended.ContentDigest = contentDigest(suspended.SchemaVersion, suspended.Bindings)
	root.DefaultBackendProfileID = stringPointerForRuntime(suspended.ProfileID)
	tenantSnapshot, err = tenant.NewConfigurationSnapshot(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewBackendExecutionSnapshot(tenantSnapshot, suspended, catalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("active Profile without Session error = %v", err)
	}
}

func TestBackendExecutionSnapshotRequiresActiveTenantBoundary(t *testing.T) {
	root, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "inactive-backend", DisplayName: "Inactive", Status: tenant.StatusSuspended,
		AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tenant.NewConfigurationSnapshot(root); !errors.Is(err, tenant.ErrInvalid) {
		t.Fatalf("inactive Tenant entered protected snapshot boundary: %v", err)
	}
}

func TestZeroBackendExecutionSnapshotCannotProduceFactoryState(t *testing.T) {
	var snapshot BackendExecutionSnapshot
	if snapshot.Tenant().TenantID != "" || snapshot.Profile().ProfileID != "" {
		t.Fatal("zero snapshot accessors returned trusted-looking state")
	}
	if _, err := snapshot.CacheKey(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero snapshot produced cache key: %v", err)
	}
	if _, err := snapshot.FactoryInput(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero snapshot produced Factory input: %v", err)
	}
}

const tenantTwoForRuntime = "t_01J1K9ZQTVE4PAWF1TSB2WMHNQ"

func backendExecutionFixture(t *testing.T) (*tenant.Tenant, tenant.ConfigurationSnapshot, *Profile, *ProviderCatalog) {
	t.Helper()
	catalog := newTestCatalog(t)
	root, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "backend-execution", DisplayName: "Backend Execution",
		AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := NewProfile(CreateInput{
		TenantID: root.TenantID, ProfileKey: "primary", DisplayName: "Primary", Bindings: sessionBinding(),
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	root.DefaultBackendProfileID = stringPointerForRuntime(profile.ProfileID)
	root.Version++
	root.UpdatedAt = root.UpdatedAt.Add(1)
	tenantSnapshot, err := tenant.NewConfigurationSnapshot(root)
	if err != nil {
		t.Fatal(err)
	}
	return root, tenantSnapshot, profile, catalog
}

func stringPointerForRuntime(value string) *string { return &value }
