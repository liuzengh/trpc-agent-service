package model

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestModelExecutionSnapshotFreezesInputAndKeepsSecretOutOfState(t *testing.T) {
	root, tenantSnapshot, profile, catalog := modelExecutionFixture(t, "secret://tenant/model")
	snapshot, err := NewModelExecutionSnapshot(tenantSnapshot, profile, catalog)
	if err != nil {
		t.Fatal(err)
	}
	input := assertModelSnapshotCacheAndInput(t, snapshot, root, profile)
	assertModelSnapshotDefensiveCopies(t, snapshot, profile, input)
	assertModelSnapshotContextBoundary(t, snapshot, profile)
}

func assertModelSnapshotCacheAndInput(t *testing.T, snapshot ModelExecutionSnapshot, root *tenant.Tenant, profile *Profile) ModelFactoryInput {
	t.Helper()
	key, err := snapshot.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	if key.TenantID != root.TenantID || key.TenantVersion != root.Version || key.ProfileID != profile.ProfileID || key.ProfileVersion != profile.Version || key.ContentDigest != profile.ContentDigest {
		t.Fatalf("unexpected model cache key: %+v", key)
	}
	input, err := snapshot.FactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	if input.TenantID != root.TenantID || input.ProfileID != profile.ProfileID || input.Provider != "public" || input.Model != "chat" || input.SecretRef != profile.Configuration.SecretRef {
		t.Fatalf("unexpected model factory input: %+v", input)
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "super-secret") {
		t.Fatal("secret value entered serialized factory input")
	}
	return input
}

func assertModelSnapshotDefensiveCopies(t *testing.T, snapshot ModelExecutionSnapshot, profile *Profile, input ModelFactoryInput) {
	t.Helper()
	profile.Configuration.SecretRef = "secret://other/value"
	if snapshot.Profile().Configuration.SecretRef != "secret://tenant/model" {
		t.Fatal("snapshot retained mutable source Profile")
	}
	input.SecretRef = "caller-mutation"
	again, err := snapshot.FactoryInput()
	if err != nil {
		t.Fatal(err)
	}
	if again.SecretRef != "secret://tenant/model" {
		t.Fatal("Factory input mutation changed snapshot state")
	}
}

func assertModelSnapshotContextBoundary(t *testing.T, snapshot ModelExecutionSnapshot, profile *Profile) {
	t.Helper()
	ctx := WithModelExecutionSnapshot(context.Background(), snapshot)
	fromContext, ok := ModelExecutionSnapshotFromContext(ctx)
	if !ok || fromContext.Profile().ProfileID != profile.ProfileID {
		t.Fatal("valid model snapshot was not carried by context")
	}
	ctx = WithModelExecutionSnapshot(ctx, ModelExecutionSnapshot{})
	if _, ok := ModelExecutionSnapshotFromContext(ctx); ok {
		t.Fatal("zero model snapshot entered context")
	}
}

func TestSecretScopeRejectsMissingOrInvalidTenant(t *testing.T) {
	if err := (SecretScope{SecretRef: "secret://model"}).Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing tenant error = %v", err)
	}
	if err := (SecretScope{TenantID: "other-tenant", SecretRef: "secret://model"}).Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid tenant error = %v", err)
	}
	if err := (SecretScope{TenantID: modelTestTenantOne}).Validate(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("missing secret reference error = %v", err)
	}
}

func TestModelExecutionSnapshotRejectsInvalidStatesAndInputs(t *testing.T) {
	root, tenantSnapshot, profile, catalog := modelExecutionFixture(t, "")
	if _, err := NewModelExecutionSnapshot(tenantSnapshot, profile, catalog); err != nil {
		t.Fatal(err)
	}
	assertInvalidModelSnapshotAccessors(t)
	assertInvalidModelExecutionStates(t, root, profile, catalog)
	assertInvalidModelFactoryInputs(t, root)
}

func assertInvalidModelSnapshotAccessors(t *testing.T) {
	t.Helper()
	if zero := (ModelExecutionSnapshot{}).Tenant(); zero.TenantID != "" {
		t.Fatalf("zero snapshot tenant = %+v", zero)
	}
	if zero := (ModelExecutionSnapshot{}).Profile(); zero.ProfileID != "" {
		t.Fatalf("zero snapshot profile = %+v", zero)
	}
	if _, err := (ModelExecutionSnapshot{}).CacheKey(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero snapshot cache key error = %v", err)
	}
	if _, err := (ModelExecutionSnapshot{}).FactoryInput(); !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero snapshot factory input error = %v", err)
	}
	if _, ok := ModelExecutionSnapshotFromContext(context.Background()); ok {
		t.Fatal("empty context returned a model snapshot")
	}
	if contextWithZero := WithModelExecutionSnapshot(context.Background(), ModelExecutionSnapshot{}); func() bool {
		_, ok := ModelExecutionSnapshotFromContext(contextWithZero)
		return ok
	}() {
		t.Fatal("zero model snapshot entered context")
	}
}

func assertInvalidModelExecutionStates(t *testing.T, root *tenant.Tenant, profile *Profile, catalog *ProviderCatalog) {
	t.Helper()
	if _, err := NewModelExecutionSnapshot(tenant.ConfigurationSnapshot{}, profile, catalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid tenant snapshot error = %v", err)
	}

	invalidTenant := root.Clone()
	invalidTenant.TenantID = "tenant"
	if err := validateExecutionState(invalidTenant, profile, catalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid tenant state error = %v", err)
	}
	inactiveTenant := root.Clone()
	inactiveTenant.Status = tenant.StatusSuspended
	if err := validateExecutionState(inactiveTenant, profile, catalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("inactive tenant state error = %v", err)
	}
	if err := validateExecutionState(*root, nil, catalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil profile state error = %v", err)
	}
	if err := validateExecutionState(*root, profile, nil); !errors.Is(err, ErrInvalid) {
		t.Fatalf("nil catalog state error = %v", err)
	}
	invalidProfile := profile.Clone()
	invalidProfile.ContentDigest = "wrong"
	if err := validateExecutionState(*root, &invalidProfile, catalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid profile state error = %v", err)
	}
	differentTenantProfile := profile.Clone()
	differentTenantProfile.TenantID = "t_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	if err := validateExecutionState(*root, &differentTenantProfile, catalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("cross-tenant profile state error = %v", err)
	}
	inactiveProfile := profile.Clone()
	inactiveProfile.Status = StatusSuspended
	if err := validateExecutionState(*root, &inactiveProfile, catalog); !errors.Is(err, ErrInvalid) {
		t.Fatalf("inactive profile state error = %v", err)
	}
}

func assertInvalidModelFactoryInputs(t *testing.T, root *tenant.Tenant) {
	t.Helper()
	incompleteInputs := []ModelFactoryInput{
		{TenantID: root.TenantID},
		{TenantID: root.TenantID, ProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", TenantVersion: 1, ProfileVersion: 1, SchemaVersion: 99, ContentDigest: "digest", Provider: "fake", Model: "deterministic"},
		{TenantID: root.TenantID, ProfileID: "mp_01ARZ3NDEKTSV4RRFFQ69G5FAV", TenantVersion: 1, ProfileVersion: 1, SchemaVersion: SchemaVersionV1, ContentDigest: "digest", Provider: "fake", Model: "deterministic", SecretRef: "bad ref"},
	}
	for _, incomplete := range incompleteInputs {
		if err := incomplete.Validate(); !errors.Is(err, ErrInvalid) {
			t.Errorf("factory input %+v error = %v", incomplete, err)
		}
	}
}

func modelExecutionFixture(t *testing.T, secretRef string) (*tenant.Tenant, tenant.ConfigurationSnapshot, *Profile, *ProviderCatalog) {
	t.Helper()
	catalog := modelTestCatalog(t)
	root, err := tenant.NewTenant(tenant.CreateInput{
		TenantKey: "model-runtime", DisplayName: "Model Runtime",
		AuditRetentionDays: 30, LogMaskingLevel: tenant.MaskingBasic, TraceSamplingRate: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	profile, err := NewProfile(CreateInput{
		TenantID: root.TenantID, ProfileKey: "primary", DisplayName: "Primary",
		Configuration: Configuration{Provider: "public", Model: "chat", SecretRef: secretRef},
	}, catalog)
	if err != nil {
		t.Fatal(err)
	}
	tenantSnapshot, err := tenant.NewConfigurationSnapshot(root)
	if err != nil {
		t.Fatal(err)
	}
	return root, tenantSnapshot, profile, catalog
}
