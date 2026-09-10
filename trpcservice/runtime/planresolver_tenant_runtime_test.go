package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestPlanResolverEnsuresTenantRuntimeWithTenantID(t *testing.T) {
	fixture := runtimeFixture(t)
	var ensuredTenantID string
	ensureCalls := 0
	config := testPlanResolverConfig(fixture)
	config.TenantRuntime = &planResolverTenantRuntimeStub{ensure: func(_ context.Context, tenantID string) error {
		ensureCalls++
		ensuredTenantID = tenantID
		return nil
	}}
	resolver, err := NewPlanResolver(config)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := resolver.Resolve(context.Background(), PlanRequest{TenantID: fixture.root.TenantID, AppID: fixture.app.AppID}); err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if ensureCalls != 1 {
		t.Fatalf("TenantRuntime.Ensure calls = %d, want 1", ensureCalls)
	}
	if ensuredTenantID != fixture.root.TenantID {
		t.Fatalf("TenantRuntime.Ensure tenant ID = %q, want %q", ensuredTenantID, fixture.root.TenantID)
	}
}

func TestPlanResolverRedactsTenantRuntimeMaterializationFailure(t *testing.T) {
	fixture := runtimeFixture(t)
	materializerErr := errors.New("provider secret and connection detail")
	config := testPlanResolverConfig(fixture)
	config.TenantRuntime = &planResolverTenantRuntimeStub{ensure: func(context.Context, string) error {
		return materializerErr
	}}
	resolver, err := NewPlanResolver(config)
	if err != nil {
		t.Fatal(err)
	}

	_, err = resolver.Resolve(context.Background(), PlanRequest{TenantID: fixture.root.TenantID, AppID: fixture.app.AppID})
	if !errors.Is(err, ErrPlanUnavailable) {
		t.Fatalf("Resolve() error = %v, want ErrPlanUnavailable", err)
	}
	if errors.Is(err, materializerErr) || strings.Contains(err.Error(), materializerErr.Error()) {
		t.Fatalf("Resolve() leaked materializer detail: %v", err)
	}
}

func TestPlanResolverPropagatesTenantRuntimeCancellation(t *testing.T) {
	fixture := runtimeFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config := testPlanResolverConfig(fixture)
	config.TenantRuntime = &planResolverTenantRuntimeStub{ensure: func(context.Context, string) error {
		cancel()
		return errors.New("materializer stopped")
	}}
	resolver, err := NewPlanResolver(config)
	if err != nil {
		t.Fatal(err)
	}

	_, err = resolver.Resolve(ctx, PlanRequest{TenantID: fixture.root.TenantID, AppID: fixture.app.AppID})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Resolve() error = %v, want context.Canceled", err)
	}
	if errors.Is(err, ErrPlanUnavailable) {
		t.Fatalf("Resolve() returned redacted failure after cancellation: %v", err)
	}
}

func TestPlanResolverRedactsMissingRepositoryInputs(t *testing.T) {
	fixture := runtimeFixture(t)
	tests := []struct {
		name      string
		configure func(*PlanResolverConfig)
	}{
		{
			name: "tenant repository returns nil",
			configure: func(config *PlanResolverConfig) {
				config.Tenants = testPlanTenantRepository{}
			},
		},
		{
			name: "app repository returns nil",
			configure: func(config *PlanResolverConfig) {
				config.Apps = testPlanAppRepository{revision: fixture.revision}
			},
		},
		{
			name: "app has no current revision",
			configure: func(config *PlanResolverConfig) {
				appRoot := fixture.app.Clone()
				appRoot.CurrentRevision = nil
				config.Apps = testPlanAppRepository{app: &appRoot, revision: fixture.revision}
			},
		},
		{
			name: "revision repository returns nil",
			configure: func(config *PlanResolverConfig) {
				config.Apps = testPlanAppRepository{app: fixture.app}
			},
		},
		{
			name: "model repository returns nil",
			configure: func(config *PlanResolverConfig) {
				config.Models = testPlanModelRepository{}
			},
		},
		{
			name: "tenant has no default backend",
			configure: func(config *PlanResolverConfig) {
				root := fixture.root.Clone()
				root.DefaultBackendProfileID = nil
				config.Tenants = testPlanTenantRepository{value: &root}
			},
		},
		{
			name: "backend repository returns nil",
			configure: func(config *PlanResolverConfig) {
				config.Backends = testPlanBackendRepository{}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := testPlanResolverConfig(fixture)
			test.configure(&config)
			resolver, err := NewPlanResolver(config)
			if err != nil {
				t.Fatal(err)
			}

			_, err = resolver.Resolve(context.Background(), PlanRequest{TenantID: fixture.root.TenantID, AppID: fixture.app.AppID})
			if !errors.Is(err, ErrPlanUnavailable) {
				t.Fatalf("Resolve() error = %v, want ErrPlanUnavailable", err)
			}
		})
	}
}

type planResolverTenantRuntimeStub struct {
	ensure func(context.Context, string) error
}

func (runtime *planResolverTenantRuntimeStub) Ensure(ctx context.Context, tenantID string) error {
	return runtime.ensure(ctx, tenantID)
}

var _ TenantRuntime = (*planResolverTenantRuntimeStub)(nil)
