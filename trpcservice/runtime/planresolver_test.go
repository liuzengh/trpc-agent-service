package runtime

import (
	"context"
	"errors"
	"strings"
	"testing"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestPlanResolverResolvesNeutralPlanRequest(t *testing.T) {
	fixture := runtimeFixture(t)
	resolver, err := NewPlanResolver(testPlanResolverConfig(fixture))
	if err != nil {
		t.Fatal(err)
	}
	if !resolver.Ready() {
		t.Fatal("resolver is not ready with complete dependencies")
	}

	plan, err := resolver.Resolve(context.Background(), PlanRequest{TenantID: fixture.root.TenantID, AppID: fixture.app.AppID})
	if err != nil {
		t.Fatal(err)
	}
	key, err := plan.CacheKey()
	if err != nil {
		t.Fatal(err)
	}
	if key.TenantID != fixture.root.TenantID || key.AppID != fixture.app.AppID || key.Revision != fixture.revision.Revision || key.ModelProfileID != fixture.modelProfile.ProfileID || key.BackendProfileID != fixture.backendProfile.ProfileID {
		t.Fatalf("unexpected plan cache key: %+v", key)
	}
}

func TestPlanResolverUsesCanaryRevision(t *testing.T) {
	fixture := runtimeFixture(t)
	current, candidate := fixture.revision.Revision, fixture.revision.Revision+1
	appRoot := fixture.app.Clone()
	appRoot.CurrentRevision = &current
	appRoot.CanaryRevision = &candidate
	if err := appRoot.Validate(); err != nil {
		t.Fatal(err)
	}
	candidateRevision := fixture.revision.Clone()
	candidateRevision.Revision = candidate
	config := testPlanResolverConfig(fixture)
	config.Apps = testPlanAppRepository{app: &appRoot, revision: &candidateRevision}

	resolver, err := NewPlanResolver(config)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := resolver.Resolve(context.Background(), PlanRequest{TenantID: fixture.root.TenantID, AppID: appRoot.AppID})
	if err != nil {
		t.Fatal(err)
	}
	if got := plan.AgentSnapshot().Revision().Revision; got != candidate {
		t.Fatalf("resolved revision = %d, want canary revision %d", got, candidate)
	}
}

func TestPlanResolverValidatesRuntimeBoundaryAndRedactsFailures(t *testing.T) {
	if _, err := NewPlanResolver(PlanResolverConfig{}); !errors.Is(err, ErrInvalidPlanResolverConfig) {
		t.Fatalf("missing resolver dependencies error = %v", err)
	}
	var nilResolver *PlanResolver
	if nilResolver.Ready() {
		t.Fatal("nil resolver is ready")
	}
	if _, err := nilResolver.Resolve(context.Background(), PlanRequest{}); !errors.Is(err, ErrPlanResolverNotReady) {
		t.Fatalf("nil resolver error = %v", err)
	}

	fixture := runtimeFixture(t)
	resolver, err := NewPlanResolver(testPlanResolverConfig(fixture))
	if err != nil {
		t.Fatal(err)
	}
	for name, request := range map[string]PlanRequest{
		"missing tenant": {AppID: fixture.app.AppID},
		"missing app":    {TenantID: fixture.root.TenantID},
		"whitespace tenant": {
			TenantID: " \t", AppID: fixture.app.AppID,
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := resolver.Resolve(context.Background(), request); !errors.Is(err, ErrInvalidPlanRequest) {
				t.Fatalf("Resolve() error = %v, want ErrInvalidPlanRequest", err)
			}
		})
	}
	var nilContext context.Context
	if _, err := resolver.Resolve(nilContext, PlanRequest{TenantID: fixture.root.TenantID, AppID: fixture.app.AppID}); !errors.Is(err, ErrInvalidPlanRequest) {
		t.Fatalf("nil context error = %v", err)
	}

	failedConfig := testPlanResolverConfig(fixture)
	failedConfig.Tenants = testPlanTenantRepository{err: errors.New("tenant-secret-provider-detail")}
	failedResolver, err := NewPlanResolver(failedConfig)
	if err != nil {
		t.Fatal(err)
	}
	_, err = failedResolver.Resolve(context.Background(), PlanRequest{TenantID: fixture.root.TenantID, AppID: fixture.app.AppID})
	if !errors.Is(err, ErrPlanUnavailable) {
		t.Fatalf("repository failure = %v, want ErrPlanUnavailable", err)
	}
	if strings.Contains(err.Error(), "secret-provider-detail") {
		t.Fatalf("Resolve() leaked repository detail: %v", err)
	}
}

func TestPlanResolverPreservesCancellationAfterRepositoryStep(t *testing.T) {
	fixture := runtimeFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config := testPlanResolverConfig(fixture)
	config.Tenants = testPlanTenantRepository{value: fixture.root, afterGet: cancel}
	resolver, err := NewPlanResolver(config)
	if err != nil {
		t.Fatal(err)
	}
	_, err = resolver.Resolve(ctx, PlanRequest{TenantID: fixture.root.TenantID, AppID: fixture.app.AppID})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Resolve() error = %v, want context.Canceled", err)
	}
}

func testPlanResolverConfig(fixture runtimeFixtureData) PlanResolverConfig {
	return PlanResolverConfig{
		Tenants:        testPlanTenantRepository{value: fixture.root},
		Apps:           testPlanAppRepository{app: fixture.app, revision: fixture.revision},
		Models:         testPlanModelRepository{profile: fixture.modelProfile},
		Backends:       testPlanBackendRepository{profile: fixture.backendProfile},
		ModelCatalog:   fixture.modelCatalog,
		BackendCatalog: fixture.backendCatalog,
	}
}

type testPlanTenantRepository struct {
	tenant.Repository
	value    *tenant.Tenant
	err      error
	afterGet func()
}

func (repository testPlanTenantRepository) Get(context.Context, string) (*tenant.Tenant, error) {
	if repository.afterGet != nil {
		repository.afterGet()
	}
	if repository.err != nil {
		return nil, repository.err
	}
	if repository.value == nil {
		return nil, nil
	}
	value := repository.value.Clone()
	return &value, nil
}

type testPlanAppRepository struct {
	appmodel.Repository
	app      *appmodel.App
	revision *appmodel.Revision
}

func (repository testPlanAppRepository) Get(context.Context, string, string) (*appmodel.App, error) {
	if repository.app == nil {
		return nil, nil
	}
	value := repository.app.Clone()
	return &value, nil
}

func (repository testPlanAppRepository) GetRevision(context.Context, string, string, int64) (*appmodel.Revision, error) {
	if repository.revision == nil {
		return nil, nil
	}
	value := repository.revision.Clone()
	return &value, nil
}

type testPlanModelRepository struct {
	modelprofile.Repository
	profile *modelprofile.Profile
}

func (repository testPlanModelRepository) Get(context.Context, string, string) (*modelprofile.Profile, error) {
	if repository.profile == nil {
		return nil, nil
	}
	value := repository.profile.Clone()
	return &value, nil
}

type testPlanBackendRepository struct {
	backend.Repository
	profile *backend.Profile
}

func (repository testPlanBackendRepository) Get(context.Context, string, string) (*backend.Profile, error) {
	if repository.profile == nil {
		return nil, nil
	}
	value := repository.profile.Clone()
	return &value, nil
}
