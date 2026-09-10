package gateway

import (
	"context"
	"errors"
	"strings"
	"testing"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	"github.com/XnLemon/trpc-agent-service/trpcservice/model"
	serviceruntime "github.com/XnLemon/trpc-agent-service/trpcservice/runtime"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

func TestMapPlanResolverErrorKeepsGatewayErrorBoundary(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want error
	}{
		{name: "resolver not ready", err: serviceruntime.ErrPlanResolverNotReady, want: ErrNotReady},
		{name: "invalid request", err: serviceruntime.ErrInvalidPlanRequest, want: ErrInvalid},
		{name: "canceled", err: context.Canceled, want: context.Canceled},
		{name: "dependency failure", err: errors.New("repository detail"), want: ErrPlanUnavailable},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := mapPlanResolverError(test.err); !errors.Is(got, test.want) {
				t.Fatalf("mapPlanResolverError(%v) = %v, want %v", test.err, got, test.want)
			}
		})
	}
}

func TestPlanResolverNormalizesRepositoryFailures(t *testing.T) {
	tests := []struct {
		name  string
		setup func(gatewayFixture) serviceruntime.PlanResolverConfig
	}{
		{
			name: "tenant repository error",
			setup: func(fixture gatewayFixture) serviceruntime.PlanResolverConfig {
				config := resolverTestConfig(fixture)
				config.Tenants = resolverTenantRepository{
					Repository: fixture.tenants,
					getFn: func(context.Context, string) (*tenant.Tenant, error) {
						return nil, errors.New("tenant-secret-provider-detail")
					},
				}
				return config
			},
		},
		{
			name: "tenant repository returns nil",
			setup: func(fixture gatewayFixture) serviceruntime.PlanResolverConfig {
				config := resolverTestConfig(fixture)
				config.Tenants = resolverTenantRepository{
					Repository: fixture.tenants,
					getFn:      func(context.Context, string) (*tenant.Tenant, error) { return nil, nil },
				}
				return config
			},
		},
		{
			name: "invalid tenant snapshot",
			setup: func(fixture gatewayFixture) serviceruntime.PlanResolverConfig {
				config := resolverTestConfig(fixture)
				config.Tenants = resolverTenantRepository{
					Repository: fixture.tenants,
					getFn:      func(context.Context, string) (*tenant.Tenant, error) { return &tenant.Tenant{}, nil },
				}
				return config
			},
		},
		{
			name: "app repository error",
			setup: func(fixture gatewayFixture) serviceruntime.PlanResolverConfig {
				config := resolverTestConfig(fixture)
				config.Apps = resolverAgentRepository{
					Repository: fixture.apps,
					getFn: func(context.Context, string, string) (*appmodel.App, error) {
						return nil, errors.New("app-secret-provider-detail")
					},
				}
				return config
			},
		},
		{
			name: "app repository returns nil",
			setup: func(fixture gatewayFixture) serviceruntime.PlanResolverConfig {
				config := resolverTestConfig(fixture)
				config.Apps = resolverAgentRepository{
					Repository: fixture.apps,
					getFn:      func(context.Context, string, string) (*appmodel.App, error) { return nil, nil },
				}
				return config
			},
		},
		{
			name: "app has no current revision",
			setup: func(fixture gatewayFixture) serviceruntime.PlanResolverConfig {
				config := resolverTestConfig(fixture)
				config.Apps = resolverAgentRepository{
					Repository: fixture.apps,
					getFn: func(ctx context.Context, tenantID, appID string) (*appmodel.App, error) {
						appValue, err := fixture.apps.Get(ctx, tenantID, appID)
						if err != nil {
							return nil, err
						}
						appCopy := appValue.Clone()
						appCopy.CurrentRevision = nil
						return &appCopy, nil
					},
				}
				return config
			},
		},
		{
			name: "revision repository error",
			setup: func(fixture gatewayFixture) serviceruntime.PlanResolverConfig {
				config := resolverTestConfig(fixture)
				config.Apps = resolverAgentRepository{
					Repository: fixture.apps,
					getRevisionFn: func(context.Context, string, string, int64) (*appmodel.Revision, error) {
						return nil, errors.New("revision-secret-provider-detail")
					},
				}
				return config
			},
		},
		{
			name: "revision repository returns nil",
			setup: func(fixture gatewayFixture) serviceruntime.PlanResolverConfig {
				config := resolverTestConfig(fixture)
				config.Apps = resolverAgentRepository{
					Repository: fixture.apps,
					getRevisionFn: func(context.Context, string, string, int64) (*appmodel.Revision, error) {
						return nil, nil
					},
				}
				return config
			},
		},
		{
			name: "model repository error",
			setup: func(fixture gatewayFixture) serviceruntime.PlanResolverConfig {
				config := resolverTestConfig(fixture)
				config.Models = resolverModelRepository{
					Repository: fixture.models,
					getFn: func(context.Context, string, string) (*model.Profile, error) {
						return nil, errors.New("model-secret-provider-detail")
					},
				}
				return config
			},
		},
		{
			name: "model repository returns nil",
			setup: func(fixture gatewayFixture) serviceruntime.PlanResolverConfig {
				config := resolverTestConfig(fixture)
				config.Models = resolverModelRepository{
					Repository: fixture.models,
					getFn:      func(context.Context, string, string) (*model.Profile, error) { return nil, nil },
				}
				return config
			},
		},
		{
			name: "tenant has no default backend",
			setup: func(fixture gatewayFixture) serviceruntime.PlanResolverConfig {
				config := resolverTestConfig(fixture)
				config.Tenants = resolverTenantRepository{
					Repository: fixture.tenants,
					getFn: func(ctx context.Context, tenantID string) (*tenant.Tenant, error) {
						tenantValue, err := fixture.tenants.Get(ctx, tenantID)
						if err != nil {
							return nil, err
						}
						tenantCopy := tenantValue.Clone()
						tenantCopy.DefaultBackendProfileID = nil
						return &tenantCopy, nil
					},
				}
				return config
			},
		},
		{
			name: "backend repository error",
			setup: func(fixture gatewayFixture) serviceruntime.PlanResolverConfig {
				config := resolverTestConfig(fixture)
				config.Backends = resolverBackendRepository{
					Repository: fixture.backends,
					getFn: func(context.Context, string, string) (*backend.Profile, error) {
						return nil, errors.New("backend-secret-provider-detail")
					},
				}
				return config
			},
		},
		{
			name: "backend repository returns nil",
			setup: func(fixture gatewayFixture) serviceruntime.PlanResolverConfig {
				config := resolverTestConfig(fixture)
				config.Backends = resolverBackendRepository{
					Repository: fixture.backends,
					getFn:      func(context.Context, string, string) (*backend.Profile, error) { return nil, nil },
				}
				return config
			},
		},
		{
			name: "invalid execution plan snapshot",
			setup: func(fixture gatewayFixture) serviceruntime.PlanResolverConfig {
				config := resolverTestConfig(fixture)
				config.Models = resolverModelRepository{
					Repository: fixture.models,
					getFn:      func(context.Context, string, string) (*model.Profile, error) { return &model.Profile{}, nil },
				}
				return config
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newGatewayFixture(t)
			resolver, err := NewPlanResolver(test.setup(fixture))
			if err != nil {
				t.Fatal(err)
			}
			_, err = resolver.Resolve(context.Background(), mustAPIPrincipal(t, fixture.tenant.TenantID, fixture.app.AppID))
			if !errors.Is(err, ErrPlanUnavailable) {
				t.Fatalf("Resolve() error = %v, want ErrPlanUnavailable", err)
			}
			if strings.Contains(err.Error(), "secret-provider-detail") {
				t.Fatalf("Resolve() leaked repository detail: %v", err)
			}
		})
	}
}

func TestPlanResolverReturnsCancellationAfterRepositorySteps(t *testing.T) {
	tests := []struct {
		name  string
		setup func(gatewayFixture, context.CancelFunc) serviceruntime.PlanResolverConfig
	}{
		{
			name: "tenant get",
			setup: func(fixture gatewayFixture, cancel context.CancelFunc) serviceruntime.PlanResolverConfig {
				config := resolverTestConfig(fixture)
				config.Tenants = cancelAfterTenantGet{Repository: fixture.tenants, cancel: cancel}
				return config
			},
		},
		{
			name: "app get",
			setup: func(fixture gatewayFixture, cancel context.CancelFunc) serviceruntime.PlanResolverConfig {
				config := resolverTestConfig(fixture)
				config.Apps = cancelAfterAppGet{Repository: fixture.apps, cancel: cancel}
				return config
			},
		},
		{
			name: "revision get",
			setup: func(fixture gatewayFixture, cancel context.CancelFunc) serviceruntime.PlanResolverConfig {
				config := resolverTestConfig(fixture)
				config.Apps = cancelAfterRevisionGet{Repository: fixture.apps, cancel: cancel}
				return config
			},
		},
		{
			name: "model get",
			setup: func(fixture gatewayFixture, cancel context.CancelFunc) serviceruntime.PlanResolverConfig {
				config := resolverTestConfig(fixture)
				config.Models = cancelAfterModelGet{Repository: fixture.models, cancel: cancel}
				return config
			},
		},
		{
			name: "backend get",
			setup: func(fixture gatewayFixture, cancel context.CancelFunc) serviceruntime.PlanResolverConfig {
				config := resolverTestConfig(fixture)
				config.Backends = cancelAfterBackendGet{Repository: fixture.backends, cancel: cancel}
				return config
			},
		},
		{
			name: "dependency error after cancellation",
			setup: func(fixture gatewayFixture, cancel context.CancelFunc) serviceruntime.PlanResolverConfig {
				config := resolverTestConfig(fixture)
				config.Tenants = resolverTenantRepository{
					Repository: fixture.tenants,
					getFn: func(context.Context, string) (*tenant.Tenant, error) {
						cancel()
						return nil, errors.New("redacted dependency detail")
					},
				}
				return config
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newGatewayFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			resolver, err := NewPlanResolver(test.setup(fixture, cancel))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := resolver.Resolve(ctx, mustAPIPrincipal(t, fixture.tenant.TenantID, fixture.app.AppID)); !errors.Is(err, context.Canceled) {
				t.Fatalf("Resolve() error = %v, want context.Canceled", err)
			}
		})
	}
}

type resolverTenantRepository struct {
	tenant.Repository
	getFn func(context.Context, string) (*tenant.Tenant, error)
}

func (repository resolverTenantRepository) Get(ctx context.Context, tenantID string) (*tenant.Tenant, error) {
	if repository.getFn != nil {
		return repository.getFn(ctx, tenantID)
	}
	return repository.Repository.Get(ctx, tenantID)
}

type resolverAgentRepository struct {
	appmodel.Repository
	getFn         func(context.Context, string, string) (*appmodel.App, error)
	getRevisionFn func(context.Context, string, string, int64) (*appmodel.Revision, error)
}

func (repository resolverAgentRepository) Get(ctx context.Context, tenantID, appID string) (*appmodel.App, error) {
	if repository.getFn != nil {
		return repository.getFn(ctx, tenantID, appID)
	}
	return repository.Repository.Get(ctx, tenantID, appID)
}

func (repository resolverAgentRepository) GetRevision(ctx context.Context, tenantID, appID string, revision int64) (*appmodel.Revision, error) {
	if repository.getRevisionFn != nil {
		return repository.getRevisionFn(ctx, tenantID, appID, revision)
	}
	return repository.Repository.GetRevision(ctx, tenantID, appID, revision)
}

type resolverModelRepository struct {
	model.Repository
	getFn func(context.Context, string, string) (*model.Profile, error)
}

func (repository resolverModelRepository) Get(ctx context.Context, tenantID, profileID string) (*model.Profile, error) {
	if repository.getFn != nil {
		return repository.getFn(ctx, tenantID, profileID)
	}
	return repository.Repository.Get(ctx, tenantID, profileID)
}

type resolverBackendRepository struct {
	backend.Repository
	getFn func(context.Context, string, string) (*backend.Profile, error)
}

func (repository resolverBackendRepository) Get(ctx context.Context, tenantID, profileID string) (*backend.Profile, error) {
	if repository.getFn != nil {
		return repository.getFn(ctx, tenantID, profileID)
	}
	return repository.Repository.Get(ctx, tenantID, profileID)
}

type cancelAfterTenantGet struct {
	tenant.Repository
	cancel context.CancelFunc
}

func (repository cancelAfterTenantGet) Get(ctx context.Context, tenantID string) (*tenant.Tenant, error) {
	tenantValue, err := repository.Repository.Get(ctx, tenantID)
	repository.cancel()
	return tenantValue, err
}

type cancelAfterAppGet struct {
	appmodel.Repository
	cancel context.CancelFunc
}

func (repository cancelAfterAppGet) Get(ctx context.Context, tenantID, appID string) (*appmodel.App, error) {
	appValue, err := repository.Repository.Get(ctx, tenantID, appID)
	repository.cancel()
	return appValue, err
}

type cancelAfterRevisionGet struct {
	appmodel.Repository
	cancel context.CancelFunc
}

func (repository cancelAfterRevisionGet) GetRevision(ctx context.Context, tenantID, appID string, revision int64) (*appmodel.Revision, error) {
	revisionValue, err := repository.Repository.GetRevision(ctx, tenantID, appID, revision)
	repository.cancel()
	return revisionValue, err
}

func resolverTestConfig(fixture gatewayFixture) serviceruntime.PlanResolverConfig {
	return serviceruntime.PlanResolverConfig{
		Tenants: fixture.tenants, Apps: fixture.apps, Models: fixture.models, Backends: fixture.backends,
		ModelCatalog: fixture.modelCatalog, BackendCatalog: fixture.backendCatalog,
	}
}
