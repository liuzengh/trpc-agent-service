package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"

	appmodel "github.com/XnLemon/trpc-agent-service/trpcservice/app"
	"github.com/XnLemon/trpc-agent-service/trpcservice/backend"
	modelprofile "github.com/XnLemon/trpc-agent-service/trpcservice/model"
	"github.com/XnLemon/trpc-agent-service/trpcservice/tenant"
)

var (
	// ErrInvalidPlanResolverConfig reports missing plan resolver dependencies.
	ErrInvalidPlanResolverConfig = errors.New("invalid execution plan resolver configuration")
	// ErrInvalidPlanRequest reports a malformed runtime plan request.
	ErrInvalidPlanRequest = errors.New("invalid execution plan request")
	// ErrPlanResolverNotReady reports a resolver that cannot accept requests.
	ErrPlanResolverNotReady = errors.New("execution plan resolver is not ready")
	// ErrPlanUnavailable is the stable, redacted plan-resolution failure.
	ErrPlanUnavailable = errors.New("execution plan unavailable")
)

// PlanResolverConfig contains the repository and provider catalog dependencies
// needed to construct an immutable ExecutionPlan.
type PlanResolverConfig struct {
	Tenants        tenant.Repository
	Apps           appmodel.Repository
	Models         modelprofile.Repository
	Backends       backend.Repository
	ModelCatalog   *modelprofile.ProviderCatalog
	BackendCatalog *backend.ProviderCatalog
	TenantRuntime  TenantRuntime
}

// PlanRequest identifies the tenant and App whose published configuration
// should be frozen into one execution plan. Authentication and route proof
// validation belong to the caller-facing Gateway boundary.
type PlanRequest struct {
	TenantID string
	AppID    string
}

// Validate checks the minimum identity required for plan resolution. Runtime
// deliberately does not recreate the Gateway's proof validation rules.
func (request PlanRequest) Validate() error {
	if strings.TrimSpace(request.TenantID) == "" || strings.TrimSpace(request.AppID) == "" {
		return fmt.Errorf("%w: tenant and app IDs are required", ErrInvalidPlanRequest)
	}
	return nil
}

// PlanResolver resolves one fixed ExecutionPlan from a neutral PlanRequest.
// It owns configuration lookup and snapshot assembly, but not authentication.
type PlanResolver struct {
	tenants        tenant.Repository
	apps           appmodel.Repository
	models         modelprofile.Repository
	backends       backend.Repository
	modelCatalog   *modelprofile.ProviderCatalog
	backendCatalog *backend.ProviderCatalog
	tenantRuntime  TenantRuntime
}

type resolvedPlanInputs struct {
	tenantSnapshot tenant.ConfigurationSnapshot
	app            *appmodel.App
	revision       *appmodel.Revision
	model          *modelprofile.Profile
	backend        *backend.Profile
}

// NewPlanResolver validates the dependencies required to resolve execution
// plans from tenant-scoped configuration.
func NewPlanResolver(config PlanResolverConfig) (*PlanResolver, error) {
	if config.Tenants == nil || config.Apps == nil || config.Models == nil || config.Backends == nil || config.ModelCatalog == nil || config.BackendCatalog == nil {
		return nil, fmt.Errorf("%w: all repositories and provider catalogs are required", ErrInvalidPlanResolverConfig)
	}
	return &PlanResolver{
		tenants: config.Tenants, apps: config.Apps, models: config.Models, backends: config.Backends,
		modelCatalog: config.ModelCatalog, backendCatalog: config.BackendCatalog,
		tenantRuntime: config.TenantRuntime,
	}, nil
}

// Ready reports whether all required resolver dependencies are present.
func (resolver *PlanResolver) Ready() bool {
	return resolver != nil && resolver.tenants != nil && resolver.apps != nil && resolver.models != nil && resolver.backends != nil && resolver.modelCatalog != nil && resolver.backendCatalog != nil
}

// Resolve constructs one immutable plan. All non-cancellation failures are
// reduced to ErrPlanUnavailable so repository existence and provider details do
// not escape this internal scheduling boundary.
func (resolver *PlanResolver) Resolve(ctx context.Context, request PlanRequest) (ExecutionPlan, error) {
	if ctx == nil {
		return ExecutionPlan{}, fmt.Errorf("%w: context is required", ErrInvalidPlanRequest)
	}
	if err := ctx.Err(); err != nil {
		return ExecutionPlan{}, err
	}
	if !resolver.Ready() {
		return ExecutionPlan{}, ErrPlanResolverNotReady
	}
	if err := request.Validate(); err != nil {
		return ExecutionPlan{}, err
	}
	if resolver.tenantRuntime != nil {
		if err := resolver.tenantRuntime.Ensure(ctx, request.TenantID); err != nil {
			if ctx.Err() != nil {
				return ExecutionPlan{}, ctx.Err()
			}
			return ExecutionPlan{}, ErrPlanUnavailable
		}
	}
	inputs, err := resolver.resolveInputs(ctx, request)
	if err != nil {
		return ExecutionPlan{}, err
	}
	plan, err := NewExecutionPlanFromInput(ExecutionPlanInput{
		TenantSnapshot: inputs.tenantSnapshot,
		AppRoot:        inputs.app,
		Revision:       inputs.revision,
		ModelProfile:   inputs.model,
		ModelCatalog:   resolver.modelCatalog,
		BackendProfile: inputs.backend,
		BackendCatalog: resolver.backendCatalog,
	})
	if err != nil {
		return ExecutionPlan{}, ErrPlanUnavailable
	}
	if _, err := plan.CacheKey(); err != nil {
		return ExecutionPlan{}, ErrPlanUnavailable
	}
	if err := ctx.Err(); err != nil {
		return ExecutionPlan{}, err
	}
	return plan, nil
}

func (resolver *PlanResolver) resolveInputs(ctx context.Context, request PlanRequest) (resolvedPlanInputs, error) {
	tenantValue, err := resolver.tenants.Get(ctx, request.TenantID)
	if err != nil || tenantValue == nil {
		return resolvedPlanInputs{}, resolver.planError(ctx)
	}
	if err := ctx.Err(); err != nil {
		return resolvedPlanInputs{}, err
	}
	tenantSnapshot, err := tenant.NewConfigurationSnapshot(tenantValue)
	if err != nil {
		return resolvedPlanInputs{}, ErrPlanUnavailable
	}
	appValue, err := resolver.apps.Get(ctx, request.TenantID, request.AppID)
	if err != nil || appValue == nil || appValue.CurrentRevision == nil {
		return resolvedPlanInputs{}, resolver.planError(ctx)
	}
	if err := ctx.Err(); err != nil {
		return resolvedPlanInputs{}, err
	}
	selectedRevision := appValue.CurrentRevision
	if appValue.CanaryRevision != nil {
		selectedRevision = appValue.CanaryRevision
	}
	revisionValue, err := resolver.apps.GetRevision(ctx, request.TenantID, request.AppID, *selectedRevision)
	if err != nil || revisionValue == nil {
		return resolvedPlanInputs{}, resolver.planError(ctx)
	}
	modelValue, err := resolver.models.Get(ctx, request.TenantID, revisionValue.ModelProfileID)
	if err != nil || modelValue == nil {
		return resolvedPlanInputs{}, resolver.planError(ctx)
	}
	if err := ctx.Err(); err != nil {
		return resolvedPlanInputs{}, err
	}
	if tenantValue.DefaultBackendProfileID == nil {
		return resolvedPlanInputs{}, ErrPlanUnavailable
	}
	backendValue, err := resolver.backends.Get(ctx, request.TenantID, *tenantValue.DefaultBackendProfileID)
	if err != nil || backendValue == nil {
		return resolvedPlanInputs{}, resolver.planError(ctx)
	}
	if err := ctx.Err(); err != nil {
		return resolvedPlanInputs{}, err
	}
	return resolvedPlanInputs{tenantSnapshot: tenantSnapshot, app: appValue, revision: revisionValue, model: modelValue, backend: backendValue}, nil
}

func (resolver *PlanResolver) planError(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return ErrPlanUnavailable
}
