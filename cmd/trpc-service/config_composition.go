package main

import (
	"context"
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/configpub"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

// configComposition is the optional P1-08 production composition. It is
// disabled by default: when CONFIGPUB_ENABLED is unset or false the assembly
// keeps the exact pre-P1-08 resolver and agent-resolution behavior. When
// enabled, tenant-safe rollout assignment and immutable revision snapshots
// resolve the config version of every new request and every accepted job,
// failing closed on any configuration fact source failure.
type configComposition struct {
	coordinator *configpub.Coordinator
	repository  *configpub.PostgresRepository
	telemetry   configpubTelemetry
}

// configpubTelemetry adapts the injected P1-07 runtime to the narrow
// configpub observability seam. A nil runtime is fully safe: nothing is
// emitted and no outcome changes.
type configpubTelemetry struct{ runtime *telemetry.Runtime }

func (a configpubTelemetry) StartConfigSpan(ctx context.Context, operation string) (context.Context, func(string)) {
	if a.runtime == nil {
		return ctx, func(string) {}
	}
	return a.runtime.StartConfigSpan(ctx, operation)
}

func (a configpubTelemetry) ConfigOperation(operation, outcome string) {
	if a.runtime != nil {
		a.runtime.ConfigOperation(operation, outcome)
	}
}

func (a configpubTelemetry) Event(ctx context.Context, level int, event, component, operation, outcome string, fields ...string) {
	if a.runtime != nil {
		a.runtime.Logger().Event(ctx, slog.Level(level), event, component, operation, outcome, fields...)
	}
}

// newConfigComposition builds the P1-08 composition over the production pool.
// It returns (nil, nil) when the composition is disabled.
func newConfigComposition(pool *pgxpool.Pool, telemetryRuntime *telemetry.Runtime) (*configComposition, error) {
	enabled, err := configPubEnabled()
	if err != nil {
		return nil, err
	}
	if !enabled {
		return nil, nil
	}
	if pool == nil {
		return nil, errors.New("configuration publication requires a database pool")
	}
	repository, err := configpub.NewPostgresRepository(pool)
	if err != nil {
		return nil, errors.New("configuration publication repository initialization failed")
	}
	coordinator, err := configpub.NewCoordinator(repository, repository, configpubTelemetry{runtime: telemetryRuntime})
	if err != nil {
		return nil, errors.New("configuration publication coordinator initialization failed")
	}
	return &configComposition{coordinator: coordinator, repository: repository, telemetry: configpubTelemetry{runtime: telemetryRuntime}}, nil
}

// configPubEnabled reports whether the P1-08 composition is enabled. Invalid
// boolean values fail closed.
func configPubEnabled() (bool, error) {
	return optionalBoolEnv("CONFIGPUB_ENABLED", false)
}

// assignmentResolver decorates the production resolver with durable rollout
// assignment. It is installed only when the P1-08 composition is enabled.
type assignmentResolver struct {
	inner tenant.TenantResolver
	co    *configpub.Coordinator
}

func (r assignmentResolver) Resolve(ctx context.Context, req tenant.ResolveRequest) (tenant.TenantContext, error) {
	return configpub.AssignmentResolver{Inner: r.inner, Co: r.co}.Resolve(ctx, req)
}

// configSnapshotAgentResolver resolves agent specifications from the immutable
// revision a request or job was accepted with. It never re-reads the active
// configuration and fails closed on mismatch or unavailability.
type configSnapshotAgentResolver struct {
	inner    configpub.SnapshotAgentResolver
	fallback func(context.Context, tenant.TenantContext) (agent.AgentSpec, error)
}

func (r configSnapshotAgentResolver) resolve(ctx context.Context, tc tenant.TenantContext, ref queue.AgentRefDTO) (agent.AgentSpec, error) {
	if r.inner.Co == nil {
		return agent.AgentSpec{}, configpub.ErrInvalidArgument
	}
	if _, managedTenant, err := r.inner.Co.RolloutState(ctx, tc.TenantID); err != nil {
		return agent.AgentSpec{}, err
	} else if !managedTenant {
		if r.fallback == nil {
			return agent.AgentSpec{}, configpub.ErrInvalidArgument
		}
		return r.fallback(ctx, tc)
	}
	return r.inner.Resolve(ctx, tc, ref)
}

// ResolveForIngress satisfies the ingress ResolveAgent seam.
func (r configSnapshotAgentResolver) ResolveForIngress(ctx context.Context, tc tenant.TenantContext) (agent.AgentSpec, error) {
	return r.resolve(ctx, tc, queue.AgentRefDTO{TenantID: tc.TenantID, AgentAppID: tc.AgentAppID, Version: tc.ConfigVersion})
}

// Resolve satisfies the worker AgentResolver seam.
func (r configSnapshotAgentResolver) Resolve(ctx context.Context, tc tenant.TenantContext, ref queue.AgentRefDTO) (agent.AgentSpec, error) {
	return r.resolve(ctx, tc, ref)
}
