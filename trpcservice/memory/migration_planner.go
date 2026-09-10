package memory

import (
	"context"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/memorydriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/memory"
)

type migrationAuthority interface {
	List(context.Context, string, string) ([]migration.Migration, error)
}
type migrationConfigReader interface {
	Get(context.Context, string, int64) (config.Snapshot, error)
}

// TargetApplierFactory is deployment-owned: profiles select an opaque
// connection_id, while Worker composition owns the concrete database client.
type TargetApplierFactory func(context.Context, provider.BackendProfileSnapshot) (memorydriver.UserApplier, error)

// MigrationPlanner projects one active authority record onto an immutable
// execution profile. It never changes tenant pointers and deliberately only
// decorates the primary service for the exact source/target config versions.
type MigrationPlanner struct {
	Authority   migrationAuthority
	Configs     migrationConfigReader
	Profiles    BackendProfileReader
	Ledger      memorydriver.MutationLedger
	BuildTarget TargetApplierFactory
}

func (p MigrationPlanner) Decorate(ctx context.Context, snapshot profile.ExecutionProfileSnapshot, primary agentmemory.Service) (agentmemory.Service, error) {
	if p.Authority == nil || p.Configs == nil || p.Profiles == nil || p.Ledger == nil || p.BuildTarget == nil || primary == nil {
		return primary, nil // migration support is explicitly opt-in at composition.
	}
	all, err := p.Authority.List(ctx, snapshot.Key.TenantID, memorydriver.Domain)
	if err != nil {
		return nil, err
	}
	var selected *migration.Migration
	for index := range all {
		if activeMemoryMigration(all[index]) {
			if selected != nil {
				return nil, runtime.ErrInvariantViolation
			}
			selected = &all[index]
		}
	}
	if selected == nil {
		return primary, nil
	}
	direction, targetConfig, ok := migrationDirection(*selected, snapshot.Key.ConfigVersion)
	if !ok {
		return primary, nil
	}
	targetSnapshot, err := p.Configs.Get(ctx, snapshot.Key.TenantID, targetConfig)
	if err != nil {
		return nil, err
	}
	binding, ok := memoryBinding(targetSnapshot)
	if !ok {
		return nil, runtime.ErrCapabilityUnsupported
	}
	backend, err := p.Profiles.GetBackend(ctx, snapshot.Key.TenantID, binding.BackendProfileID, binding.BackendVersion)
	if err != nil {
		return nil, err
	}
	if backend.TenantID != snapshot.Key.TenantID || backend.Status != "active" || !backend.Capabilities["strong_ryw"] {
		return nil, runtime.ErrCapabilityUnsupported
	}
	target, err := p.BuildTarget(ctx, backend)
	if err != nil {
		return nil, err
	}
	return NewDualWriteService(primary, DualWritePlan{TenantID: snapshot.Key.TenantID, MigrationID: selected.MigrationID,
		Epoch: selected.Epoch, ConfigVersion: snapshot.Key.ConfigVersion, Direction: direction, Ledger: p.Ledger, Target: target})
}

func activeMemoryMigration(value migration.Migration) bool {
	return value.Domain == memorydriver.Domain && (value.State == migration.StateDualWrite || value.State == migration.StateBackfill ||
		value.State == migration.StateVerify || value.State == migration.StateCutover || value.State == migration.StateObserve)
}

func migrationDirection(value migration.Migration, configVersion int64) (memorydriver.Direction, int64, bool) {
	if configVersion == value.Source.ConfigVersion {
		return memorydriver.DirectionForward, value.Target.ConfigVersion, true
	}
	if (value.State == migration.StateCutover || value.State == migration.StateObserve) && configVersion == value.Target.ConfigVersion {
		return memorydriver.DirectionReverse, value.Source.ConfigVersion, true
	}
	return "", 0, false
}

func memoryBinding(snapshot config.Snapshot) (profile.BackendBinding, bool) {
	var result profile.BackendBinding
	for _, binding := range snapshot.Payload.BackendBindings {
		if binding.Domain != memoryDomain {
			continue
		}
		if result.Domain != "" {
			return profile.BackendBinding{}, false
		}
		result = profile.BackendBinding{Domain: binding.Domain, BackendProfileID: binding.BackendProfileID, BackendVersion: binding.BackendVersion, Required: append([]string(nil), binding.Required...)}
	}
	return result, result.Domain != ""
}

var _ ServiceDecorator = MigrationPlanner{}
