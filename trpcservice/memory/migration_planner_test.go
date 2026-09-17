package memory

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/memorydriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
)

type plannerAuthority struct{ values []migration.Migration }

func (p plannerAuthority) List(context.Context, string, string) ([]migration.Migration, error) {
	return p.values, nil
}

type plannerConfigs struct{ values map[int64]config.Snapshot }

func (p plannerConfigs) Get(_ context.Context, _ string, version int64) (config.Snapshot, error) {
	return p.values[version], nil
}

type plannerProfiles struct {
	value provider.BackendProfileSnapshot
}

func (p plannerProfiles) GetBackend(context.Context, string, string, int64) (provider.BackendProfileSnapshot, error) {
	return p.value, nil
}

type plannerLedger struct{}

func (plannerLedger) Record(context.Context, memorydriver.RecordRequest) (memorydriver.Mutation, error) {
	return memorydriver.Mutation{}, nil
}
func (plannerLedger) Claim(context.Context, memorydriver.ClaimRequest) ([]memorydriver.Mutation, error) {
	return nil, nil
}
func (plannerLedger) MarkApplied(context.Context, memorydriver.CompleteRequest) (memorydriver.Mutation, error) {
	return memorydriver.Mutation{}, nil
}
func (plannerLedger) MarkRetry(context.Context, memorydriver.RetryRequest) (memorydriver.Mutation, error) {
	return memorydriver.Mutation{}, nil
}
func (plannerLedger) Outstanding(context.Context, string, string) (int64, error) { return 0, nil }

type plannerTarget struct{ calls int }

func (p *plannerTarget) ApplyUser(_ context.Context, _ memorydriver.UserKey, images []memorydriver.Image) (string, error) {
	p.calls++
	return memorydriver.Digest(images)
}

func TestMigrationPlannerWrapsActiveSourceBundle(t *testing.T) {
	target := &plannerTarget{}
	planner := MigrationPlanner{
		Authority: plannerAuthority{values: []migration.Migration{{TenantID: "tenant-a", MigrationID: "move", Domain: memorydriver.Domain, Epoch: 2,
			Source: migration.Binding{ConfigVersion: 1, BackendProfileID: "redis", BackendVersion: 1}, Target: migration.Binding{ConfigVersion: 2, BackendProfileID: "postgres", BackendVersion: 1}, State: migration.StateDualWrite}}},
		Configs:  plannerConfigs{values: map[int64]config.Snapshot{2: {TenantID: "tenant-a", ConfigVersion: 2, Payload: config.ConfigV1{BackendBindings: []config.BackendBinding{{Domain: "memory", BackendProfileID: "postgres", BackendVersion: 1}}}}}},
		Profiles: plannerProfiles{value: provider.BackendProfileSnapshot{TenantID: "tenant-a", ProfileID: "postgres", Version: 1, Status: "active", Provider: "postgres", Capabilities: provider.CapabilitySet{"strong_ryw": true}}},
		Ledger:   plannerLedger{}, BuildTarget: func(context.Context, provider.BackendProfileSnapshot) (memorydriver.UserApplier, error) {
			return target, nil
		},
	}
	snapshot := profile.ExecutionProfileSnapshot{Key: profile.ExecutionProfileKey{TenantID: "tenant-a", ConfigVersion: 1}, AppName: "tenant-a/app"}
	service, err := planner.Decorate(context.Background(), snapshot, memoryinmemory.NewMemoryService())
	if err != nil {
		t.Fatal(err)
	}
	ctx := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant-a", RequestID: "request"})
	if err := service.AddMemory(ctx, agentmemory.UserKey{AppName: snapshot.AppName, UserID: "user"}, "value", nil); err != nil {
		t.Fatal(err)
	}
	if target.calls != 1 {
		t.Fatalf("target calls=%d, want dual-write", target.calls)
	}
}

// A target-config bundle stays available during cutover. Its reverse
// dual-write target is the source Redis backend, so this locks the deployment
// contract that originally made this path fail with ErrCapabilityUnsupported.
func TestMigrationPlannerWrapsCutoverTargetBundleWithRedisReverseTarget(t *testing.T) {
	target := &plannerTarget{}
	var built provider.BackendProfileSnapshot
	planner := MigrationPlanner{
		Authority: plannerAuthority{values: []migration.Migration{{TenantID: "tenant-a", MigrationID: "move", Domain: memorydriver.Domain, Epoch: 2,
			Source: migration.Binding{ConfigVersion: 1, BackendProfileID: "redis", BackendVersion: 1}, Target: migration.Binding{ConfigVersion: 2, BackendProfileID: "postgres", BackendVersion: 1}, State: migration.StateCutover}}},
		Configs:  plannerConfigs{values: map[int64]config.Snapshot{1: {TenantID: "tenant-a", ConfigVersion: 1, Payload: config.ConfigV1{BackendBindings: []config.BackendBinding{{Domain: "memory", BackendProfileID: "redis", BackendVersion: 1}}}}}},
		Profiles: plannerProfiles{value: provider.BackendProfileSnapshot{TenantID: "tenant-a", ProfileID: "redis", Version: 1, Status: "active", Provider: "redis-memory", Capabilities: provider.CapabilitySet{"strong_ryw": true}}},
		Ledger:   plannerLedger{}, BuildTarget: func(_ context.Context, backend provider.BackendProfileSnapshot) (memorydriver.UserApplier, error) {
			built = backend
			return target, nil
		},
	}
	snapshot := profile.ExecutionProfileSnapshot{Key: profile.ExecutionProfileKey{TenantID: "tenant-a", ConfigVersion: 2}, AppName: "tenant-a/app"}
	service, err := planner.Decorate(context.Background(), snapshot, memoryinmemory.NewMemoryService())
	if err != nil {
		t.Fatal(err)
	}
	ctx := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant-a", RequestID: "request"})
	if err := service.AddMemory(ctx, agentmemory.UserKey{AppName: snapshot.AppName, UserID: "user"}, "value", nil); err != nil {
		t.Fatal(err)
	}
	if built.Provider != "redis-memory" || target.calls != 1 {
		t.Fatalf("built=%#v target calls=%d, want redis reverse dual-write", built, target.calls)
	}
}
