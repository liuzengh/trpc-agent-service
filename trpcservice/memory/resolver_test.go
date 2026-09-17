package memory_test

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	servicememory "github.com/liuzengh/trpc-agent-service/trpcservice/memory"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration/memorydriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	"github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secrets"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	agentmemoryredis "trpc.group/trpc-go/trpc-agent-go/memory/redis"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type backendReader struct {
	value provider.BackendProfileSnapshot
}

type migrationLedger struct{ recorded []memorydriver.RecordRequest }

func (l *migrationLedger) Record(_ context.Context, value memorydriver.RecordRequest) (memorydriver.Mutation, error) {
	l.recorded = append(l.recorded, value)
	return memorydriver.Mutation{}, nil
}
func (*migrationLedger) Claim(context.Context, memorydriver.ClaimRequest) ([]memorydriver.Mutation, error) {
	return nil, nil
}
func (*migrationLedger) MarkApplied(context.Context, memorydriver.CompleteRequest) (memorydriver.Mutation, error) {
	return memorydriver.Mutation{}, nil
}
func (*migrationLedger) MarkRetry(context.Context, memorydriver.RetryRequest) (memorydriver.Mutation, error) {
	return memorydriver.Mutation{}, nil
}
func (*migrationLedger) Outstanding(context.Context, string, string) (int64, error) { return 0, nil }

type targetApplier struct {
	fail   bool
	calls  int
	images []memorydriver.Image
}

type switchingDecorator struct {
	active bool
	target *targetApplier
	ledger *migrationLedger
}

func (d *switchingDecorator) Decorate(_ context.Context, snapshot profile.ExecutionProfileSnapshot, primary agentmemory.Service) (agentmemory.Service, error) {
	if !d.active {
		return primary, nil
	}
	return servicememory.NewDualWriteService(primary, servicememory.DualWritePlan{TenantID: snapshot.Key.TenantID, MigrationID: "move", Epoch: 2,
		ConfigVersion: snapshot.Key.ConfigVersion, Direction: memorydriver.DirectionForward, Ledger: d.ledger, Target: d.target})
}

func (t *targetApplier) ApplyUser(_ context.Context, _ memorydriver.UserKey, images []memorydriver.Image) (string, error) {
	t.calls++
	t.images = images
	if t.fail {
		return "", errors.New("target unavailable")
	}
	return memorydriver.Digest(images)
}

type secretProvider struct {
	gotScope secrets.Scope
	gotRef   secrets.SecretRef
	value    secrets.SecretValue
}

func (p *secretProvider) Resolve(_ context.Context, scope secrets.Scope, ref secrets.SecretRef) (secrets.SecretValue, error) {
	p.gotScope, p.gotRef = scope, ref
	return p.value, nil
}

func (r backendReader) GetBackend(_ context.Context, _, _ string, _ int64) (provider.BackendProfileSnapshot, error) {
	return r.value, nil
}

func TestResolverBuildsExactPostgresProfileAndEnforcesAppScope(t *testing.T) {
	snapshot := memorySnapshot()
	backend := provider.BackendProfileSnapshot{TenantID: "tenant-a", ProfileID: "memory", Version: 7, Status: "active",
		Provider: "postgres", SchemaVersion: 1, Capabilities: provider.CapabilitySet{"strong_ryw": true}}
	var gotDSN string
	resolver := servicememory.Resolver{Profiles: backendReader{value: backend}, PostgresConnections: map[string]string{"default": "postgres://memory"},
		BuildPostgres: func(dsn string) (agentmemory.Service, error) {
			gotDSN = dsn
			return memoryinmemory.NewMemoryService(), nil
		}}
	service, err := resolver.Resolve(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if gotDSN != "postgres://memory" {
		t.Fatalf("postgres DSN = %q", gotDSN)
	}
	if err := service.AddMemory(context.Background(), agentmemory.UserKey{AppName: snapshot.AppName, UserID: "u"}, "likes tea", nil); err != nil {
		t.Fatal(err)
	}
	if err := service.AddMemory(context.Background(), agentmemory.UserKey{AppName: "tenant-b/app", UserID: "u"}, "forged", nil); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("cross-app add error = %v, want tenant scope", err)
	}
}

func TestResolverBuildsRedisOnlyForPinnedTenantProfile(t *testing.T) {
	snapshot := memorySnapshot()
	snapshot.BackendBindings[0].BackendProfileID = "redis-profile"
	backend := provider.BackendProfileSnapshot{TenantID: "tenant-a", ProfileID: "redis-profile", Version: 7, Status: "active",
		Provider: "redis-memory", SchemaVersion: 1, Configuration: map[string]string{"connection_id": "shared"},
		Capabilities: provider.CapabilitySet{"strong_ryw": true}}
	var gotURL, gotPrefix string
	resolver := servicememory.Resolver{Profiles: backendReader{value: backend}, RedisConnections: map[string]string{"shared": "redis://redis:6379/0"},
		BuildRedis: func(redisURL, prefix string) (agentmemory.Service, error) {
			gotURL, gotPrefix = redisURL, prefix
			return memoryinmemory.NewMemoryService(), nil
		}}
	service, err := resolver.Resolve(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if gotURL != "redis://redis:6379/0" || gotPrefix != "trpc-memory" {
		t.Fatalf("redis builder args = (%q, %q)", gotURL, gotPrefix)
	}
	if err := service.ClearMemories(context.Background(), agentmemory.UserKey{AppName: "tenant-b/app", UserID: "u"}); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("cross-app clear error = %v, want tenant scope", err)
	}
}

func TestResolverUsesOfficialRedisMemoryService(t *testing.T) {
	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer redisServer.Close()

	snapshot := memorySnapshot()
	snapshot.BackendBindings[0].BackendProfileID = "redis-profile"
	backend := provider.BackendProfileSnapshot{TenantID: "tenant-a", ProfileID: "redis-profile", Version: 7, Status: "active",
		Provider: "redis-memory", SchemaVersion: 1, Configuration: map[string]string{"connection_id": "shared"},
		Capabilities: provider.CapabilitySet{"strong_ryw": true}}
	resolver := servicememory.Resolver{Profiles: backendReader{value: backend}, RedisConnections: map[string]string{"shared": "redis://" + redisServer.Addr()},
		BuildRedis: func(redisURL, prefix string) (agentmemory.Service, error) {
			return agentmemoryredis.NewService(agentmemoryredis.WithRedisClientURL(redisURL), agentmemoryredis.WithKeyPrefix(prefix))
		}}
	service, err := resolver.Resolve(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	key := agentmemory.UserKey{AppName: snapshot.AppName, UserID: "u"}
	if err := service.AddMemory(context.Background(), key, "likes tea", []string{"preference"}); err != nil {
		t.Fatal(err)
	}
	entries, err := service.ReadMemories(context.Background(), key, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Memory == nil || entries[0].Memory.Memory != "likes tea" {
		t.Fatalf("Redis memories = %#v", entries)
	}
}

func TestResolverBuildsInMemoryOnlyWhenExplicitlyAllowed(t *testing.T) {
	snapshot := memorySnapshot()
	backend := provider.BackendProfileSnapshot{TenantID: "tenant-a", ProfileID: "memory", Version: 7, Status: "active",
		Provider: "inmemory-memory", SchemaVersion: 1, Capabilities: provider.CapabilitySet{"strong_ryw": true, "single_node_only": true}}
	resolver := servicememory.Resolver{Profiles: backendReader{value: backend}}
	if _, err := resolver.Resolve(context.Background(), snapshot); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("default in-memory resolve error = %v, want unsupported", err)
	}
	resolver.AllowInMemory = true
	service, err := resolver.Resolve(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	key := agentmemory.UserKey{AppName: snapshot.AppName, UserID: "u"}
	if err := service.AddMemory(context.Background(), key, "ephemeral", nil); err != nil {
		t.Fatal(err)
	}
}

func TestResolverBuildsMem0WithTenantScopedBackendCredential(t *testing.T) {
	snapshot := memorySnapshot()
	snapshot.BackendBindings[0].BackendProfileID = "mem0-profile"
	snapshot.BackendBindings[0].Required = []string{"eventual_visibility", "external_ingest", "read_only_tools"}
	backend := provider.BackendProfileSnapshot{TenantID: "tenant-a", ProfileID: "mem0-profile", Version: 7, Status: "active",
		Provider: "mem0-memory", SchemaVersion: 1, Configuration: map[string]string{"connection_id": "cloud"},
		CredentialRef: secrets.SecretRef{Ref: "secret://tenant/mem0", Version: 3}, Capabilities: provider.CapabilitySet{"eventual_visibility": true, "external_ingest": true, "read_only_tools": true}}
	credentialProvider := &secretProvider{value: secrets.SecretValue{Bytes: []byte("mem0-key"), Version: 3}}
	var gotConnection servicememory.Mem0Connection
	var gotKey string
	resolver := servicememory.Resolver{Profiles: backendReader{value: backend}, Mem0Connections: map[string]servicememory.Mem0Connection{"cloud": {Host: "https://mem0.example.test"}},
		Secrets: credentialProvider, Subject: "worker-memory", BuildMem0: func(connection servicememory.Mem0Connection, apiKey string) (agentmemory.Service, error) {
			gotConnection, gotKey = connection, apiKey
			return memoryinmemory.NewMemoryService(), nil
		}}
	if _, err := resolver.Resolve(context.Background(), snapshot); err != nil {
		t.Fatal(err)
	}
	if gotConnection.Host != "https://mem0.example.test" || gotKey != "mem0-key" {
		t.Fatalf("mem0 builder = %#v / %q", gotConnection, gotKey)
	}
	if credentialProvider.gotScope.TenantID != "tenant-a" || credentialProvider.gotScope.Purpose != secrets.PurposeBackendConnect || credentialProvider.gotScope.ResourceID != "mem0-profile" || credentialProvider.gotRef != backend.CredentialRef {
		t.Fatalf("secret scope/ref = %#v / %#v", credentialProvider.gotScope, credentialProvider.gotRef)
	}
	if string(credentialProvider.value.Bytes) != "\x00\x00\x00\x00\x00\x00\x00\x00" || gotKey == "" {
		t.Fatalf("credential should be wiped after construction: %#v", credentialProvider.value.Bytes)
	}
}

func TestMem0AdapterRejectsUnsupportedDirectWrites(t *testing.T) {
	service, err := servicememory.NewMem0Service(servicememory.Mem0Connection{Host: "http://mem0:8888", SelfHostedOSS: true}, "")
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	if err := service.AddMemory(context.Background(), agentmemory.UserKey{AppName: "tenant-a/app", UserID: "u"}, "not direct", nil); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("direct Mem0 write error = %v, want unsupported", err)
	}
}

func TestResolverRejectsMissingRequiredCapability(t *testing.T) {
	snapshot := memorySnapshot()
	snapshot.BackendBindings[0].Required = nil
	resolver := servicememory.Resolver{Profiles: backendReader{value: provider.BackendProfileSnapshot{}}}
	if _, err := resolver.Resolve(context.Background(), snapshot); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("Resolve error = %v, want unsupported", err)
	}
}

func TestDualWriteServiceRecordsRepairWhenTargetFails(t *testing.T) {
	primary := memoryinmemory.NewMemoryService()
	ledger, target := &migrationLedger{}, &targetApplier{fail: true}
	service, err := servicememory.NewDualWriteService(primary, servicememory.DualWritePlan{TenantID: "tenant-a", MigrationID: "move", Epoch: 2,
		ConfigVersion: 7, Direction: memorydriver.DirectionForward, Ledger: ledger, Target: target})
	if err != nil {
		t.Fatal(err)
	}
	ctx := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant-a", RequestID: "request-1"})
	key := agentmemory.UserKey{AppName: "tenant-a/app", UserID: "u"}
	if err := service.AddMemory(ctx, key, "likes tea", nil); err != nil {
		t.Fatal(err)
	}
	if target.calls != 1 || len(ledger.recorded) != 1 || ledger.recorded[0].ConfigVersion != 7 || ledger.recorded[0].Key.UserID != "u" || ledger.recorded[0].MutationID == "" {
		t.Fatalf("target=%+v ledger=%+v", target, ledger.recorded)
	}
	if err := service.AddMemory(context.Background(), key, "needs identity", nil); !errors.Is(err, runtime.ErrInvariantViolation) {
		t.Fatalf("missing request id error=%v", err)
	}
	entries, err := primary.ReadMemories(context.Background(), key, 0)
	if err != nil || len(entries) != 1 || entries[0].Memory.Memory != "likes tea" {
		t.Fatalf("untracked write reached primary: entries=%#v err=%v", entries, err)
	}
}

func TestDualWriteServiceMakesTargetImageMatchClear(t *testing.T) {
	primary := memoryinmemory.NewMemoryService()
	ledger, target := &migrationLedger{}, &targetApplier{}
	service, err := servicememory.NewDualWriteService(primary, servicememory.DualWritePlan{TenantID: "tenant-a", MigrationID: "move", Epoch: 2,
		ConfigVersion: 7, Direction: memorydriver.DirectionForward, Ledger: ledger, Target: target})
	if err != nil {
		t.Fatal(err)
	}
	ctx := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant-a", RequestID: "request-2"})
	key := agentmemory.UserKey{AppName: "tenant-a/app", UserID: "u"}
	if err := service.AddMemory(ctx, key, "one", nil); err != nil {
		t.Fatal(err)
	}
	if err := service.ClearMemories(ctx, key); err != nil {
		t.Fatal(err)
	}
	if target.calls != 2 || len(target.images) != 0 || len(ledger.recorded) != 0 {
		t.Fatalf("target=%+v repairs=%+v", target, ledger.recorded)
	}
}

func TestDualWriteServiceRejectsUntrackableAutoMemory(t *testing.T) {
	primary := memoryinmemory.NewMemoryService()
	service, err := servicememory.NewDualWriteService(primary, servicememory.DualWritePlan{TenantID: "tenant-a", MigrationID: "move", Epoch: 2,
		ConfigVersion: 7, Direction: memorydriver.DirectionForward, Ledger: &migrationLedger{}, Target: &targetApplier{}})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.EnqueueAutoMemoryJob(context.Background(), &session.Session{AppName: "tenant-a/app"}); !errors.Is(err, runtime.ErrCapabilityUnsupported) {
		t.Fatalf("auto memory error=%v", err)
	}
}

func TestResolverReprojectsMigrationDecoratorForCachedServiceWrites(t *testing.T) {
	snapshot := memorySnapshot()
	backend := provider.BackendProfileSnapshot{TenantID: "tenant-a", ProfileID: "memory", Version: 7, Status: "active", Provider: "postgres", SchemaVersion: 1,
		Capabilities: provider.CapabilitySet{"strong_ryw": true}}
	target, ledger := &targetApplier{}, &migrationLedger{}
	decorator := &switchingDecorator{target: target, ledger: ledger}
	resolver := servicememory.Resolver{Profiles: backendReader{value: backend}, PostgresConnections: map[string]string{"default": "postgres://memory"}, Decorator: decorator,
		BuildPostgres: func(string) (agentmemory.Service, error) { return memoryinmemory.NewMemoryService(), nil }}
	service, err := resolver.Resolve(context.Background(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	key := agentmemory.UserKey{AppName: snapshot.AppName, UserID: "u"}
	if err := service.AddMemory(context.Background(), key, "before migration", nil); err != nil {
		t.Fatal(err)
	}
	if target.calls != 0 {
		t.Fatalf("target calls before migration=%d", target.calls)
	}
	decorator.active = true // models a control transition while the Bundle is cached.
	ctx := runtime.WithExecutionContext(context.Background(), runtime.ExecutionContext{TenantID: "tenant-a", RequestID: "after-transition"})
	if err := service.AddMemory(ctx, key, "after migration", nil); err != nil {
		t.Fatal(err)
	}
	if target.calls != 1 {
		t.Fatalf("target calls after migration=%d, want 1", target.calls)
	}
}

func TestResolverRejectsBackendProfileOutsidePinnedTenantScope(t *testing.T) {
	snapshot := memorySnapshot()
	resolver := servicememory.Resolver{Profiles: backendReader{value: provider.BackendProfileSnapshot{
		TenantID: "tenant-b", ProfileID: "memory", Version: 7, Status: "active", Provider: "postgres", SchemaVersion: 1,
		Capabilities: provider.CapabilitySet{"strong_ryw": true},
	}}}
	if _, err := resolver.Resolve(context.Background(), snapshot); !errors.Is(err, runtime.ErrTenantScope) {
		t.Fatalf("Resolve error = %v, want tenant scope", err)
	}
}

func memorySnapshot() profile.ExecutionProfileSnapshot {
	return profile.ExecutionProfileSnapshot{
		Key: profile.ExecutionProfileKey{TenantID: "tenant-a", AgentAppID: "app", ConfigVersion: 2}, AppName: "tenant-a/app",
		BackendBindings:     []profile.BackendBinding{{Domain: "memory", BackendProfileID: "memory", BackendVersion: 7, Required: []string{"strong_ryw"}}},
		BackendRequirements: profile.CapabilitySet{"strong_ryw": true},
	}
}
