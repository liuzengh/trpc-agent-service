package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	servicememory "github.com/liuzengh/trpc-agent-service/trpcservice/memory"
	memorymigration "github.com/liuzengh/trpc-agent-service/trpcservice/migration/memorydriver"
	"github.com/liuzengh/trpc-agent-service/trpcservice/profile"
	"github.com/liuzengh/trpc-agent-service/trpcservice/provider"
	serviceruntime "github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	redisclient "github.com/redis/go-redis/v9"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/memory"
	agentmemorypostgres "trpc.group/trpc-go/trpc-agent-go/memory/postgres"
	agentmemoryredis "trpc.group/trpc-go/trpc-agent-go/memory/redis"
)

// TestComposeTenantMemoryBackendRouting is deliberately small: it proves the
// tenant runtime boundary against the actual Compose PostgreSQL and Redis
// services, rather than replacing either backend with an in-memory fake. Two
// Resolver instances represent different stateless Workers.
func TestComposeTenantMemoryBackendRouting(t *testing.T) {
	if os.Getenv("TRPC_RUNTIME_TEST") != "1" {
		t.Skip("TRPC_RUNTIME_TEST=1 is required")
	}
	postgresDSN := os.Getenv("TRPC_POSTGRES_TEST_DSN")
	redisAddress := os.Getenv("TRPC_REDIS_TEST_ADDR")
	if postgresDSN == "" || redisAddress == "" {
		t.Skip("PostgreSQL and Redis Compose endpoints are required")
	}

	stamp := time.Now().UTC().UnixNano()
	postgresSnapshot := tenantMemorySnapshot("compose-memory-postgres", "app", "pg-memory", 1)
	redisSnapshot := tenantMemorySnapshot("compose-memory-redis", "app", "redis-memory", 1)
	postgresProfile := provider.BackendProfileSnapshot{
		TenantID: postgresSnapshot.Key.TenantID, ProfileID: "pg-memory", Version: 1,
		Status: "active", Provider: "postgres", SchemaVersion: 2,
		Configuration: map[string]string{"connection_id": "memory-postgres"},
		Capabilities:  provider.CapabilitySet{"strong_ryw": true},
	}
	redisProfile := provider.BackendProfileSnapshot{
		TenantID: redisSnapshot.Key.TenantID, ProfileID: "redis-memory", Version: 1,
		Status: "active", Provider: "redis-memory", SchemaVersion: 1,
		Configuration: map[string]string{"connection_id": "memory-redis"},
		Capabilities:  provider.CapabilitySet{"strong_ryw": true},
	}

	postgresResolver := composeMemoryResolver(postgresProfile, postgresDSN, redisAddress, fmt.Sprintf("compose_mem_%d", stamp))
	redisWorkerOne := composeMemoryResolver(redisProfile, postgresDSN, redisAddress, "unused")
	redisWorkerTwo := composeMemoryResolver(redisProfile, postgresDSN, redisAddress, "unused")

	ctx := context.Background()
	postgresMemory, err := postgresResolver.Resolve(ctx, postgresSnapshot)
	if err != nil {
		t.Fatalf("resolve PostgreSQL tenant memory: %v", err)
	}
	t.Cleanup(func() { _ = postgresMemory.Close() })
	redisMemoryOne, err := redisWorkerOne.Resolve(ctx, redisSnapshot)
	if err != nil {
		t.Fatalf("resolve Redis tenant memory on worker one: %v", err)
	}
	t.Cleanup(func() { _ = redisMemoryOne.Close() })
	redisMemoryTwo, err := redisWorkerTwo.Resolve(ctx, redisSnapshot)
	if err != nil {
		t.Fatalf("resolve Redis tenant memory on worker two: %v", err)
	}
	t.Cleanup(func() { _ = redisMemoryTwo.Close() })

	postgresKey := agentmemory.UserKey{AppName: postgresSnapshot.AppName, UserID: "user"}
	redisKey := agentmemory.UserKey{AppName: redisSnapshot.AppName, UserID: "user"}
	if err := postgresMemory.AddMemory(ctx, postgresKey, "only-postgres", []string{"acceptance"}); err != nil {
		t.Fatalf("write PostgreSQL memory: %v", err)
	}
	if err := redisMemoryOne.AddMemory(ctx, redisKey, "visible-on-worker-two", []string{"acceptance"}); err != nil {
		t.Fatalf("write Redis memory: %v", err)
	}

	assertComposeMemory(t, postgresMemory, postgresKey, "only-postgres")
	assertComposeMemory(t, redisMemoryTwo, redisKey, "visible-on-worker-two")
	if err := postgresMemory.AddMemory(ctx, redisKey, "forged-cross-tenant-write", nil); !errors.Is(err, serviceruntime.ErrTenantScope) {
		t.Fatalf("cross-tenant memory write error=%v, want tenant scope", err)
	}
}

// TestComposeRedisMemoryBackfillToPostgres proves the migration data plane
// against the official Redis hash encoding and the baseline-owned memories
// table. Online migration authority/dual-write is exercised separately; this
// test is intentionally the deterministic export/apply/verify step.
func TestComposeRedisMemoryBackfillToPostgres(t *testing.T) {
	if os.Getenv("TRPC_RUNTIME_TEST") != "1" {
		t.Skip("TRPC_RUNTIME_TEST=1 is required")
	}
	postgresDSN, redisAddress := os.Getenv("TRPC_POSTGRES_TEST_DSN"), os.Getenv("TRPC_REDIS_TEST_ADDR")
	if postgresDSN == "" || redisAddress == "" {
		t.Skip("PostgreSQL and Redis Compose endpoints are required")
	}
	stamp := time.Now().UTC().UnixNano()
	tenantID, appName, prefix := fmt.Sprintf("compose-migrate-%d", stamp), "", fmt.Sprintf("migrate_mem_%d", stamp)
	appName = tenantID + "/app"
	redisService, err := agentmemoryredis.NewService(agentmemoryredis.WithRedisClientURL("redis://"+redisAddress+"/0"), agentmemoryredis.WithKeyPrefix(prefix))
	if err != nil {
		t.Fatal(err)
	}
	defer redisService.Close()
	key := agentmemory.UserKey{AppName: appName, UserID: "user"}
	if err := redisService.AddMemory(context.Background(), key, "migrate-me", []string{"migration"}); err != nil {
		t.Fatal(err)
	}
	redisRaw := redisclient.NewClient(&redisclient.Options{Addr: redisAddress})
	defer redisRaw.Close()
	images, sourceDigest, err := (memorymigration.RedisSource{Client: redisRaw, KeyPrefix: prefix}).ExportTenant(context.Background(), tenantID)
	if err != nil || len(images) != 1 {
		t.Fatalf("redis export = %#v / %s / %v", images, sourceDigest, err)
	}
	db, err := sql.Open("pgx", postgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	targetDigest, err := (memorymigration.PostgresTarget{DB: db}).Apply(context.Background(), images)
	if err != nil || targetDigest != sourceDigest {
		t.Fatalf("postgres apply digest/error = %s / %v; source=%s", targetDigest, err, sourceDigest)
	}
	postgresService, err := agentmemorypostgres.NewService(agentmemorypostgres.WithPostgresClientDSN(postgresDSN), agentmemorypostgres.WithSkipDBInit(true))
	if err != nil {
		t.Fatal(err)
	}
	defer postgresService.Close()
	assertComposeMemory(t, postgresService, key, "migrate-me")
	if again, err := (memorymigration.PostgresTarget{DB: db}).Apply(context.Background(), images); err != nil || again != sourceDigest {
		t.Fatalf("idempotent postgres apply digest/error = %s / %v", again, err)
	}
}

// TestComposeRedisMemoryDualWriteToPostgres verifies the Worker-side write
// decorator against actual backend clients, including a Clear that must remove
// stale target rows rather than merely upserting the remaining entries.
func TestComposeRedisMemoryDualWriteToPostgres(t *testing.T) {
	if os.Getenv("TRPC_RUNTIME_TEST") != "1" {
		t.Skip("TRPC_RUNTIME_TEST=1 is required")
	}
	postgresDSN, redisAddress := os.Getenv("TRPC_POSTGRES_TEST_DSN"), os.Getenv("TRPC_REDIS_TEST_ADDR")
	if postgresDSN == "" || redisAddress == "" {
		t.Skip("PostgreSQL and Redis Compose endpoints are required")
	}
	stamp := time.Now().UTC().UnixNano()
	tenantID, prefix := fmt.Sprintf("compose-dual-%d", stamp), fmt.Sprintf("dual_mem_%d", stamp)
	primary, err := agentmemoryredis.NewService(agentmemoryredis.WithRedisClientURL("redis://"+redisAddress+"/0"), agentmemoryredis.WithKeyPrefix(prefix))
	if err != nil {
		t.Fatal(err)
	}
	defer primary.Close()
	db, err := sql.Open("pgx", postgresDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dual, err := servicememory.NewDualWriteService(primary, servicememory.DualWritePlan{TenantID: tenantID, MigrationID: "dual-write", Epoch: 1,
		ConfigVersion: 1, Direction: memorymigration.DirectionForward, Ledger: &composeMemoryLedger{}, Target: memorymigration.PostgresTarget{DB: db}})
	if err != nil {
		t.Fatal(err)
	}
	key := agentmemory.UserKey{AppName: tenantID + "/app", UserID: "user"}
	ctx := serviceruntime.WithExecutionContext(context.Background(), serviceruntime.ExecutionContext{TenantID: tenantID, RequestID: "dual-request"})
	if err := dual.AddMemory(ctx, key, "replicated", []string{"migration"}); err != nil {
		t.Fatal(err)
	}
	postgresService, err := agentmemorypostgres.NewService(agentmemorypostgres.WithPostgresClientDSN(postgresDSN), agentmemorypostgres.WithSkipDBInit(true))
	if err != nil {
		t.Fatal(err)
	}
	defer postgresService.Close()
	assertComposeMemory(t, postgresService, key, "replicated")
	if err := dual.ClearMemories(ctx, key); err != nil {
		t.Fatal(err)
	}
	images, _, err := (memorymigration.PostgresTarget{DB: db}).LoadUser(ctx, memorymigration.UserKey{TenantID: tenantID, AppName: key.AppName, UserID: key.UserID})
	if err != nil || len(images) != 0 {
		t.Fatalf("target was not cleared images=%#v err=%v", images, err)
	}
}

type composeBackendReader struct {
	profile provider.BackendProfileSnapshot
}

type composeMemoryLedger struct{}

func (*composeMemoryLedger) Record(context.Context, memorymigration.RecordRequest) (memorymigration.Mutation, error) {
	return memorymigration.Mutation{}, nil
}
func (*composeMemoryLedger) Claim(context.Context, memorymigration.ClaimRequest) ([]memorymigration.Mutation, error) {
	return nil, nil
}
func (*composeMemoryLedger) MarkApplied(context.Context, memorymigration.CompleteRequest) (memorymigration.Mutation, error) {
	return memorymigration.Mutation{}, nil
}
func (*composeMemoryLedger) MarkRetry(context.Context, memorymigration.RetryRequest) (memorymigration.Mutation, error) {
	return memorymigration.Mutation{}, nil
}
func (*composeMemoryLedger) Outstanding(context.Context, string, string) (int64, error) {
	return 0, nil
}

func (r composeBackendReader) GetBackend(_ context.Context, _, _ string, _ int64) (provider.BackendProfileSnapshot, error) {
	return r.profile, nil
}

func composeMemoryResolver(backend provider.BackendProfileSnapshot, postgresDSN, redisAddress, table string) servicememory.Resolver {
	return servicememory.Resolver{
		Profiles:            composeBackendReader{profile: backend},
		PostgresConnections: map[string]string{"memory-postgres": postgresDSN},
		RedisConnections:    map[string]string{"memory-redis": "redis://" + redisAddress + "/0"},
		BuildPostgres: func(dsn string) (agentmemory.Service, error) {
			return agentmemorypostgres.NewService(agentmemorypostgres.WithPostgresClientDSN(dsn), agentmemorypostgres.WithTableName(table))
		},
		BuildRedis: func(redisURL, prefix string) (agentmemory.Service, error) {
			return agentmemoryredis.NewService(agentmemoryredis.WithRedisClientURL(redisURL), agentmemoryredis.WithKeyPrefix(prefix))
		},
	}
}

func tenantMemorySnapshot(tenantID, appID, profileID string, version int64) profile.ExecutionProfileSnapshot {
	return profile.ExecutionProfileSnapshot{
		Key: profile.ExecutionProfileKey{TenantID: tenantID, AgentAppID: appID, ConfigVersion: 1}, AppName: tenantID + "/" + appID,
		BackendBindings: []profile.BackendBinding{{Domain: "memory", BackendProfileID: profileID, BackendVersion: version,
			Required: []string{"strong_ryw"}}},
	}
}

func assertComposeMemory(t *testing.T, service agentmemory.Service, key agentmemory.UserKey, want string) {
	t.Helper()
	entries, err := service.ReadMemories(context.Background(), key, 10)
	if err != nil {
		t.Fatalf("read %s memory: %v", key.AppName, err)
	}
	if len(entries) != 1 || entries[0].Memory == nil || entries[0].Memory.Memory != want {
		t.Fatalf("memories for %s = %#v, want one %q", key.AppName, entries, want)
	}
}
