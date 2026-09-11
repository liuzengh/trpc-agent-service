package backendmigration

import (
	"context"
	"encoding/json"
	"net"
	"net/url"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/memorystore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/internal/execution/adapter/outbound/sessionstore"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/memorymigrations"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/sessionmigrations"
	"trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
)

func TestPostgresRedisRoundTrip(t *testing.T) {
	postgresSession := os.Getenv("WORKER_BACKEND_MIGRATION_SESSION_POSTGRES_URL")
	postgresMemory := os.Getenv("WORKER_BACKEND_MIGRATION_MEMORY_POSTGRES_URL")
	sessionMigration := os.Getenv("WORKER_BACKEND_MIGRATION_SESSION_MIGRATION_URL")
	memoryMigration := os.Getenv("WORKER_BACKEND_MIGRATION_MEMORY_MIGRATION_URL")
	redisAddress := os.Getenv("WORKER_BACKEND_MIGRATION_REDIS_ADDR")
	if postgresSession == "" || postgresMemory == "" || sessionMigration == "" || memoryMigration == "" || redisAddress == "" {
		t.Skip("explicit disposable PostgreSQL and Redis migration fixtures required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	applyMigrations(t, ctx, sessionMigration, memoryMigration)

	pgSession := openPostgresSession(t, ctx, postgresSession)
	defer pgSession.Close()
	pgMemory := openPostgresMemory(t, ctx, postgresMemory)
	defer pgMemory.Close()
	redisSession, redisMemory := openRedisStores(t, ctx, redisAddress)
	defer redisSession.Close()
	defer redisMemory.Close()

	pgPlan := seedDirection(t, ctx, "postgres-source", pgSession, pgMemory)
	for attempt := 0; attempt < 2; attempt++ {
		result, err := Migrate(ctx, pgPlan, pgSession, redisSession, pgMemory, redisMemory)
		if err != nil || result != (Result{SessionsCopied: 1, MemoriesCopied: 1}) {
			t.Fatal("postgres to redis", result, err)
		}
	}
	assertDirection(t, ctx, pgPlan, redisSession, redisMemory)

	redisPlan := seedDirection(t, ctx, "redis-source", redisSession, redisMemory)
	for attempt := 0; attempt < 2; attempt++ {
		result, err := Migrate(ctx, redisPlan, redisSession, pgSession, redisMemory, pgMemory)
		if err != nil || result != (Result{SessionsCopied: 1, MemoriesCopied: 1}) {
			t.Fatal("redis to postgres", result, err)
		}
	}
	assertDirection(t, ctx, redisPlan, pgSession, pgMemory)
	for _, scope := range []memorystore.Scope{{TenantID: "other", ID: pgPlan.Memories[0].ID}, {TenantID: pgPlan.Memories[0].TenantID, ID: "other"}} {
		got, err := redisMemory.Load(ctx, scope)
		if err != nil || got.Revision != 0 || len(got.Entries) != 0 {
			t.Fatal("tenant isolation", scope, got, err)
		}
	}
}

type acceptedMemoryStore interface {
	MemoryTarget
	ApplyAccepted(context.Context, memorystore.Accepted, memorystore.Candidate) (memorystore.Snapshot, error)
}

func seedDirection(t *testing.T, ctx context.Context, prefix string, sessions SessionStore, memories acceptedMemoryStore) Plan {
	t.Helper()
	candidate := sessionstore.Candidate{
		Identity:       sessionstore.Identity{TenantID: prefix + "-tenant", SessionID: prefix + "-session", RunID: prefix + "-run", AttemptID: prefix + "-attempt"},
		ContentVersion: sessionstore.ContentVersion,
		Snapshot:       json.RawMessage(`{"events":["accepted"],"summary":"kept"}`),
	}
	head, err := sessions.Put(ctx, candidate)
	if err != nil {
		t.Fatal("seed session", err)
	}
	scope := memorystore.Scope{TenantID: prefix + "-tenant", ID: prefix + "-scope"}
	sdk := inmemory.NewMemoryService()
	t.Cleanup(func() { _ = sdk.Close() })
	if err = sdk.AddMemory(ctx, scope.Key(), prefix+" memory", []string{"migration"}); err != nil {
		t.Fatal("seed memory", err)
	}
	entries, err := sdk.ReadMemories(ctx, scope.Key(), 0)
	if err != nil {
		t.Fatal("read seed memory", err)
	}
	memoryCandidate := memorystore.Candidate{Scope: scope, Entries: entries}
	digest, err := memoryCandidate.Digest()
	if err != nil {
		t.Fatal("digest seed memory", err)
	}
	if _, err = memories.ApplyAccepted(ctx, memorystore.Accepted{CompletionID: prefix + "-completion", RunID: prefix + "-run", AttemptID: prefix + "-attempt", CandidateDigest: digest}, memoryCandidate); err != nil {
		t.Fatal("seed accepted memory", err)
	}
	return Plan{Sessions: []SessionRoot{{TenantID: candidate.Identity.TenantID, SessionID: candidate.Identity.SessionID, Head: head}}, Memories: []memorystore.Scope{scope}}
}

func assertDirection(t *testing.T, ctx context.Context, plan Plan, sessions SessionStore, memories MemorySource) {
	t.Helper()
	root := plan.Sessions[0]
	candidate, err := sessions.Load(ctx, root.TenantID, root.SessionID, root.Head)
	if err != nil || string(candidate.Snapshot) != `{"events":["accepted"],"summary":"kept"}` {
		t.Fatal("session target mismatch", candidate, err)
	}
	snapshot, err := memories.Load(ctx, plan.Memories[0])
	if err != nil || snapshot.Revision != 1 || len(snapshot.Entries) != 1 || snapshot.Entries[0].Memory.Memory != plan.Memories[0].TenantID[:len(plan.Memories[0].TenantID)-len("-tenant")]+" memory" {
		t.Fatal("memory target mismatch", snapshot, err)
	}
}

func applyMigrations(t *testing.T, ctx context.Context, sessionURL, memoryURL string) {
	t.Helper()
	sessionPool, err := pgxpool.New(ctx, sessionURL)
	if err != nil {
		t.Fatal(err)
	}
	defer sessionPool.Close()
	if err = sessionmigrations.Apply(ctx, sessionPool); err != nil {
		t.Fatal("session migrations", err)
	}
	memoryPool, err := pgxpool.New(ctx, memoryURL)
	if err != nil {
		t.Fatal(err)
	}
	defer memoryPool.Close()
	if err = memorymigrations.Apply(ctx, memoryPool); err != nil {
		t.Fatal("memory migrations", err)
	}
}

func openPostgresSession(t *testing.T, ctx context.Context, dsn string) *sessionstore.Postgres {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	store, err := sessionstore.Open(ctx, dsn, sessionstore.Target{Host: cfg.ConnConfig.Host, Port: cfg.ConnConfig.Port, Database: cfg.ConnConfig.Database, Username: cfg.ConnConfig.User, SSLMode: u.Query().Get("sslmode")}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func openPostgresMemory(t *testing.T, ctx context.Context, dsn string) *memorystore.Postgres {
	t.Helper()
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	store, err := memorystore.Open(ctx, dsn, memorystore.Target{Host: cfg.ConnConfig.Host, Port: cfg.ConnConfig.Port, Database: cfg.ConnConfig.Database, Username: cfg.ConnConfig.User, SSLMode: u.Query().Get("sslmode")}, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func openRedisStores(t *testing.T, ctx context.Context, address string) (*sessionstore.Redis, *memorystore.Redis) {
	t.Helper()
	host, rawPort, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := sessionstore.OpenRedis(ctx, sessionstore.RedisTarget{Host: host, Port: uint16(port), Username: "session_runtime", MaxConcurrency: 2}, os.Getenv("WORKER_BACKEND_MIGRATION_SESSION_REDIS_PASSWORD"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	memories, err := memorystore.OpenRedis(ctx, memorystore.RedisTarget{Host: host, Port: uint16(port), Username: "memory_runtime", MaxConcurrency: 2}, os.Getenv("WORKER_BACKEND_MIGRATION_MEMORY_REDIS_PASSWORD"), 1<<20)
	if err != nil {
		sessions.Close()
		t.Fatal(err)
	}
	return sessions, memories
}
