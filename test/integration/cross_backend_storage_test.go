//go:build integration

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	redisv8 "github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/controlplane"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/fence"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

type crossBackendTenant struct {
	value  *tenant.Tenant
	app    string
	tokens map[string]fence.Token
}

type crossBackendLease struct {
	sessions session.Service
	memories memory.Service
	release  func()
}

func TestCrossBackendStorageAdapterSharesDataAcrossNodesAndIsolatesScopes(t *testing.T) {
	dbA, dbB := openDatabase(t), openDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	redisOptions, err := redisv8.ParseURL(os.Getenv("TEST_REDIS_URL"))
	if err != nil {
		t.Fatalf("parse real Session lease Redis endpoint: %v", err)
	}
	redisClient := redisv8.NewClient(redisOptions)
	t.Cleanup(func() { _ = redisClient.Close() })
	sessionLocks := storage.NewSessionLockManager(redisClient)
	profiles, err := storage.LoadBackendProfiles(`[
		{"id":"cross-redis","backend":"redis","connectionEnv":"TEST_REDIS_URL","allowInsecure":true},
		{"id":"cross-postgres","backend":"postgres","connectionEnv":"TEST_DATABASE_URL","allowInsecure":true}
	]`, os.LookupEnv)
	if err != nil {
		t.Fatal(err)
	}
	tenants := []crossBackendTenant{
		newCrossBackendTenant(t, ctx, dbA, "redis", "postgres"),
		newCrossBackendTenant(t, ctx, dbA, "postgres", "redis"),
	}
	nodes := []*storage.MultiTenantStorageAdapterImpl{
		storage.NewMultiTenantStorageAdapterImplWithOptions(storage.StorageCacheOptions{BackendProfiles: profiles, WriteFence: controlplane.NewPostgresSessionFence(dbA)}),
		storage.NewMultiTenantStorageAdapterImplWithOptions(storage.StorageCacheOptions{BackendProfiles: profiles, WriteFence: controlplane.NewPostgresSessionFence(dbB)}),
	}
	for _, node := range nodes {
		t.Cleanup(func() {
			if err := node.Close(); err != nil {
				t.Errorf("close storage adapter: %v", err)
			}
		})
		if !node.AtomicWriteFenceEnabled() {
			t.Fatal("production storage fence was not installed")
		}
	}
	var leases [2][2]crossBackendLease
	for nodeIndex, node := range nodes {
		for tenantIndex, value := range tenants {
			sessions, memories, release, err := node.AcquireServices(ctx, value.value)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(release)
			leases[nodeIndex][tenantIndex] = crossBackendLease{sessions, memories, release}
		}
	}
	for index, value := range tenants {
		first, second := leases[0][index], leases[1][index]
		if first.sessions == second.sessions || first.memories == second.memories {
			t.Fatal("independent adapters unexpectedly share an in-process service")
		}
		key := crossBackendSessionKey(value)
		actorCtx := fence.WithToken(ctx, value.tokens["alice"])
		invocationLease, err := sessionLocks.AcquireLease(actorCtx, storage.SessionInvocationLeaseKey(value.value.ID, key), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			if err := invocationLease.Release(cleanup); err != nil {
				t.Errorf("release Session invocation lease: %v", err)
			}
		})
		actorCtx = storage.ContextWithSessionLease(actorCtx, key, invocationLease)
		created, err := first.sessions.CreateSession(actorCtx, key, session.StateMap{"tenant": []byte(value.value.ID)})
		if err != nil {
			t.Fatal(err)
		}
		incarnation, err := storage.SessionIncarnationID(created)
		if err != nil || incarnation == "" || incarnation != storage.SessionIncarnationFromContext(actorCtx) {
			t.Fatalf("strict %s creation did not bind its incarnation: id=%q err=%v", value.value.Storage.SessionBackend, incarnation, err)
		}
		// Cleanup borrows the second node until all test operations finish.
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			if err := second.sessions.DeleteSession(fence.WithToken(cleanup, value.tokens["alice"]), key); err != nil {
				t.Errorf("clean up selected Session backend: %v", err)
			}
			for actor, token := range value.tokens {
				if err := second.memories.ClearMemories(fence.WithToken(cleanup, token), memory.UserKey{AppName: value.app, UserID: actor}); err != nil {
					t.Errorf("clean up selected Memory backend: %v", err)
				}
			}
		})
		loaded, err := second.sessions.GetSession(actorCtx, key, session.WithEventNum(10))
		if err != nil || loaded == nil || string(loaded.State["tenant"]) != value.value.ID {
			t.Fatalf("node 2 missed committed %s Session: value=%v err=%v", value.value.Storage.SessionBackend, loaded, err)
		}
		loadedIncarnation, incarnationErr := storage.SessionIncarnationID(loaded)
		if incarnationErr != nil || loadedIncarnation != incarnation {
			t.Fatalf("node 2 missed committed %s incarnation: id=%q want=%q err=%v", value.value.Storage.SessionBackend, loadedIncarnation, incarnation, incarnationErr)
		}
		if err := second.sessions.AppendEvent(actorCtx, loaded, &event.Event{ID: "same-event-id", Timestamp: time.Now().UTC(),
			Response: &model.Response{Choices: []model.Choice{{Message: model.Message{Role: model.RoleUser, Content: value.value.ID}}}}}); err != nil {
			t.Fatal(err)
		}
		loaded, err = first.sessions.GetSession(actorCtx, key, session.WithEventNum(10))
		if err != nil || loaded == nil || len(loaded.Events) != 1 || loaded.Events[0].ID != "same-event-id" || loaded.Events[0].Response == nil || len(loaded.Events[0].Choices) != 1 || loaded.Events[0].Choices[0].Message.Content != value.value.ID {
			t.Fatalf("node 1 missed node 2 Session event: value=%v err=%v", loaded, err)
		}
		for actor, token := range value.tokens {
			memoryCtx := fence.WithToken(ctx, token)
			memoryKey := memory.UserKey{AppName: value.app, UserID: actor}
			if err := first.memories.AddMemory(memoryCtx, memoryKey, "same logical memory", []string{value.value.ID, actor}); err != nil {
				t.Fatal(err)
			}
			entry := assertCrossBackendMemory(t, memoryCtx, second.memories, memoryKey, "same logical memory", []string{value.value.ID, actor})
			if err := second.memories.UpdateMemory(memoryCtx, memory.Key{AppName: value.app, UserID: actor, MemoryID: entry.ID}, "updated by node 2", []string{value.value.ID, actor}); err != nil {
				t.Fatal(err)
			}
			assertCrossBackendMemory(t, memoryCtx, first.memories, memoryKey, "updated by node 2", []string{value.value.ID, actor})
		}
		assertCrossBackendScopeRejection(t, actorCtx, second, value, tenants[1-index])
		assertCrossBackendPhysicalSelection(t, ctx, profiles, value)
	}

	// Closing one node must drain its leases while the other remains usable.
	leases[0][0].release()
	leases[0][0].release()
	closed := make(chan error, 1)
	go func() { closed <- nodes[0].Close() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, _, release, err := nodes[0].AcquireServices(ctx, tenants[0].value)
		if errors.Is(err, storage.ErrBackendCacheClosed) {
			break
		}
		if release != nil {
			release()
		}
		if err != nil || time.Now().After(deadline) {
			t.Fatalf("adapter did not start draining: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-closed:
		t.Fatalf("adapter closed with an active tenant lease: %v", err)
	default:
	}
	value := tenants[1]
	assertCrossBackendMemory(t, fence.WithToken(ctx, value.tokens["alice"]), leases[0][1].memories,
		memory.UserKey{AppName: value.app, UserID: "alice"}, "updated by node 2", []string{value.value.ID, "alice"})
	leases[0][1].release()
	leases[0][1].release()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("adapter close did not finish after final lease release")
	}
	for index, value := range tenants {
		assertCrossBackendMemory(t, fence.WithToken(ctx, value.tokens["alice"]), leases[1][index].memories,
			memory.UserKey{AppName: value.app, UserID: "alice"}, "updated by node 2", []string{value.value.ID, "alice"})
	}
}

func crossBackendSessionKey(value crossBackendTenant) session.Key {
	return session.Key{AppName: value.app, UserID: "group-owner", SessionID: "same-session-id"}
}

func assertCrossBackendMemory(t *testing.T, ctx context.Context, service memory.Service, key memory.UserKey, text string, topics []string) *memory.Entry {
	t.Helper()
	entries, err := service.ReadMemories(ctx, key, 10)
	if err != nil || len(entries) != 1 || entries[0] == nil || entries[0].Memory == nil {
		t.Fatalf("read scoped Memory: count=%d err=%v", len(entries), err)
	}
	entry := entries[0]
	if entry.AppName != key.AppName || entry.UserID != key.UserID || entry.Memory.Memory != text || !reflect.DeepEqual(entry.Memory.Topics, topics) {
		t.Fatalf("Memory escaped tenant/actor scope: %+v memory=%+v", entry, entry.Memory)
	}
	return entry
}

func assertCrossBackendScopeRejection(t *testing.T, ctx context.Context, service crossBackendLease, value, other crossBackendTenant) {
	t.Helper()
	otherApp, err := storage.TenantScopedAppName(value.value, "other-app")
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []memory.UserKey{{AppName: other.app, UserID: "alice"}, {AppName: otherApp, UserID: "alice"}, {AppName: value.app, UserID: "bob"}, {AppName: value.app, UserID: "group-owner"}} {
		if _, err := service.memories.ReadMemories(ctx, key, 10); !errors.Is(err, fence.ErrScopeMismatch) {
			t.Fatalf("cross-scope Memory read allowed for %+v: %v", key, err)
		}
		if err := service.memories.AddMemory(ctx, key, "forbidden", nil); !errors.Is(err, fence.ErrScopeMismatch) {
			t.Fatalf("cross-scope Memory write allowed for %+v: %v", key, err)
		}
	}
	for _, key := range []session.Key{{AppName: other.app, UserID: "group-owner", SessionID: "same-session-id"}, {AppName: otherApp, UserID: "group-owner", SessionID: "same-session-id"}, {AppName: value.app, UserID: "other-owner", SessionID: "same-session-id"}} {
		if _, err := service.sessions.GetSession(ctx, key); !errors.Is(err, fence.ErrScopeMismatch) {
			t.Fatalf("cross-scope Session read allowed for %+v: %v", key, err)
		}
		if _, err := service.sessions.CreateSession(ctx, key, nil); !errors.Is(err, fence.ErrScopeMismatch) {
			t.Fatalf("cross-scope Session write allowed for %+v: %v", key, err)
		}
	}
	if _, err := service.memories.ReadMemories(context.Background(), memory.UserKey{AppName: value.app, UserID: "alice"}, 10); !errors.Is(err, fence.ErrTokenRequired) {
		t.Fatalf("unfenced Memory read allowed: %v", err)
	}
}

func assertCrossBackendPhysicalSelection(t *testing.T, ctx context.Context, profiles *storage.BackendProfileCatalog, value crossBackendTenant) {
	t.Helper()
	// Read the same scoped keys using the opposite native backends. This catches
	// a factory that ignores either tenant selection while both nodes agree.
	probe := storage.NewMultiTenantStorageAdapterImplWithOptions(storage.StorageCacheOptions{BackendProfiles: profiles})
	defer func() {
		if err := probe.Close(); err != nil {
			t.Errorf("close native backend probe: %v", err)
		}
	}()
	opposite := *value.value
	opposite.Storage.SessionBackend, opposite.Storage.MemoryBackend = value.value.Storage.MemoryBackend, value.value.Storage.SessionBackend
	opposite.Storage.SessionProfile, opposite.Storage.MemoryProfile = value.value.Storage.MemoryProfile, value.value.Storage.SessionProfile
	sessions, memories, release, err := probe.AcquireServices(ctx, &opposite)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	values, err := sessions.ListSessions(ctx, session.UserKey{AppName: value.app, UserID: "group-owner"})
	if err != nil || len(values) != 0 {
		t.Fatalf("Session landed in unselected %s backend: count=%d err=%v", opposite.Storage.SessionBackend, len(values), err)
	}
	for actor := range value.tokens {
		entries, err := memories.ReadMemories(ctx, memory.UserKey{AppName: value.app, UserID: actor}, 10)
		if err != nil || len(entries) != 0 {
			t.Fatalf("Memory landed in unselected %s backend: count=%d err=%v", opposite.Storage.MemoryBackend, len(entries), err)
		}
	}
}

func newCrossBackendTenant(t *testing.T, ctx context.Context, db *sql.DB, sessionBackend, memoryBackend string) crossBackendTenant {
	t.Helper()
	value := &tenant.Tenant{ID: "cross-storage-" + uuid.NewString(), ConfigVersion: 1, Storage: tenant.StorageConfig{
		SessionBackend: sessionBackend, SessionProfile: "cross-" + sessionBackend,
		MemoryBackend: memoryBackend, MemoryProfile: "cross-" + memoryBackend,
	}}
	config, err := json.Marshal(map[string]any{"storage": value.Storage})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO tenants(id,name,status,config,config_version) VALUES($1,'cross backend storage','active',$2,1)`, value.ID, config); err != nil {
		t.Fatal(err)
	}
	appID, versionID, deploymentID := "app-"+uuid.NewString(), "version-"+uuid.NewString(), "deployment-"+uuid.NewString()
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		for _, query := range []string{
			`DELETE FROM session_execution_guards WHERE tenant_id=$1`,
			`DELETE FROM execution_records WHERE tenant_id=$1`,
			`DELETE FROM deployments WHERE tenant_id=$1`,
			`DELETE FROM agent_versions WHERE agent_app_id IN (SELECT id FROM agent_apps WHERE tenant_id=$1)`,
			`DELETE FROM agent_apps WHERE tenant_id=$1`,
			`DELETE FROM tenants WHERE id=$1`,
		} {
			if _, err := db.ExecContext(cleanup, query, value.ID); err != nil {
				t.Errorf("clean up cross backend fixture: %v", err)
				return
			}
		}
	})
	if _, err := db.ExecContext(ctx, `INSERT INTO agent_apps(id,tenant_id,name,status) VALUES($1,$2,'support','active')`, appID, value.ID); err != nil {
		t.Fatal(err)
	}
	snapshot := `{"agent":{"name":"support","type":"llm","defaultModel":"gpt"},"model":{"provider":"openai","modelName":"gpt"}}`
	if _, err := db.ExecContext(ctx, `INSERT INTO agent_versions(id,agent_app_id,version_number,config_snapshot,config_hash,status,created_by) VALUES($1,$2,1,$3,$4,'published','integration')`, versionID, appID, snapshot, strings.Repeat("a", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO deployments(id,tenant_id,agent_app_id,agent_version_id,kind,traffic_bps,status,created_by) VALUES($1,$2,$3,$4,'stable',10000,'active','integration')`, deploymentID, value.ID, appID, versionID); err != nil {
		t.Fatal(err)
	}
	app, err := storage.TenantScopedAppName(value, "support")
	if err != nil {
		t.Fatal(err)
	}
	result := crossBackendTenant{value: value, app: app, tokens: make(map[string]fence.Token)}
	// Seed admitted executions; each service operation uses the real database
	// authorizer to acquire, validate and release its execution fence.
	for _, actor := range []string{"alice", "bob"} {
		sessionID := "same-session-id"
		if actor == "bob" {
			sessionID = "other-actor-session"
		}
		token := fence.Token{TenantID: value.ID, AgentAppID: appID, AgentAppName: "support", ScopedAppName: app,
			UserID: actor, SessionOwnerID: "group-owner", SessionID: sessionID, Generation: 1, Value: uuid.NewString()}
		if err := db.QueryRowContext(ctx, `INSERT INTO execution_records(tenant_id,session_id,agent_app_id,agent_version_id,deployment_id,
			idempotency_key,payload_hash,attempt_number,execution_token,status,heartbeat_at,lease_until)
			VALUES($1,$2,$3,$4,$5,$6,$7,1,$8,'RUNNING',clock_timestamp(),clock_timestamp()+INTERVAL '15 minutes') RETURNING id`,
			value.ID, sessionID, appID, versionID, deploymentID, "cross-storage:"+actor, strings.Repeat("b", 64), token.Value).Scan(&token.ExecutionID); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ExecContext(ctx, `INSERT INTO session_execution_guards(tenant_id,agent_app_id,session_id,generation,status,current_execution_id)
			VALUES($1,$2,$3,1,'RUNNING',$4)`, value.ID, appID, sessionID, token.ExecutionID); err != nil {
			t.Fatal(err)
		}
		result.tokens[actor] = token
	}
	return result
}
