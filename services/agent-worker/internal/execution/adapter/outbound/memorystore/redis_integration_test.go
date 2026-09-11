package memorystore

import (
	"context"
	"encoding/json"
	"errors"
	redisclient "github.com/redis/go-redis/v9"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
)

func redisFixture(t *testing.T) (context.Context, *Redis, *redisclient.Client) {
	t.Helper()
	addr := os.Getenv("WORKER_MEMORY_REDIS_ADDR")
	if addr == "" {
		t.Skip("isolated WORKER_MEMORY_REDIS_ADDR required")
	}
	port, err := strconv.Atoi(strings.Split(addr, ":")[1])
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	target := RedisTarget{Host: "127.0.0.1", Port: uint16(port), Database: 0, Username: "memory_runtime", MaxConcurrency: 2}
	store, err := OpenRedis(ctx, target, os.Getenv("WORKER_MEMORY_REDIS_PASSWORD"), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	if store.client.Options().PoolSize != 2 || store.client.Options().MaxActiveConns != 2 {
		t.Fatal("pool policy not applied")
	}
	admin := redisclient.NewClient(&redisclient.Options{Addr: addr, Username: "fixture_admin", Password: os.Getenv("WORKER_MEMORY_REDIS_ADMIN_PASSWORD"), MaxRetries: -1})
	t.Cleanup(func() { admin.Close() })
	return ctx, store, admin
}
func redisAccepted(c Candidate, id string) Accepted {
	d, _ := c.Digest()
	return Accepted{CompletionID: id, RunID: id, AttemptID: id, CandidateDigest: d}
}
func TestRedisMemoryContract(t *testing.T) {
	ctx, store, admin := redisFixture(t)
	if err := admin.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	scope := Scope{TenantID: "tenant", ID: "scope"}
	sdk := inmemory.NewMemoryService()
	defer sdk.Close()
	if err := sdk.AddMemory(ctx, scope.Key(), "tea", []string{"drink"}); err != nil {
		t.Fatal(err)
	}
	entries, _ := sdk.ReadMemories(ctx, scope.Key(), 0)
	encodedEntries, _ := json.Marshal(entries)
	var detached []*memory.Entry
	if err := json.Unmarshal(encodedEntries, &detached); err != nil {
		t.Fatal(err)
	}
	c := Candidate{Scope: scope, Entries: detached}
	a := redisAccepted(c, "first")
	empty, err := store.Load(ctx, scope)
	if err != nil || empty.Revision != 0 {
		t.Fatal(empty, err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, e := store.ApplyAccepted(ctx, a, c)
			if e == nil && v.Revision != 1 {
				e = ErrCorrupt
			}
			errs <- e
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal("replay", e)
		}
	}
	loaded, err := store.Load(ctx, scope)
	if err != nil || loaded.Revision != 1 || loaded.Entries[0].Memory.Memory != "tea" {
		t.Fatal(loaded, err)
	}
	loaded.Entries[0].Memory.Memory = "caller alias"
	again, _ := store.Load(ctx, scope)
	if again.Entries[0].Memory.Memory != "tea" {
		t.Fatal("alias leak")
	}
	if err = sdk.UpdateMemory(ctx, memory.Key{AppName: scope.Key().AppName, UserID: scope.ID, MemoryID: entries[0].ID}, "coffee", []string{"drink"}); err != nil {
		t.Fatal(err)
	}
	entries, _ = sdk.ReadMemories(ctx, scope.Key(), 0)
	update := Candidate{Scope: scope, BaseRevision: 1, Entries: entries}
	if _, err = store.ApplyAccepted(ctx, redisAccepted(update, "update"), update); err != nil {
		t.Fatal(err)
	}
	if err = sdk.DeleteMemory(ctx, memory.Key{AppName: scope.Key().AppName, UserID: scope.ID, MemoryID: entries[0].ID}); err != nil {
		t.Fatal(err)
	}
	clear := Candidate{Scope: scope, BaseRevision: 2}
	if _, err = store.ApplyAccepted(ctx, redisAccepted(clear, "clear"), clear); err != nil {
		t.Fatal(err)
	}
	old, err := store.ApplyAccepted(ctx, a, c)
	if err != nil || old.Revision != 1 || len(old.Entries) != 1 {
		t.Fatal("old receipt", err)
	}
	current, err := store.Load(ctx, scope)
	if err != nil || current.Revision != 3 || len(current.Entries) != 0 {
		t.Fatal("old replay regressed head", err)
	}
	if _, err = store.ApplyAccepted(ctx, redisAccepted(c, "stale"), c); !errors.Is(err, ErrConflict) {
		t.Fatal("stale revision", err)
	}
	foreign := Candidate{Scope: Scope{TenantID: scope.TenantID, ID: "foreign"}}
	cross := redisAccepted(foreign, a.CompletionID)
	if _, err = store.ApplyAccepted(ctx, cross, foreign); !errors.Is(err, ErrConflict) {
		t.Fatal("completion crossed scope", err)
	}
	sameAttempt := redisAccepted(foreign, "different-completion")
	sameAttempt.RunID = a.RunID
	sameAttempt.AttemptID = a.AttemptID
	if _, err = store.ApplyAccepted(ctx, sameAttempt, foreign); !errors.Is(err, ErrConflict) {
		t.Fatal("attempt reused", err)
	}
	for _, s := range []Scope{{TenantID: "foreign", ID: scope.ID}, {TenantID: scope.TenantID, ID: "foreign"}} {
		v, e := store.Load(ctx, s)
		if e != nil || v.Revision != 0 || len(v.Entries) != 0 {
			t.Fatal("scope leak", e)
		}
	}
	race := Candidate{Scope: Scope{TenantID: "tenant", ID: "race"}}
	errs = make(chan error, 2)
	for _, id := range []string{"race-a", "race-b"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			_, e := store.ApplyAccepted(ctx, redisAccepted(race, id), race)
			errs <- e
		}(id)
	}
	wg.Wait()
	close(errs)
	wins, conflicts := 0, 0
	for e := range errs {
		if e == nil {
			wins++
		} else if errors.Is(e, ErrConflict) {
			conflicts++
		} else {
			t.Fatal(e)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatal(wins, conflicts)
	}
	next := Candidate{Scope: scope, BaseRevision: 3}
	nextA := redisAccepted(next, "denied-write")
	before, _ := admin.Get(ctx, redisKey(scope, "head", scope.ID)).Result()
	if err = admin.Do(ctx, "ACL", "SETUSER", "memory_runtime", "-mset").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ApplyAccepted(ctx, nextA, next); !errors.Is(err, ErrUnavailable) {
		t.Fatal("write ACL ignored", err)
	}
	if err = admin.Do(ctx, "ACL", "SETUSER", "memory_runtime", "+mset").Err(); err != nil {
		t.Fatal(err)
	}
	after, _ := admin.Get(ctx, redisKey(scope, "head", scope.ID)).Result()
	if before != after {
		t.Fatal("failed MSET partially changed head")
	}
	exists, _ := admin.Exists(ctx, redisKey(scope, "receipt", nextA.CompletionID), redisKey(scope, "attempt", nextA.RunID, nextA.AttemptID)).Result()
	if exists != 0 {
		t.Fatal("failed MSET left receipt")
	}
	tiny := &Redis{client: store.client, capacity: 1}
	if _, err = tiny.Load(ctx, scope); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	if _, err = tiny.ApplyAccepted(ctx, a, c); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = store.Load(cancelled, scope); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel ignored", err)
	}
	// Revisions above Lua's exact integer range remain exact decimal strings.
	high := Candidate{Scope: Scope{TenantID: "tenant", ID: "high"}, BaseRevision: 9007199254740992}
	body, _ := high.encode()
	record := redisRecord{Revision: "9007199254740993", CompletionID: "high", RunID: "high", AttemptID: "high", Digest: digest(body), Body: string(body)}
	raw, _ := json.Marshal(record)
	if err = admin.Set(ctx, redisKey(high.Scope, "head", high.Scope.ID), raw, 0).Err(); err != nil {
		t.Fatal(err)
	}
	high.BaseRevision++
	if _, err = store.ApplyAccepted(ctx, redisAccepted(high, "high-next"), high); err != nil {
		t.Fatal("high integer precision", err)
	}
	highLoaded, _ := store.Load(ctx, high.Scope)
	if highLoaded.Revision != 9007199254740994 {
		t.Fatal(highLoaded.Revision)
	}
	migrationScope := Scope{TenantID: "tenant", ID: "migration"}
	if err = sdk.AddMemory(ctx, migrationScope.Key(), "migrated redis head", []string{"migration"}); err != nil {
		t.Fatal(err)
	}
	migrationEntries, err := sdk.ReadMemories(ctx, migrationScope.Key(), 0)
	if err != nil {
		t.Fatal(err)
	}
	migrationSnapshot := Snapshot{Revision: 9, Entries: migrationEntries}
	if err = store.ImportSnapshot(ctx, migrationScope, migrationSnapshot); err != nil {
		t.Fatal("snapshot import", err)
	}
	if err = store.ImportSnapshot(ctx, migrationScope, migrationSnapshot); err != nil {
		t.Fatal("snapshot import replay", err)
	}
	migrated, err := store.Load(ctx, migrationScope)
	if err != nil || migrated.Revision != 9 || len(migrated.Entries) != 1 || migrated.Entries[0].Memory.Memory != "migrated redis head" {
		t.Fatal("snapshot import roundtrip", migrated, err)
	}
	changedMigration := migrationSnapshot
	changedMigration.Revision++
	if err = store.ImportSnapshot(ctx, migrationScope, changedMigration); !errors.Is(err, ErrConflict) {
		t.Fatal("snapshot import overwrite", err)
	}
	if ttl := admin.PTTL(ctx, redisKey(migrationScope, "head", migrationScope.ID)).Val(); ttl != -1 {
		t.Fatal("snapshot import TTL", ttl)
	}
	if err = admin.Set(ctx, redisKey(scope, "head", scope.ID), "corrupt", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Load(ctx, scope); !errors.Is(err, ErrCorrupt) {
		t.Fatal("corrupt head accepted", err)
	}
	bad := Candidate{Scope: Scope{TenantID: "tenant", ID: "badtype"}}
	badA := redisAccepted(bad, "badtype")
	if err = admin.LPush(ctx, redisKey(bad.Scope, "receipt", badA.CompletionID), "wrongtype").Err(); err != nil {
		t.Fatal(err)
	}
	if _, err = store.ApplyAccepted(ctx, badA, bad); !errors.Is(err, ErrCorrupt) {
		t.Fatal("type failure", err)
	}
	if exists, _ = admin.Exists(ctx, redisKey(bad.Scope, "head", bad.Scope.ID)).Result(); exists != 0 {
		t.Fatal("validation error wrote head")
	}
	durable := Candidate{Scope: Scope{TenantID: "tenant", ID: "persistence"}, Entries: nil}
	if _, err = store.ApplyAccepted(ctx, redisAccepted(durable, "persistence"), durable); err != nil {
		t.Fatal(err)
	}
	keys, _ := admin.Keys(ctx, "runtime_memory:*").Result()
	for _, key := range keys {
		ttl, e := admin.TTL(ctx, key).Result()
		if e != nil || ttl != -1 {
			t.Fatal("implicit TTL", ttl, e)
		}
	}
}
func TestRedisMemoryAfterRestart(t *testing.T) {
	if os.Getenv("WORKER_MEMORY_REDIS_VERIFY_RESTART") != "1" {
		t.Skip("separate post-restart phase")
	}
	ctx, store, _ := redisFixture(t)
	scope := Scope{TenantID: "tenant", ID: "persistence"}
	v, err := store.Load(ctx, scope)
	if err != nil || v.Revision != 1 {
		t.Fatal("AOF lost accepted head", err)
	}
	c := Candidate{Scope: scope}
	out, err := store.ApplyAccepted(ctx, redisAccepted(c, "persistence"), c)
	if err != nil || out.Revision != 1 {
		t.Fatal("AOF lost receipt", err)
	}
}
