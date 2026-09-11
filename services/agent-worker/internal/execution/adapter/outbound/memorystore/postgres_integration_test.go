package memorystore

import (
	"context"
	"errors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/memorymigrations"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"
	"trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
)

func TestMemoryPostgresAcceptedContract(t *testing.T) {
	adminURL, migrationURL, runtimeURL := os.Getenv("WORKER_MEMORY_TEST_ADMIN_URL"), os.Getenv("WORKER_MEMORY_TEST_MIGRATION_URL"), os.Getenv("WORKER_MEMORY_TEST_RUNTIME_URL")
	if adminURL == "" || migrationURL == "" || runtimeURL == "" {
		t.Skip("explicit disposable Memory PostgreSQL URLs required")
	}
	if os.Getenv("WORKER_MEMORY_TEST_ALLOW_RESET") != "1" {
		t.Fatal("requires disposable schema reset opt-in")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, adminURL)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	migration, err := pgxpool.New(ctx, migrationURL)
	if err != nil {
		t.Fatal(err)
	}
	defer migration.Close()
	cfg, err := pgxpool.ParseConfig(runtimeURL)
	if err != nil {
		t.Fatal(err)
	}
	u, _ := url.Parse(runtimeURL)
	target := Target{MaxConcurrency: 2, Host: cfg.ConnConfig.Host, Port: cfg.ConnConfig.Port, Database: cfg.ConnConfig.Database, Username: cfg.ConnConfig.User, SSLMode: u.Query().Get("sslmode")}
	if _, err = admin.Exec(ctx, `DROP TABLE IF EXISTS runtime_memory.memory_receipts,runtime_memory.memory_heads,runtime_memory.memory_schema_migrations`); err != nil {
		t.Fatal(err)
	}
	if _, err = Open(ctx, runtimeURL, target, 1<<20); !errors.Is(err, ErrPreparation) {
		t.Fatal("implicit preparation", err)
	}
	if err = memorymigrations.Apply(ctx, admin); err == nil {
		t.Fatal("admin migration allowed")
	}
	if err = memorymigrations.Apply(ctx, migration); err != nil {
		t.Fatal(err)
	}
	if err = memorymigrations.Apply(ctx, migration); err != nil {
		t.Fatal("migration replay", err)
	}
	store, err := Open(ctx, runtimeURL, target, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if store.pool.Config().MaxConns != 2 {
		t.Fatal("published Memory concurrency not applied")
	}
	scope := Scope{TenantID: "tenant", ID: "scope"}
	empty, err := store.Load(ctx, scope)
	if err != nil || empty.Revision != 0 || len(empty.Entries) != 0 {
		t.Fatal(empty, err)
	}
	sdk := inmemory.NewMemoryService()
	defer sdk.Close()
	if err = sdk.AddMemory(ctx, scope.Key(), "remember tea", []string{"drink"}); err != nil {
		t.Fatal(err)
	}
	entries, err := sdk.ReadMemories(ctx, scope.Key(), 0)
	if err != nil {
		t.Fatal(err)
	}
	candidate := Candidate{Scope: scope, Entries: entries}
	hash, _ := candidate.Digest()
	accepted := Accepted{CompletionID: "completion", RunID: "run", AttemptID: "attempt", CandidateDigest: hash}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, e := store.ApplyAccepted(ctx, accepted, candidate)
			if e == nil && (out.Revision != 1 || len(out.Entries) != 1) {
				e = ErrCorrupt
			}
			errs <- e
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal("idempotent replay", e)
		}
	}
	loaded, err := store.Load(ctx, scope)
	if err != nil || loaded.Revision != 1 || loaded.Entries[0].Memory.Memory != "remember tea" {
		t.Fatal(loaded, err)
	}
	loaded.Entries[0].Memory.Memory = "caller mutation"
	reloaded, _ := store.Load(ctx, scope)
	if reloaded.Entries[0].Memory.Memory != "remember tea" {
		t.Fatal("alias escaped")
	}
	stale := accepted
	stale.CompletionID = "stale"
	if _, err = store.ApplyAccepted(ctx, stale, candidate); !errors.Is(err, ErrConflict) {
		t.Fatal("stale CAS", err)
	}
	wrong := candidate
	wrong.Scope.ID = "foreign"
	if _, err = wrong.Digest(); !errors.Is(err, ErrIdentity) {
		t.Fatal("cross scope entries accepted", err)
	}
	for _, foreign := range []Scope{{TenantID: "other", ID: "scope"}, {TenantID: "tenant", ID: "other"}} {
		out, e := store.Load(ctx, foreign)
		if e != nil || out.Revision != 0 || len(out.Entries) != 0 {
			t.Fatal("scope leak", e)
		}
	}
	clear := Candidate{Scope: scope, BaseRevision: 1}
	clearHash, _ := clear.Digest()
	clearAccepted := Accepted{CompletionID: "clear", RunID: "run2", AttemptID: "attempt2", CandidateDigest: clearHash}
	if _, err = store.ApplyAccepted(ctx, clearAccepted, clear); err != nil {
		t.Fatal(err)
	}
	replay, err := store.ApplyAccepted(ctx, accepted, candidate)
	if err != nil || replay.Revision != 1 || len(replay.Entries) != 1 {
		t.Fatal("old completion must return original application", err)
	}
	after, err := store.Load(ctx, scope)
	if err != nil || after.Revision != 2 || len(after.Entries) != 0 {
		t.Fatal("old replay overwrote head", err)
	}
	conflict := clearAccepted
	conflict.CompletionID = accepted.CompletionID
	if _, err = store.ApplyAccepted(ctx, conflict, clear); !errors.Is(err, ErrConflict) {
		t.Fatal("completion changed candidate", err)
	}
	foreign := Candidate{Scope: Scope{TenantID: scope.TenantID, ID: "foreign"}}
	foreignHash, _ := foreign.Digest()
	same := accepted
	same.CandidateDigest = foreignHash
	if _, err = store.ApplyAccepted(ctx, same, foreign); !errors.Is(err, ErrConflict) {
		t.Fatal("completion scope reused", err)
	}
	cancelled, c := context.WithCancel(ctx)
	c()
	if _, err = store.Load(cancelled, scope); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel ignored", err)
	}
	// A receipt failure after the head UPDATE must roll the whole transaction back.
	if _, err = admin.Exec(ctx, `ALTER TABLE runtime_memory.memory_receipts ADD CONSTRAINT reject_fixture CHECK(completion_id <> 'fail-after-head')`); err != nil {
		t.Fatal(err)
	}
	nextCandidate := Candidate{Scope: scope, BaseRevision: 2}
	nextHash, _ := nextCandidate.Digest()
	if _, err = store.ApplyAccepted(ctx, Accepted{CompletionID: "fail-after-head", RunID: "run3", AttemptID: "attempt3", CandidateDigest: nextHash}, nextCandidate); err == nil {
		t.Fatal("injected write failure ignored")
	}
	rollbackHead, err := store.Load(ctx, scope)
	if err != nil || rollbackHead.Revision != 2 {
		t.Fatal("partial head write survived failed receipt", err)
	}
	reused := clearAccepted
	reused.CompletionID = "same-attempt-different-completion"
	reused.CandidateDigest = nextHash
	if _, err = store.ApplyAccepted(ctx, reused, nextCandidate); !errors.Is(err, ErrConflict) {
		t.Fatal("same attempt applied twice", err)
	}
	// Two accepted candidates based on the same scope revision cannot both win.
	competing := Candidate{Scope: Scope{TenantID: "tenant", ID: "competing"}}
	competingHash, _ := competing.Digest()
	outcomes := make(chan error, 2)
	for _, id := range []string{"winner-a", "winner-b"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			_, e := store.ApplyAccepted(ctx, Accepted{CompletionID: id, RunID: id, AttemptID: id, CandidateDigest: competingHash}, competing)
			outcomes <- e
		}(id)
	}
	wg.Wait()
	close(outcomes)
	wins, conflicts := 0, 0
	for e := range outcomes {
		if e == nil {
			wins++
		} else if errors.Is(e, ErrConflict) {
			conflicts++
		} else {
			t.Fatal(e)
		}
	}
	if wins != 1 || conflicts != 1 {
		t.Fatal("revision CAS allowed competing writes", wins, conflicts)
	}
	small, err := Open(ctx, runtimeURL, target, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = small.Load(ctx, scope); !errors.Is(err, ErrCapacity) {
		t.Fatal("read capacity", err)
	}
	if _, err = small.ApplyAccepted(ctx, accepted, candidate); !errors.Is(err, ErrCapacity) {
		t.Fatal("write capacity", err)
	}
	small.Close()
	migrationScope := Scope{TenantID: "tenant", ID: "migration"}
	if err = sdk.AddMemory(ctx, migrationScope.Key(), "migrated postgres head", []string{"migration"}); err != nil {
		t.Fatal(err)
	}
	migrationEntries, err := sdk.ReadMemories(ctx, migrationScope.Key(), 0)
	if err != nil {
		t.Fatal(err)
	}
	migrationSnapshot := Snapshot{Revision: 7, Entries: migrationEntries}
	if err = store.ImportSnapshot(ctx, migrationScope, migrationSnapshot); err != nil {
		t.Fatal("snapshot import", err)
	}
	if err = store.ImportSnapshot(ctx, migrationScope, migrationSnapshot); err != nil {
		t.Fatal("snapshot import replay", err)
	}
	migrated, err := store.Load(ctx, migrationScope)
	if err != nil || migrated.Revision != 7 || len(migrated.Entries) != 1 || migrated.Entries[0].Memory.Memory != "migrated postgres head" {
		t.Fatal("snapshot import roundtrip", migrated, err)
	}
	changedMigration := migrationSnapshot
	changedMigration.Revision++
	if err = store.ImportSnapshot(ctx, migrationScope, changedMigration); !errors.Is(err, ErrConflict) {
		t.Fatal("snapshot import overwrite", err)
	}
	var receipts int
	if err = admin.QueryRow(ctx, `SELECT count(*) FROM runtime_memory.memory_receipts`).Scan(&receipts); err != nil || receipts != 3 {
		t.Fatal("failed calls left receipts", receipts, err)
	}
	if _, err = admin.Exec(ctx, `UPDATE runtime_memory.memory_heads SET digest='tampered' WHERE tenant_id='tenant'`); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Load(ctx, scope); !errors.Is(err, ErrCorrupt) {
		t.Fatal("tampering undetected", err)
	}
}
