package sessionstore

import (
	"context"
	"errors"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/services/agent-worker/sessionmigrations"
)

func TestSessionCandidatePostgresContract(t *testing.T) {
	adminURL, migrationURL, runtimeURL := os.Getenv("WORKER_SESSION_TEST_ADMIN_URL"), os.Getenv("WORKER_SESSION_TEST_MIGRATION_URL"), os.Getenv("WORKER_SESSION_TEST_RUNTIME_URL")
	if adminURL == "" || migrationURL == "" || runtimeURL == "" {
		t.Skip("WORKER_SESSION_TEST_ADMIN_URL, WORKER_SESSION_TEST_MIGRATION_URL and WORKER_SESSION_TEST_RUNTIME_URL are required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	target := Target{Host: cfg.ConnConfig.Host, Port: cfg.ConnConfig.Port, Database: cfg.ConnConfig.Database, Username: cfg.ConnConfig.User, SSLMode: u.Query().Get("sslmode")}
	// This integration contract intentionally owns its disposable namespace. It
	// never runs against a shared production instance: require explicit opt-in.
	if os.Getenv("WORKER_SESSION_TEST_ALLOW_RESET") != "1" {
		t.Fatal("disposable session schema requires WORKER_SESSION_TEST_ALLOW_RESET=1")
	}
	if _, err = admin.Exec(ctx, `DROP TABLE IF EXISTS runtime_session.session_candidates,runtime_session.session_schema_migrations`); err != nil {
		t.Fatal(err)
	}
	if _, err = Open(ctx, runtimeURL, target, 1<<20); !errors.Is(err, ErrPreparation) {
		t.Fatalf("missing schema expected preparation error, got %v", err)
	}
	var present *string
	if err = admin.QueryRow(ctx, `SELECT to_regclass('runtime_session.session_candidates')::text`).Scan(&present); err != nil || present != nil {
		t.Fatalf("Open ran implicit migration: present=%v err=%v", present, err)
	}
	if err = sessionmigrations.Apply(ctx, admin); err == nil {
		t.Fatal("administrator migration identity accepted")
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- sessionmigrations.Apply(ctx, migration) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	store, err := Open(ctx, runtimeURL, target, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	candidate := fixture()
	heads := make(chan Head, 12)
	errs = make(chan error, 12)
	for range 12 {
		wg.Add(1)
		go func() { defer wg.Done(); head, err := store.Put(ctx, candidate); heads <- head; errs <- err }()
	}
	wg.Wait()
	close(heads)
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var head Head
	for got := range heads {
		if head.Ref != "" && got != head {
			t.Fatal("concurrent replay returned distinct candidate")
		}
		head = got
	}
	var count int
	if err = admin.QueryRow(ctx, `SELECT count(*) FROM runtime_session.session_candidates`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	loaded, err := store.Load(ctx, "tenant-a", "session-a", head)
	if err != nil || string(loaded.Snapshot) != string(candidate.Snapshot) {
		t.Fatalf("roundtrip=%+v err=%v", loaded, err)
	}
	for _, ids := range [][2]string{{"tenant-b", "session-a"}, {"tenant-a", "session-b"}} {
		if _, err = store.Load(ctx, ids[0], ids[1], head); !errors.Is(err, ErrNotFound) {
			t.Fatalf("cross scope=%v", err)
		}
	}
	conflict := candidate
	conflict.Snapshot = []byte(`{"events":["mutated"]}`)
	if _, err = store.Put(ctx, conflict); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict=%v", err)
	}
	conflict = candidate
	conflict.Parent = head
	if _, err = store.Put(ctx, conflict); !errors.Is(err, ErrConflict) {
		t.Fatalf("parent conflict=%v", err)
	}
	// Simulate a durable but unaccepted orphan; the next accepted-head read must
	// load exactly the caller's head, never the newest attempt row.
	orphan := candidate
	orphan.Identity.AttemptID = "late-attempt"
	orphan.Snapshot = []byte(`{"events":["unaccepted partial"]}`)
	orphanHead, err := store.Put(ctx, orphan)
	if err != nil {
		t.Fatal(err)
	}
	if orphanHead == head {
		t.Fatal("attempt key collision")
	}
	loaded, err = store.Load(ctx, "tenant-a", "session-a", head)
	if err != nil || string(loaded.Snapshot) != string(candidate.Snapshot) {
		t.Fatal("orphan replaced accepted content")
	}
	for _, sql := range []string{`UPDATE runtime_session.session_candidates SET content='bad'`, `DELETE FROM runtime_session.session_candidates`, `TRUNCATE runtime_session.session_candidates`, `CREATE TABLE runtime_session.implicit_ddl(id int)`, `INSERT INTO runtime_session.session_schema_migrations(version,digest) VALUES('forged','forged')`, `UPDATE runtime_session.session_schema_migrations SET digest='forged'`} {
		if _, err = store.pool.Exec(ctx, sql); err == nil {
			t.Fatalf("runtime mutation passed: %s", sql)
		}
	}
	badTarget := target
	badTarget.Database = "other"
	if _, err = Open(ctx, runtimeURL, badTarget, 1<<20); !errors.Is(err, ErrIdentity) {
		t.Fatalf("target change=%v", err)
	}
	if _, err = admin.Exec(ctx, `UPDATE runtime_session.session_candidates SET content=convert_to('{"corrupt":true}','UTF8') WHERE candidate_ref=$1`, head.Ref); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Load(ctx, "tenant-a", "session-a", head); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("tampered content=%v", err)
	}
	if _, err = admin.Exec(ctx, `DELETE FROM runtime_session.session_candidates WHERE tenant_id='tenant-a' AND session_id='session-a'`); err != nil {
		t.Fatal(err)
	}
	t.Log("SESSION CANDIDATE PASS: 8 concurrent migrations, 12 immutable replays, fixed-key conflicts, cross-scope rejection, no implicit DDL, orphan isolation, digest verification, runtime append-only permissions")
}
