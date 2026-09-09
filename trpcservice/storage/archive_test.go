package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// Archival moves rows older than the retention window into the archive tables
// and keeps fresh rows hot; a re-run is idempotent. Needs the PG from compose.
func TestArchiverMovesOldRows(t *testing.T) {
	_, pool := pgSessionService(t)
	ctx := context.Background()

	// One session with two events: one old, one fresh.
	key := testKey(t.Name())
	cleanupSession(t, pool, key)
	t.Cleanup(func() { cleanupSession(t, pool, key) })
	svc := storage.NewPGSessionService(pool)
	sess, err := svc.CreateSession(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.AppendEvent(ctx, sess, textEvent("old-1", "user", "旧消息")); err != nil {
		t.Fatal(err)
	}
	if err := svc.AppendEvent(ctx, sess, textEvent("new-1", "user", "新消息")); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-40 * 24 * time.Hour)
	if _, err := pool.Exec(ctx,
		`UPDATE session_event SET created_at = $1
		 WHERE event->>'id' = 'old-1' AND session_id IN
		   (SELECT id FROM session WHERE app_id=$2 AND session_key=$3)`,
		old, key.AppName, key.SessionID); err != nil {
		t.Fatal(err)
	}

	// One old + one fresh audit row for the test tenant.
	if _, err := pool.Exec(ctx,
		`INSERT INTO audit_log (tenant_id, decision, created_at) VALUES ($1, 'allow', $2), ($1, 'allow', now())`,
		testTenantID, old); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM audit_log WHERE tenant_id=$1 AND agent_name IS NULL AND session_id IS NULL`, testTenantID)
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM audit_log_archive WHERE tenant_id=$1 AND agent_name IS NULL AND session_id IS NULL`, testTenantID)
	})

	a := storage.NewArchiver(pool, 30*24*time.Hour, time.Hour)
	events, audits, err := a.ArchiveOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if events != 1 || audits != 1 {
		t.Fatalf("want 1 event + 1 audit archived, got %d/%d", events, audits)
	}

	var hotEvents, archivedEvents int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM session_event se JOIN session s ON s.id=se.session_id
		 WHERE s.app_id=$1 AND s.session_key=$2`, key.AppName, key.SessionID).Scan(&hotEvents); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM session_event_archive se JOIN session s ON s.id=se.session_id
		 WHERE s.app_id=$1 AND s.session_key=$2`, key.AppName, key.SessionID).Scan(&archivedEvents); err != nil {
		t.Fatal(err)
	}
	if hotEvents != 1 || archivedEvents != 1 {
		t.Fatalf("want 1 hot + 1 archived event, got %d/%d", hotEvents, archivedEvents)
	}

	// Idempotent: the second run moves nothing.
	events, audits, err = a.ArchiveOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if events != 0 || audits != 0 {
		t.Fatalf("second sweep must be a no-op, got %d/%d", events, audits)
	}
}

// A session quiet long enough for the sweep to archive every one of its events
// must still continue its sequence when the user comes back: the next event_seq
// is derived from the hot table and the archive together. Deriving it from the
// hot table alone restarts at 1, reusing seqs that still exist in
// session_event_archive — the next sweep violates uk_session_event_archive_seq
// and wedges the batch transaction.
func TestEventSeqSurvivesFullArchive(t *testing.T) {
	svc, pool := pgSessionService(t)
	ctx := context.Background()
	key := testKey(t.Name())
	cleanupSession(t, pool, key)
	t.Cleanup(func() { cleanupSession(t, pool, key) })
	// cleanupSession leaves the archive alone, and archived rows outlive their
	// session row (the archive tables drop the FKs): registered last so it runs
	// first, while the session row is still there to resolve the id.
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM session_event_archive WHERE session_id IN
			   (SELECT id FROM session WHERE app_id=$1 AND session_key=$2)`,
			key.AppName, key.SessionID)
	})

	sess, err := svc.CreateSession(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"seq-1", "seq-2"} {
		if err := svc.AppendEvent(ctx, sess, textEvent(id, "user", "旧消息")); err != nil {
			t.Fatal(err)
		}
	}
	var sessID string
	if err := pool.QueryRow(ctx,
		`SELECT id FROM session WHERE app_id=$1 AND session_key=$2`,
		key.AppName, key.SessionID).Scan(&sessID); err != nil {
		t.Fatal(err)
	}

	old := time.Now().Add(-40 * 24 * time.Hour)
	backdate := func() {
		t.Helper()
		if _, err := pool.Exec(ctx,
			`UPDATE session_event SET created_at = $1 WHERE session_id = $2`, old, sessID); err != nil {
			t.Fatal(err)
		}
	}
	a := storage.NewArchiver(pool, 30*24*time.Hour, time.Hour)

	// Archive both events, leaving this session with an empty hot journal.
	backdate()
	if _, _, err := a.ArchiveOnce(ctx); err != nil {
		t.Fatal(err)
	}
	var hot int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM session_event WHERE session_id=$1`, sessID).Scan(&hot); err != nil {
		t.Fatal(err)
	}
	if hot != 0 {
		t.Fatalf("the sweep must have emptied the hot journal, got %d rows", hot)
	}

	// The user returns: the next event continues at 3, it does not restart at 1.
	if err := svc.AppendEvent(ctx, sess, textEvent("seq-3", "user", "我回来了")); err != nil {
		t.Fatal(err)
	}
	var seq int64
	if err := pool.QueryRow(ctx,
		`SELECT event_seq FROM session_event WHERE session_id=$1`, sessID).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	if seq != 3 {
		t.Fatalf("event_seq must continue past the archived events, got %d, want 3", seq)
	}

	// And the new event is itself archivable: no uk_session_event_archive_seq
	// violation, all three rows end up in the archive.
	backdate()
	if _, _, err := a.ArchiveOnce(ctx); err != nil {
		t.Fatalf("re-archiving a continued sequence must not conflict: %v", err)
	}
	var archived int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM session_event_archive WHERE session_id=$1`, sessID).Scan(&archived); err != nil {
		t.Fatal(err)
	}
	if archived != 3 {
		t.Fatalf("want all 3 events archived, got %d", archived)
	}
}
