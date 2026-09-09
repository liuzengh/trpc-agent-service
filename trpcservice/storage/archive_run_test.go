package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// Run sweeps at startup and on every tick until the context is canceled; a
// canceled sweep path must not hang.
func TestArchiverRunSweepsOnTicksUntilCancel(t *testing.T) {
	_, pool := pgSessionService(t)
	ctx := context.Background()

	// One old session event, batched one row at a time to exercise the
	// pause-and-continue loop.
	key := testKey(t.Name())
	cleanupSession(t, pool, key)
	t.Cleanup(func() { cleanupSession(t, pool, key) })
	svc := storage.NewPGSessionService(pool)
	sess, err := svc.CreateSession(ctx, key, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.AppendEvent(ctx, sess, textEvent("run-old", "user", "旧消息")); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-40 * 24 * time.Hour)
	if _, err := pool.Exec(ctx,
		`UPDATE session_event SET created_at = $1
		 WHERE event->>'id' = 'run-old' AND session_id IN
		   (SELECT id FROM session WHERE app_id=$2 AND session_key=$3)`,
		old, key.AppName, key.SessionID); err != nil {
		t.Fatal(err)
	}

	// One old audit row, tagged so the assertion is immune to other rows.
	marker := "archiver-run-" + t.Name()
	if _, err := pool.Exec(ctx,
		`INSERT INTO audit_log (tenant_id, decision, error_type, created_at)
		 VALUES ($1, 'allow', $2, $3)`, testTenantID, marker, old); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM audit_log WHERE error_type = $1`, marker)
		_, _ = pool.Exec(context.Background(), `DELETE FROM audit_log_archive WHERE error_type = $1`, marker)
	})

	a := storage.NewArchiver(pool, 30*24*time.Hour, 20*time.Millisecond)
	a.BatchSize = 1
	a.BatchPause = 5 * time.Millisecond

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		a.Run(runCtx)
		close(done)
	}()

	// The startup sweep archives both old rows.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var archivedEvents, archivedAudits int
		if err := pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM session_event_archive se JOIN session s ON s.id=se.session_id
			 WHERE s.app_id=$1 AND s.session_key=$2`, key.AppName, key.SessionID).Scan(&archivedEvents); err != nil {
			t.Fatal(err)
		}
		if err := pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM audit_log_archive WHERE error_type = $1`, marker).Scan(&archivedAudits); err != nil {
			t.Fatal(err)
		}
		if archivedEvents == 1 && archivedAudits == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("startup sweep did not archive (events=%d audits=%d)", archivedEvents, archivedAudits)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Let a few ticker sweeps run (no-ops now), then cancel: Run must exit.
	time.Sleep(60 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}

	var hotEvents int
	if err := pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM session_event se JOIN session s ON s.id=se.session_id
		 WHERE s.app_id=$1 AND s.session_key=$2`, key.AppName, key.SessionID).Scan(&hotEvents); err != nil {
		t.Fatal(err)
	}
	if hotEvents != 0 {
		t.Fatalf("the old event must have left the hot table, got %d", hotEvents)
	}
}

// A sweep against a dead database logs and returns instead of hanging or
// panicking; Run still honors the canceled context.
func TestArchiverRunSweepFailurePath(t *testing.T) {
	_, pool := pgSessionService(t)
	a := storage.NewArchiver(pool, time.Hour, time.Hour)
	pool.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // canceled before the first sweep: every batch fails fast
	done := make(chan struct{})
	go func() {
		a.Run(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run must exit promptly when the context is already canceled")
	}
}
