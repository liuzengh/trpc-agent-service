package storage_test

import (
	"context"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/session"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// ensureSession inserts the session row on first append, and DeleteSession on
// a missing session is a no-op.
func TestPGSessionEnsureInsertAndSummaryEdges(t *testing.T) {
	svc, pool := pgSessionService(t)
	ctx := context.Background()

	// AppendEvent on a session that was never created: the row is inserted by
	// ensureSession inside the append transaction.
	key := testKey(t.Name())
	cleanupSession(t, pool, key)
	t.Cleanup(func() { cleanupSession(t, pool, key) })
	fresh := &session.Session{AppName: key.AppName, UserID: key.UserID, ID: key.SessionID}
	if err := svc.AppendEvent(ctx, fresh, textEvent("ensure-1", "user", "首条消息")); err != nil {
		t.Fatal(err)
	}
	got, err := svc.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got.Events) != 1 || got.Events[0].ID != "ensure-1" {
		t.Fatalf("the appended event must persist, got %+v", got)
	}

	// Deleting a session that does not exist is a no-op, not an error.
	if err := svc.DeleteSession(ctx, session.Key{AppName: key.AppName, UserID: key.UserID, SessionID: "dm:mock:never-" + t.Name()}); err != nil {
		t.Fatalf("delete of a missing session must be a no-op: %v", err)
	}
}

// Summary edge cases: a cursor pointing at a vanished event disables the
// summary (full replay), empty sessions and empty summarizer output are
// no-ops, and a summarizer failure surfaces as an error.
func TestPGSessionSummaryCursorAndEmptyEdges(t *testing.T) {
	_, pool := pgSessionService(t)
	ctx := context.Background()

	key := testKey(t.Name())
	cleanupSession(t, pool, key)
	t.Cleanup(func() { cleanupSession(t, pool, key) })

	svc := storage.NewPGSessionService(pool, storage.WithSummarizer(&fakeSummarizer{}))
	t.Cleanup(func() { _ = svc.Close() })

	sess, err := svc.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatal(err)
	}
	// A session with no journaled events: summarization is a no-op.
	if err := svc.CreateSessionSummary(ctx, sess, "assistant", true); err != nil {
		t.Fatalf("summarizing an empty session must be a no-op: %v", err)
	}
	// A session that was never persisted: also a no-op.
	ghost := &session.Session{AppName: key.AppName, UserID: key.UserID, ID: "dm:mock:ghost-" + t.Name()}
	if err := svc.CreateSessionSummary(ctx, ghost, "assistant", true); err != nil {
		t.Fatalf("summarizing an unpersisted session must be a no-op: %v", err)
	}

	// Events + a summary whose covered_event_id points nowhere: the summary
	// is ignored and the session falls back to a full replay.
	if err := svc.AppendEvent(ctx, sess, textEvent("edge-1", "user", "消息一")); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO summary (session_id, summary_text, covered_event_id, filter_key)
		 SELECT id, '孤儿摘要', gen_random_uuid(), 'assistant'
		 FROM session WHERE app_id=$1 AND session_key=$2`,
		key.AppName, key.SessionID); err != nil {
		t.Fatal(err)
	}
	got, err := svc.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Events) != 1 {
		t.Fatalf("a dangling cursor must fall back to a full replay, got %d events", len(got.Events))
	}
	if _, ok := got.Summaries["assistant"]; ok {
		t.Fatal("a summary with a dangling cursor must be ignored")
	}

	// An empty summarizer response must not write a summary row.
	svcEmpty := storage.NewPGSessionService(pool, storage.WithSummarizer(&emptySummarizer{}))
	t.Cleanup(func() { _ = svcEmpty.Close() })
	if err := svcEmpty.CreateSessionSummary(ctx, sess, "assistant", true); err != nil {
		t.Fatalf("empty summary text must be a no-op, not an error: %v", err)
	}
	var n int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM summary WHERE session_id IN
		   (SELECT id FROM session WHERE app_id=$1 AND session_key=$2)`,
		key.AppName, key.SessionID).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 { // only the dangling-cursor row from above
		t.Fatalf("empty summarizer output must not write a summary row, got %d rows", n)
	}
}

// emptySummarizer returns valid but empty text.
type emptySummarizer struct {
	fakeSummarizer
}

func (*emptySummarizer) Summarize(context.Context, *session.Session) (string, error) {
	return "  ", nil
}

// ListSessions returns the user's sessions with replayed events; the
// metadata-only option drops the events.
func TestPGSessionListSessions(t *testing.T) {
	svc, pool := pgSessionService(t)
	ctx := context.Background()

	// A dedicated user keeps the assertion immune to other tests' sessions
	// under the shared fixture app.
	userID := "u-pg-list-" + t.Name()
	keys := []session.Key{
		{AppName: testAppID, UserID: userID, SessionID: "dm:mock:list-1-" + t.Name()},
		{AppName: testAppID, UserID: userID, SessionID: "dm:mock:list-2-" + t.Name()},
	}
	for _, k := range keys {
		cleanupSession(t, pool, k)
		t.Cleanup(func() { cleanupSession(t, pool, k) })
	}

	sess1, err := svc.CreateSession(ctx, keys[0], session.StateMap{})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.AppendEvent(ctx, sess1, textEvent("list-e1", "user", "消息一")); err != nil {
		t.Fatal(err)
	}
	sess2, err := svc.CreateSession(ctx, keys[1], session.StateMap{})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.AppendEvent(ctx, sess2, textEvent("list-e2", "user", "消息二")); err != nil {
		t.Fatal(err)
	}

	userKey := session.UserKey{AppName: testAppID, UserID: userID}
	got, err := svc.ListSessions(ctx, userKey)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 sessions, got %d", len(got))
	}
	eventsByID := map[string]int{}
	for _, s := range got {
		eventsByID[s.ID] = len(s.Events)
	}
	if eventsByID[keys[0].SessionID] != 1 || eventsByID[keys[1].SessionID] != 1 {
		t.Fatalf("each listed session must carry its replayed events: %v", eventsByID)
	}

	// Metadata-only: events are omitted.
	meta, err := svc.ListSessions(ctx, userKey, session.WithListSessionOnlyMeta())
	if err != nil {
		t.Fatal(err)
	}
	if len(meta) != 2 {
		t.Fatalf("metadata listing must still return both sessions, got %d", len(meta))
	}
	for _, s := range meta {
		if len(s.Events) != 0 {
			t.Fatalf("metadata-only listing must omit events, got %d for %s", len(s.Events), s.ID)
		}
	}

	// An invalid user key is rejected before the query.
	if _, err := svc.ListSessions(ctx, session.UserKey{AppName: testAppID}); err == nil {
		t.Fatal("missing user id must be rejected")
	}
}

// CreateSession is idempotent per (app_id, session_key): the second call
// returns the stored row without overwriting its state.
func TestPGSessionCreateExistingReturnsStored(t *testing.T) {
	svc, pool := pgSessionService(t)
	ctx := context.Background()
	key := testKey(t.Name())
	cleanupSession(t, pool, key)
	t.Cleanup(func() { cleanupSession(t, pool, key) })

	first, err := svc.CreateSession(ctx, key, session.StateMap{"keep": []byte(`1`)})
	if err != nil {
		t.Fatal(err)
	}
	second, err := svc.CreateSession(ctx, key, session.StateMap{"other": []byte(`2`)})
	if err != nil {
		t.Fatal(err)
	}
	if !second.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("the existing row must be returned unchanged (created %s vs %s)",
			first.CreatedAt, second.CreatedAt)
	}
	if _, ok := second.State["keep"]; !ok {
		t.Fatalf("existing state must not be overwritten: %v", second.State)
	}
	if _, ok := second.State["other"]; ok {
		t.Fatalf("the new state must not be merged into the existing row: %v", second.State)
	}
}

// Input validation and the unsupported state scopes: every rejected path
// must return an error instead of touching the database.
func TestPGSessionValidationAndScopeErrors(t *testing.T) {
	svc, _ := pgSessionService(t)
	ctx := context.Background()

	// Key validation.
	if _, err := svc.CreateSession(ctx, session.Key{}, nil); err == nil {
		t.Fatal("an empty session key must be rejected")
	}
	if _, err := svc.GetSession(ctx, session.Key{AppName: "a", UserID: "u"}); err == nil {
		t.Fatal("a session key without SessionID must be rejected")
	}
	if err := svc.DeleteSession(ctx, session.Key{UserID: "u", SessionID: "s"}); err == nil {
		t.Fatal("a session key without AppName must be rejected")
	}

	// Nil session / event are rejected by AppendEvent.
	sess, err := svc.CreateSession(ctx, testKey(t.Name()), session.StateMap{})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.AppendEvent(ctx, nil, textEvent("v1", "user", "x")); err == nil {
		t.Fatal("a nil session must be rejected")
	}
	if err := svc.AppendEvent(ctx, sess, nil); err == nil {
		t.Fatal("a nil event must be rejected")
	}
	bad := &session.Session{AppName: "", UserID: "u", ID: "dm:mock:bad"}
	if err := svc.AppendEvent(ctx, bad, textEvent("v2", "user", "x")); err == nil {
		t.Fatal("an invalid session key must be rejected")
	}

	// An app without an agent_app row cannot resolve its tenant.
	ghost := &session.Session{AppName: "no-such-app-" + t.Name(), UserID: "u", ID: "dm:mock:ghost"}
	if err := svc.AppendEvent(ctx, ghost, textEvent("v3", "user", "x")); err == nil {
		t.Fatal("an unknown app must fail tenant resolution")
	}

	// The remaining state scopes are unsupported on the PG backend.
	if err := svc.DeleteAppState(ctx, testAppID, "k"); err == nil {
		t.Fatal("DeleteAppState must be unsupported")
	}
	if _, err := svc.ListAppStates(ctx, testAppID); err == nil {
		t.Fatal("ListAppStates must be unsupported")
	}
	if err := svc.UpdateUserState(ctx, session.UserKey{AppName: testAppID, UserID: "u"}, nil); err == nil {
		t.Fatal("UpdateUserState must be unsupported")
	}
	if err := svc.DeleteUserState(ctx, session.UserKey{AppName: testAppID, UserID: "u"}, "k"); err == nil {
		t.Fatal("DeleteUserState must be unsupported")
	}

	// Without a configured summarizer both summary entry points are no-ops,
	// and the text read misses.
	if err := svc.CreateSessionSummary(ctx, sess, "", true); err != nil {
		t.Fatalf("summary without summarizer must be a no-op: %v", err)
	}
	if err := svc.EnqueueSummaryJob(ctx, sess, "", true); err != nil {
		t.Fatalf("summary enqueue without summarizer must be a no-op: %v", err)
	}
	if text, ok := svc.GetSessionSummaryText(ctx, sess); ok {
		t.Fatalf("no summary expected, got %q", text)
	}
	if text, ok := svc.GetSessionSummaryText(ctx, nil); ok {
		t.Fatalf("a nil session must read as no summary, got %q", text)
	}
}

// A dead pool turns every entry point into a clean error (no panic, no
// partial write), and GetSessionSummaryText reads a failed lookup as missing.
func TestPGSessionClosedPoolErrors(t *testing.T) {
	_, pool := pgSessionService(t)
	svc := storage.NewPGSessionService(pool)
	pool.Close()
	ctx := context.Background()

	key := session.Key{AppName: testAppID, UserID: "u-pg", SessionID: "dm:mock:closed-" + t.Name()}
	if _, err := svc.CreateSession(ctx, key, session.StateMap{}); err == nil {
		t.Fatal("CreateSession must fail on a closed pool (tenant resolution)")
	}
	if _, err := svc.GetSession(ctx, key); err == nil {
		t.Fatal("GetSession must fail on a closed pool")
	}
	if _, err := svc.ListSessions(ctx, session.UserKey{AppName: testAppID, UserID: "u-pg"}); err == nil {
		t.Fatal("ListSessions must fail on a closed pool")
	}
	if err := svc.DeleteSession(ctx, key); err == nil {
		t.Fatal("DeleteSession must fail on a closed pool")
	}
	sess := &session.Session{AppName: key.AppName, UserID: key.UserID, ID: key.SessionID}
	if err := svc.AppendEvent(ctx, sess, textEvent("c1", "user", "x")); err == nil {
		t.Fatal("AppendEvent must fail on a closed pool")
	}
	if err := svc.UpdateSessionState(ctx, key, session.StateMap{"k": []byte(`1`)}); err == nil {
		t.Fatal("UpdateSessionState must fail on a closed pool")
	}
	if _, ok := svc.GetSessionSummaryText(ctx, sess); ok {
		t.Fatal("a failed summary lookup must read as missing")
	}
}

// The summary stays valid after its covered event has been archived away:
// loadSummary resolves the cursor through session_event_archive.
func TestPGSessionSummarySurvivesArchival(t *testing.T) {
	_, pool := pgSessionService(t)
	ctx := context.Background()
	key := testKey(t.Name())
	cleanupSession(t, pool, key)
	t.Cleanup(func() {
		// The archive table has no FKs; drop our archived rows first (while
		// the session row still identifies them), then the hot side.
		_, _ = pool.Exec(context.Background(),
			`DELETE FROM session_event_archive WHERE session_id IN
			   (SELECT id FROM session WHERE app_id=$1 AND session_key=$2)`, key.AppName, key.SessionID)
		cleanupSession(t, pool, key)
	})

	fake := &fakeSummarizer{}
	svcSum := storage.NewPGSessionService(pool, storage.WithSummarizer(fake))
	t.Cleanup(func() { _ = svcSum.Close() })

	sess, err := svcSum.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatal(err)
	}
	if err := svcSum.AppendEvent(ctx, sess, textEvent("arch-1", "user", "旧消息")); err != nil {
		t.Fatal(err)
	}
	if err := svcSum.AppendEvent(ctx, sess, textEvent("arch-2", "user", "新消息")); err != nil {
		t.Fatal(err)
	}
	if err := svcSum.CreateSessionSummary(ctx, sess, "assistant", true); err != nil {
		t.Fatal(err)
	}

	// Archive everything older than "now": both journaled events move to
	// session_event_archive; the summary row's cursor dangles in the hot
	// table but resolves in the archive.
	a := storage.NewArchiver(pool, time.Nanosecond, time.Hour)
	events, _, err := a.ArchiveOnce(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if events < 2 {
		t.Fatalf("both events must be archived, got %d", events)
	}

	got, err := svcSum.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("session must survive archival")
	}
	if len(got.Events) != 0 {
		t.Fatalf("both events are covered by the summary, got %d replayed", len(got.Events))
	}
	sum := got.Summaries["assistant"]
	if sum == nil || sum.Summary != "摘要v1" {
		t.Fatalf("summary must surface via the archive join: %+v", got.Summaries)
	}
	if sum.Boundary == nil || sum.Boundary.LastEventID != "arch-2" {
		t.Fatalf("boundary must anchor at the archived covered event: %+v", sum.Boundary)
	}
}
func TestPGSessionEventTimeFilterAndUnknownChannel(t *testing.T) {
	svc, pool := pgSessionService(t)
	ctx := context.Background()

	// No colon → no channel segment → "unknown".
	key := session.Key{AppName: testAppID, UserID: "u-pg", SessionID: "plain-session-" + t.Name()}
	cleanupSession(t, pool, key)
	t.Cleanup(func() { cleanupSession(t, pool, key) })

	sess, err := svc.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatal(err)
	}
	older := textEvent("t1", "user", "消息一")
	older.Timestamp = time.Now().Add(-2 * time.Hour)
	newer := textEvent("t2", "user", "消息二")
	newer.Timestamp = time.Now().Add(-30 * time.Minute)
	if err := svc.AppendEvent(ctx, sess, older); err != nil {
		t.Fatal(err)
	}
	if err := svc.AppendEvent(ctx, sess, newer); err != nil {
		t.Fatal(err)
	}

	var channel string
	if err := pool.QueryRow(ctx,
		`SELECT channel FROM session WHERE app_id=$1 AND session_key=$2`,
		key.AppName, key.SessionID).Scan(&channel); err != nil {
		t.Fatal(err)
	}
	if channel != "unknown" {
		t.Fatalf("a key without a channel segment must store 'unknown', got %q", channel)
	}

	// A future cutoff drops everything; a cutoff between the two timestamps
	// keeps only the newer event; a cutoff before both keeps everything.
	future, err := svc.GetSession(ctx, key, session.WithEventTime(time.Now().Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	if len(future.Events) != 0 {
		t.Fatalf("a future cutoff must drop all events, got %d", len(future.Events))
	}
	middle, err := svc.GetSession(ctx, key, session.WithEventTime(time.Now().Add(-time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	if len(middle.Events) != 1 || middle.Events[0].ID != "t2" {
		t.Fatalf("the cutoff must keep only the newer event, got %+v", middle.Events)
	}
	past, err := svc.GetSession(ctx, key, session.WithEventTime(time.Now().Add(-3*time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	if len(past.Events) != 2 {
		t.Fatalf("an early cutoff must keep all events, got %d", len(past.Events))
	}
}

// Deleting a session must take its archived events with it. session_event_archive
// declares no FK to session, so without an explicit delete the archived half of
// a conversation outlives the request that was supposed to erase it.
func TestPGSessionDeleteRemovesArchivedEvents(t *testing.T) {
	svc, pool := pgSessionService(t)
	ctx := context.Background()
	key := testKey(t.Name())
	cleanupSession(t, pool, key)
	t.Cleanup(func() { cleanupSession(t, pool, key) })

	if _, err := svc.CreateSession(ctx, key, session.StateMap{}); err != nil {
		t.Fatal(err)
	}
	var sessID string
	if err := pool.QueryRow(ctx,
		`SELECT id FROM session WHERE app_id=$1 AND session_key=$2`,
		key.AppName, key.SessionID).Scan(&sessID); err != nil {
		t.Fatal(err)
	}
	// An archived event, as the archive task leaves it behind after moving
	// the row out of session_event.
	if _, err := pool.Exec(ctx,
		`INSERT INTO session_event_archive (id, session_id, event_seq, event, created_at)
		 VALUES (gen_random_uuid(), $1, 1, '{"archived":true}', now())`, sessID); err != nil {
		t.Fatal(err)
	}

	if err := svc.DeleteSession(ctx, key); err != nil {
		t.Fatal(err)
	}
	var left int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM session_event_archive WHERE session_id=$1`, sessID).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Fatalf("archived events must go with the session, got %d left", left)
	}
}
