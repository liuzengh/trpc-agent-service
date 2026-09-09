package storage_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// testTenantID and testAppID are the fixture rows the PG tests run against,
// kept separate from the demo seed. Skips when PG is unreachable.
const (
	testTenantID = "00000000-0000-0000-0000-0000000000aa"
	testAppID    = "00000000-0000-0000-0000-0000000001aa"
)

func pgSessionService(t *testing.T) (*storage.PGSessionService, *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	pool, err := storage.NewPG(ctx, "postgres://trpc:trpc-dev-only@localhost:5432/trpc?sslmode=disable")
	if err != nil {
		t.Skipf("postgres unavailable (%v), skipping integration test", err)
	}
	t.Cleanup(func() { pool.Close() })

	if _, err := pool.Exec(ctx,
		`INSERT INTO tenant (id, name, status) VALUES ($1, 'pg-session-test', 'active') ON CONFLICT (id) DO NOTHING`,
		testTenantID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO agent_app (id, tenant_id, name, agent_type, config, version, status)
		 VALUES ($1, $2, 'pg-session-test', 'llm', '{}', 1, 'published') ON CONFLICT DO NOTHING`,
		testAppID, testTenantID); err != nil {
		t.Fatal(err)
	}
	return storage.NewPGSessionService(pool), pool
}

func testKey(suffix string) session.Key {
	return session.Key{AppName: testAppID, UserID: "u-pg", SessionID: "dm:mock:pg-" + suffix}
}

func textEvent(id, author, content string) *event.Event {
	return &event.Event{
		ID:       id,
		Author:   author,
		Response: &model.Response{Choices: []model.Choice{{Message: model.Message{Role: model.RoleUser, Content: content}}}},
	}
}

func cleanupSession(t *testing.T, pool *pgxpool.Pool, key session.Key) {
	t.Helper()
	ctx := context.Background()
	_, _ = pool.Exec(ctx, `DELETE FROM session_event WHERE session_id IN
		(SELECT id FROM session WHERE app_id=$1 AND session_key=$2)`, key.AppName, key.SessionID)
	_, _ = pool.Exec(ctx, `DELETE FROM session_event_archive WHERE session_id IN
		(SELECT id FROM session WHERE app_id=$1 AND session_key=$2)`, key.AppName, key.SessionID)
	_, _ = pool.Exec(ctx, `DELETE FROM summary WHERE session_id IN
		(SELECT id FROM session WHERE app_id=$1 AND session_key=$2)`, key.AppName, key.SessionID)
	_, _ = pool.Exec(ctx, `DELETE FROM session WHERE app_id=$1 AND session_key=$2`, key.AppName, key.SessionID)
}

func TestPGSessionEventJournalAndSnapshot(t *testing.T) {
	svc, pool := pgSessionService(t)
	ctx := context.Background()
	key := testKey(t.Name())
	cleanupSession(t, pool, key)
	t.Cleanup(func() { cleanupSession(t, pool, key) })

	sess, err := svc.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatal(err)
	}

	// Append two events; the second carries a state delta.
	if err := svc.AppendEvent(ctx, sess, textEvent("e1", "user", "你好")); err != nil {
		t.Fatal(err)
	}
	e2 := textEvent("e2", "assistant", "你好！")
	e2.StateDelta = map[string][]byte{"mood": []byte(`"happy"`)}
	if err := svc.AppendEvent(ctx, sess, e2); err != nil {
		t.Fatal(err)
	}

	// Journal: two events with ordered seqs.
	var seqs []int64
	rows, err := pool.Query(ctx,
		`SELECT event_seq FROM session_event se JOIN session s ON s.id = se.session_id
		 WHERE s.app_id=$1 AND s.session_key=$2 ORDER BY event_seq`, key.AppName, key.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var seq int64
		if err := rows.Scan(&seq); err != nil {
			t.Fatal(err)
		}
		seqs = append(seqs, seq)
	}
	rows.Close()
	if len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 2 {
		t.Fatalf("want event_seq [1 2], got %v", seqs)
	}

	// Snapshot + replay: GetSession returns the state delta and both events.
	got, err := svc.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got.Events) != 2 {
		t.Fatalf("want 2 replayed events, got %+v", got)
	}
	if got.Events[0].ID != "e1" || got.Events[1].ID != "e2" {
		t.Fatalf("events out of order: %s %s", got.Events[0].ID, got.Events[1].ID)
	}
	if string(got.State["mood"]) != `"happy"` {
		t.Fatalf("snapshot missing state delta: %v", got.State)
	}

	// The (session_id, event_seq) unique constraint is the DB-level backstop:
	// a direct duplicate insert must be rejected.
	_, err = pool.Exec(ctx,
		`INSERT INTO session_event (session_id, event_seq, event)
		 SELECT session_id, event_seq, event FROM session_event se
		 JOIN session s ON s.id = se.session_id
		 WHERE s.app_id=$1 AND s.session_key=$2 AND event_seq=1`,
		key.AppName, key.SessionID)
	if err == nil {
		t.Fatal("duplicate (session_id, event_seq) insert must fail")
	}

	// EventNum window: only the last event.
	limited, err := svc.GetSession(ctx, key, session.WithEventNum(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(limited.Events) != 1 || limited.Events[0].ID != "e2" {
		t.Fatalf("WithEventNum(1) mismatch: %+v", limited.Events)
	}
}

func TestPGSessionUpdateStateAndDelete(t *testing.T) {
	svc, pool := pgSessionService(t)
	ctx := context.Background()
	key := testKey(t.Name())
	cleanupSession(t, pool, key)
	t.Cleanup(func() { cleanupSession(t, pool, key) })

	if _, err := svc.CreateSession(ctx, key, session.StateMap{"a": []byte(`1`)}); err != nil {
		t.Fatal(err)
	}
	// Merge one key, delete another (nil value), reject scoped keys.
	if err := svc.UpdateSessionState(ctx, key, session.StateMap{"b": []byte(`2`), "a": nil}); err != nil {
		t.Fatal(err)
	}
	got, err := svc.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got.State["a"]; ok {
		t.Fatal("nil value must delete the key")
	}
	if string(got.State["b"]) != `2` {
		t.Fatalf("merge failed: %v", got.State)
	}
	if err := svc.UpdateSessionState(ctx, key, session.StateMap{session.StateAppPrefix + "x": []byte(`1`)}); err == nil {
		t.Fatal("app: prefixed keys must be rejected")
	}

	if err := svc.DeleteSession(ctx, key); err != nil {
		t.Fatal(err)
	}
	got, err = svc.GetSession(ctx, key)
	if err != nil || got != nil {
		t.Fatalf("deleted session must return nil, got %+v, err %v", got, err)
	}
	var events int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM session_event se JOIN session s ON s.id=se.session_id
		 WHERE s.app_id=$1 AND s.session_key=$2`, key.AppName, key.SessionID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 0 {
		t.Fatalf("events must be deleted with the session, got %d", events)
	}
}

// session_event has a FK to session without ON DELETE CASCADE, so deleting a
// session that already has events only works when the children go first. An
// empty session would not exercise the constraint, hence this case.
func TestPGSessionDeleteWithEvents(t *testing.T) {
	svc, pool := pgSessionService(t)
	ctx := context.Background()
	key := testKey(t.Name())
	cleanupSession(t, pool, key)
	t.Cleanup(func() { cleanupSession(t, pool, key) })

	sess, err := svc.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"e1", "e2", "e3"} {
		if err := svc.AppendEvent(ctx, sess, textEvent(id, "user", fmt.Sprintf("msg %d", i))); err != nil {
			t.Fatal(err)
		}
	}

	if err := svc.DeleteSession(ctx, key); err != nil {
		t.Fatalf("delete session with events: %v", err)
	}
	var left int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM session WHERE app_id=$1 AND session_key=$2`,
		key.AppName, key.SessionID).Scan(&left); err != nil {
		t.Fatal(err)
	}
	if left != 0 {
		t.Fatalf("session row must be gone, got %d", left)
	}
}

func TestPGSessionUnsupportedScopes(t *testing.T) {
	svc, _ := pgSessionService(t)
	ctx := context.Background()
	if err := svc.UpdateAppState(ctx, testAppID, session.StateMap{}); err == nil {
		t.Fatal("app state scope must be unsupported")
	}
	if _, err := svc.ListUserStates(ctx, session.UserKey{AppName: testAppID, UserID: "u"}); err == nil {
		t.Fatal("user state scope must be unsupported")
	}
}

// fakeSummarizer implements the framework summary.SessionSummarizer without
// an LLM: every call returns a distinct text so tests can tell summaries apart.
type fakeSummarizer struct {
	mu    sync.Mutex
	calls int
}

func (f *fakeSummarizer) ShouldSummarize(*session.Session) bool { return true }
func (f *fakeSummarizer) Summarize(context.Context, *session.Session) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return fmt.Sprintf("摘要v%d", f.calls), nil
}
func (f *fakeSummarizer) SetPrompt(string)     {}
func (f *fakeSummarizer) SetModel(model.Model) {}
func (f *fakeSummarizer) Metadata() map[string]any {
	return nil
}

// The summary pipeline: summarization persists text + covered_event_id, and
// GetSession then replays only the uncovered tail with the summary exposed
// under Session.Summaries.
func TestPGSessionSummaryAndIncrementalReplay(t *testing.T) {
	_, pool := pgSessionService(t)
	ctx := context.Background()
	key := testKey(t.Name())
	cleanupSession(t, pool, key)
	t.Cleanup(func() { cleanupSession(t, pool, key) })

	fake := &fakeSummarizer{}
	svcWithSummary := storage.NewPGSessionService(pool, storage.WithSummarizer(fake))
	t.Cleanup(func() { _ = svcWithSummary.Close() })

	sess, err := svcWithSummary.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"s1", "s2", "s3"} {
		if err := svcWithSummary.AppendEvent(ctx, sess, textEvent(id, "user", "消息"+id)); err != nil {
			t.Fatal(err)
		}
	}

	// Sync summarization (the async path is EnqueueSummaryJob, tested below).
	if err := svcWithSummary.CreateSessionSummary(ctx, sess, "assistant", true); err != nil {
		t.Fatal(err)
	}

	// The summary row points at the last covered event.
	var sumText, filterKey, coveredID string
	var coveredSeq int64
	if err := pool.QueryRow(ctx,
		`SELECT s.summary_text, s.filter_key, s.covered_event_id, e.event_seq
		 FROM summary s JOIN session_event e ON e.id = s.covered_event_id
		 WHERE s.session_id = (SELECT id FROM session WHERE app_id=$1 AND session_key=$2)`,
		key.AppName, key.SessionID).Scan(&sumText, &filterKey, &coveredID, &coveredSeq); err != nil {
		t.Fatal(err)
	}
	if sumText != "摘要v1" || filterKey != "assistant" || coveredSeq != 3 {
		t.Fatalf("unexpected summary row: %q %q seq=%d", sumText, filterKey, coveredSeq)
	}

	// Incremental replay: covered events are gone from the event list, and the
	// summary surfaces under Session.Summaries with the prompt-side boundary.
	got, err := svcWithSummary.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Events) != 0 {
		t.Fatalf("all 3 events covered, want 0 replayed, got %d", len(got.Events))
	}
	sum := got.Summaries["assistant"]
	if sum == nil || sum.Summary != "摘要v1" {
		t.Fatalf("summary missing from session: %+v", got.Summaries)
	}
	if sum.Boundary == nil || sum.Boundary.LastEventID != "s3" {
		t.Fatalf("boundary must anchor at the covered event s3: %+v", sum.Boundary)
	}

	// New events land after the cursor: replay yields only them.
	if err := svcWithSummary.AppendEvent(ctx, sess, textEvent("s4", "user", "消息s4")); err != nil {
		t.Fatal(err)
	}
	got, err = svcWithSummary.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Events) != 1 || got.Events[0].ID != "s4" {
		t.Fatalf("want only the uncovered tail, got %+v", got.Events)
	}

	// The async path: EnqueueSummaryJob hands off to the background worker.
	if err := svcWithSummary.AppendEvent(ctx, sess, textEvent("s5", "user", "消息s5")); err != nil {
		t.Fatal(err)
	}
	if err := svcWithSummary.EnqueueSummaryJob(ctx, sess, "assistant", true); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := pool.QueryRow(ctx,
			`SELECT summary_text FROM summary
			 WHERE session_id = (SELECT id FROM session WHERE app_id=$1 AND session_key=$2)`,
			key.AppName, key.SessionID).Scan(&sumText); err != nil {
			t.Fatal(err)
		}
		if sumText == "摘要v2" {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("async summary job did not advance the summary, last text %q", sumText)
}
