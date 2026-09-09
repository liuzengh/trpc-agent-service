package storage_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// failingSessionService stands in for a shadow backend whose writes all fail
// (e.g. a PG whose tenant/app fixture is gone): the fanout must log the
// shadow failure and still serve the authoritative primary path.
type failingSessionService struct {
	*sessioninmemory.SessionService
}

func (f *failingSessionService) CreateSession(context.Context, session.Key, session.StateMap, ...session.Option) (*session.Session, error) {
	return nil, errors.New("shadow write failed")
}
func (f *failingSessionService) DeleteSession(context.Context, session.Key, ...session.Option) error {
	return errors.New("shadow write failed")
}
func (f *failingSessionService) UpdateAppState(context.Context, string, session.StateMap) error {
	return errors.New("shadow write failed")
}
func (f *failingSessionService) DeleteAppState(context.Context, string, string) error {
	return errors.New("shadow write failed")
}
func (f *failingSessionService) UpdateUserState(context.Context, session.UserKey, session.StateMap) error {
	return errors.New("shadow write failed")
}
func (f *failingSessionService) DeleteUserState(context.Context, session.UserKey, string) error {
	return errors.New("shadow write failed")
}
func (f *failingSessionService) UpdateSessionState(context.Context, session.Key, session.StateMap) error {
	return errors.New("shadow write failed")
}
func (f *failingSessionService) AppendEvent(context.Context, *session.Session, *event.Event, ...session.Option) error {
	return errors.New("shadow write failed")
}
func (f *failingSessionService) CreateSessionSummary(context.Context, *session.Session, string, bool) error {
	return errors.New("shadow write failed")
}
func (f *failingSessionService) EnqueueSummaryJob(context.Context, *session.Session, string, bool) error {
	return errors.New("shadow write failed")
}

// Full delegation: writes land on both backends, reads come from the primary,
// and the app/user state scopes work when the primary supports them (the PG
// shadow rejects them, which the fanout logs instead of propagating).
func TestFanoutSessionServiceDelegatesAllMethods(t *testing.T) {
	_, pool := pgSessionService(t)
	pgSvc := storage.NewPGSessionService(pool)
	t.Cleanup(func() { _ = pgSvc.Close() })
	memSvc := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = memSvc.Close() })

	fo := &storage.FanoutSessionService{Primary: memSvc, Secondary: pgSvc}
	ctx := context.Background()

	key := session.Key{AppName: testAppID, UserID: "u-pg", SessionID: "dm:mock:fanout-full-" + t.Name()}
	cleanupSession(t, pool, key)
	t.Cleanup(func() { cleanupSession(t, pool, key) })

	// Create: both backends receive the session.
	sess, err := fo.CreateSession(ctx, key, session.StateMap{"init": []byte(`1`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pgSvc.GetSession(ctx, key); err != nil {
		t.Fatalf("shadow backend must receive the created session: %v", err)
	}

	// AppendEvent: the primary carries the event and the shadow gets its own
	// copy (via Clone).
	if err := fo.AppendEvent(ctx, sess, textEvent("fanout-e1", "user", "事件一")); err != nil {
		t.Fatal(err)
	}
	primaryGot, err := fo.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(primaryGot.Events) != 1 || primaryGot.Events[0].ID != "fanout-e1" {
		t.Fatalf("primary must carry the appended event: %+v", primaryGot.Events)
	}
	shadowGot, err := pgSvc.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(shadowGot.Events) != 1 || shadowGot.Events[0].ID != "fanout-e1" {
		t.Fatalf("shadow must receive the appended event: %+v", shadowGot.Events)
	}

	// UpdateSessionState reaches both backends.
	if err := fo.UpdateSessionState(ctx, key, session.StateMap{"mood": []byte(`"happy"`)}); err != nil {
		t.Fatal(err)
	}
	primaryGot, _ = fo.GetSession(ctx, key)
	if string(primaryGot.State["mood"]) != `"happy"` {
		t.Fatalf("primary session state not updated: %v", primaryGot.State)
	}
	shadowGot, _ = pgSvc.GetSession(ctx, key)
	if string(shadowGot.State["mood"]) != `"happy"` {
		t.Fatalf("shadow session state not updated: %v", shadowGot.State)
	}

	// ListSessions reads from the primary only.
	listed, err := fo.ListSessions(ctx, session.UserKey{AppName: testAppID, UserID: "u-pg"})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, s := range listed {
		if s.ID == key.SessionID {
			found = true
		}
	}
	if !found {
		t.Fatalf("ListSessions must come from the primary, got %+v", listed)
	}

	// App-scoped state: the primary (in-memory) accepts it and the read-back
	// goes to the primary; the PG shadow rejects the scope, which the fanout
	// logs instead of failing the write.
	if err := fo.UpdateAppState(ctx, testAppID, session.StateMap{"appkey": []byte("appval")}); err != nil {
		t.Fatalf("app state write must succeed on the primary: %v", err)
	}
	states, err := fo.ListAppStates(ctx, testAppID)
	if err != nil {
		t.Fatal(err)
	}
	if string(states["appkey"]) != "appval" {
		t.Fatalf("app state read-back mismatch: %v", states)
	}
	if err := fo.DeleteAppState(ctx, testAppID, "appkey"); err != nil {
		t.Fatal(err)
	}
	if states, _ = fo.ListAppStates(ctx, testAppID); len(states["appkey"]) != 0 {
		t.Fatalf("app state key must be deleted: %v", states)
	}

	// User-scoped state, same fanout shape.
	userKey := session.UserKey{AppName: testAppID, UserID: "u-pg"}
	if err := fo.UpdateUserState(ctx, userKey, session.StateMap{"userkey": []byte("userval")}); err != nil {
		t.Fatal(err)
	}
	userStates, err := fo.ListUserStates(ctx, userKey)
	if err != nil {
		t.Fatal(err)
	}
	if string(userStates["userkey"]) != "userval" {
		t.Fatalf("user state read-back mismatch: %v", userStates)
	}
	if err := fo.DeleteUserState(ctx, userKey, "userkey"); err != nil {
		t.Fatal(err)
	}
	if userStates, _ = fo.ListUserStates(ctx, userKey); len(userStates["userkey"]) != 0 {
		t.Fatalf("user state key must be deleted: %v", userStates)
	}

	// Summary paths: without a configured summarizer both backends no-op the
	// synchronous and enqueued summarization, and the text read misses.
	if err := fo.CreateSessionSummary(ctx, sess, "", false); err != nil {
		t.Fatalf("summary without summarizer must be a no-op: %v", err)
	}
	if err := fo.EnqueueSummaryJob(ctx, sess, "", false); err != nil {
		t.Fatalf("summary enqueue without summarizer must be a no-op: %v", err)
	}
	if text, ok := fo.GetSessionSummaryText(ctx, sess); ok {
		t.Fatalf("no summary expected, got %q", text)
	}

	// Delete removes the session from both backends.
	if err := fo.DeleteSession(ctx, key); err != nil {
		t.Fatal(err)
	}
	if got, _ := fo.GetSession(ctx, key); got != nil {
		t.Fatal("primary session must be gone after delete")
	}
	if got, _ := pgSvc.GetSession(ctx, key); got != nil {
		t.Fatal("shadow session must be gone after delete")
	}

	if err := fo.Close(); err != nil {
		t.Fatalf("Close must be a no-op returning nil: %v", err)
	}
}

// Primary is the authoritative side: a failing shadow must never fail the
// fanout's own return value, and the summary pipeline still serves reads.
func TestFanoutSessionServiceShadowFailureDoesNotBreakPrimary(t *testing.T) {
	_, pool := pgSessionService(t)
	pgSvc := storage.NewPGSessionService(pool, storage.WithSummarizer(&fakeSummarizer{}))
	t.Cleanup(func() { _ = pgSvc.Close() })
	shadow := &failingSessionService{SessionService: sessioninmemory.NewSessionService()}

	fo := &storage.FanoutSessionService{Primary: pgSvc, Secondary: shadow}
	ctx := context.Background()

	key := testKey("fanout-shadow-" + t.Name())
	cleanupSession(t, pool, key)
	t.Cleanup(func() { cleanupSession(t, pool, key) })

	// Every shadow write below fails; each call must still succeed via the
	// primary (the failures land in the log as the compensation hint).
	sess, err := fo.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatalf("shadow failure on create must not fail the fanout: %v", err)
	}
	if err := fo.AppendEvent(ctx, sess, textEvent("fs-1", "user", "消息一")); err != nil {
		t.Fatalf("shadow failure on append must not fail the fanout: %v", err)
	}
	if err := fo.UpdateSessionState(ctx, key, session.StateMap{"mood": []byte(`"fine"`)}); err != nil {
		t.Fatalf("shadow failure on state update must not fail the fanout: %v", err)
	}

	// Synchronous summarization persists on the primary despite the shadow.
	if err := fo.CreateSessionSummary(ctx, sess, "assistant", true); err != nil {
		t.Fatalf("shadow failure on summary must not fail the fanout: %v", err)
	}
	if text, ok := fo.GetSessionSummaryText(ctx, sess); !ok || text != "摘要v1" {
		t.Fatalf("summary read must come from the primary, got %q ok=%v", text, ok)
	}

	// Async summarization advances the persisted text to the next version.
	if err := fo.EnqueueSummaryJob(ctx, sess, "assistant", true); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	var sumText string
	for {
		if err := pool.QueryRow(ctx,
			`SELECT summary_text FROM summary
			 WHERE session_id = (SELECT id FROM session WHERE app_id=$1 AND session_key=$2)`,
			key.AppName, key.SessionID).Scan(&sumText); err != nil {
			t.Fatal(err)
		}
		if sumText == "摘要v2" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("async summary job did not advance the summary, last text %q", sumText)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// stalledSessionService hangs in AppendEvent until its context is done: a
// shadow backend that stopped answering must not stall the authoritative
// write behind it.
type stalledSessionService struct {
	*sessioninmemory.SessionService
	called chan struct{}
}

func (s *stalledSessionService) AppendEvent(ctx context.Context, _ *session.Session, _ *event.Event, _ ...session.Option) error {
	select {
	case s.called <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}

// The shadow write is best-effort: it must neither delay nor fail the
// authoritative append, and a secondary that hangs is bounded by its own short
// budget instead of the caller's (unbounded) one.
func TestFanoutAppendEventDoesNotWaitOnStalledShadow(t *testing.T) {
	primary := sessioninmemory.NewSessionService()
	t.Cleanup(func() { _ = primary.Close() })
	shadow := &stalledSessionService{
		SessionService: sessioninmemory.NewSessionService(),
		called:         make(chan struct{}, 1),
	}
	fo := &storage.FanoutSessionService{Primary: primary, Secondary: shadow}

	ctx := context.Background()
	key := session.Key{AppName: "a1", UserID: "u1", SessionID: "s1"}
	sess, err := primary.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatal(err)
	}

	// No deadline on the caller's context at all: only the shadow's own
	// budget can end the wait.
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- fo.AppendEvent(ctx, sess, textEvent("e1", "user", "hi")) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a stalled shadow must not fail the authoritative write: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("AppendEvent waited on a stalled shadow backend")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("the shadow write must not hold the message path, took %v", elapsed)
	}
	if len(shadow.called) == 0 {
		t.Fatal("the shadow backend was never called")
	}

	got, err := primary.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Events) != 1 || got.Events[0].ID != "e1" {
		t.Fatalf("the authoritative write must still land: %+v", got.Events)
	}
}
