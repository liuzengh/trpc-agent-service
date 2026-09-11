package sessionstore

import (
	"context"
	"errors"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

const (
	testApp     = "trpc-agent-service"
	testUser    = "demo-user"
	testSession = "demo:wecom:alice"
)

func textEvent(id, author, text string) *event.Event {
	return &event.Event{
		Response: &model.Response{
			Choices: []model.Choice{{Message: model.Message{Role: model.RoleAssistant, Content: text}}},
		},
		ID:        id,
		Author:    author,
		Timestamp: time.Now(),
	}
}

func userEvent(id, text string) *event.Event {
	e := textEvent(id, "user", text)
	e.Response.Choices[0].Message.Role = model.RoleUser
	return e
}

func TestNewWorkspaceSeedsFromBaseWithoutMutatingIt(t *testing.T) {
	base := session.NewSession(testApp, testUser, testSession)
	base.SetState("k", []byte("v"))
	baseAppend(base, userEvent("base-1", "older message"))

	w := NewWorkspace(Options{Base: base})

	live, err := w.GetSession(context.Background(), session.Key{AppName: testApp, UserID: testUser, SessionID: testSession})
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if live == nil {
		t.Fatal("GetSession returned nil for a seeded workspace")
	}
	if len(base.Events) != 1 {
		t.Fatalf("base.Events mutated: %d events, want it untouched by the workspace", len(base.Events))
	}
	if got, ok := live.GetState("k"); !ok || string(got) != "v" {
		t.Fatalf("state lost from base: %q ok=%v", got, ok)
	}
}

func baseAppend(sess *session.Session, e *event.Event) {
	sess.EventMu.Lock()
	sess.Events = append(sess.Events, *e)
	sess.EventMu.Unlock()
}

func TestAppendEventKeepsRunnerViewAndWorkspaceViewInStep(t *testing.T) {
	w := NewWorkspace(Options{})
	ctx := context.Background()

	sess, err := w.CreateSession(ctx, session.Key{AppName: testApp, UserID: testUser, SessionID: testSession}, nil)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	userMsg := userEvent("evt-user", "hello")
	if err := w.AppendEvent(ctx, sess, userMsg); err != nil {
		t.Fatalf("AppendEvent user: %v", err)
	}
	if got := len(sess.GetEvents()); got != 1 {
		t.Fatalf("caller session not updated in place: %d events, want 1", got)
	}

	assistantMsg := textEvent("evt-assistant", "assistant", "hi there")
	if err := w.AppendEvent(ctx, sess, assistantMsg); err != nil {
		t.Fatalf("AppendEvent assistant: %v", err)
	}

	reread, err := w.GetSession(ctx, session.Key{AppName: testApp, UserID: testUser, SessionID: testSession})
	if err != nil {
		t.Fatalf("GetSession after appends: %v", err)
	}
	if got := len(reread.GetEvents()); got != 2 {
		t.Fatalf("GetSession does not reflect both appends: %d events, want 2", got)
	}

	prepared, err := w.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(prepared.Events) != 2 {
		t.Fatalf("journal has %d events, want 2", len(prepared.Events))
	}
	if prepared.Events[0].ID != "evt-user" || prepared.Events[1].ID != "evt-assistant" {
		t.Fatalf("event identity rewritten: %q, %q", prepared.Events[0].ID, prepared.Events[1].ID)
	}
	if !prepared.Created {
		t.Fatal("Created must be true for a session established by this attempt")
	}
}

func TestAppendEventAppliesStateDeltaToLiveView(t *testing.T) {
	w := NewWorkspace(Options{})
	ctx := context.Background()
	sess, err := w.CreateSession(ctx, session.Key{AppName: testApp, UserID: testUser, SessionID: testSession}, nil)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	e := textEvent("evt-delta", "assistant", "done")
	e.StateDelta = map[string][]byte{"counter": []byte("3")}
	if err := w.AppendEvent(ctx, sess, e); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}

	got, ok := sess.GetState("counter")
	if !ok || string(got) != "3" {
		t.Fatalf("StateDelta not applied to caller session: %q ok=%v", got, ok)
	}
	reread, err := w.GetSession(ctx, session.Key{AppName: testApp, UserID: testUser, SessionID: testSession})
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if got, ok := reread.GetState("counter"); !ok || string(got) != "3" {
		t.Fatalf("StateDelta not applied to workspace view: %q ok=%v", got, ok)
	}
}

func TestUpdateSessionStateDirectAndPrefixRejection(t *testing.T) {
	w := NewWorkspace(Options{})
	ctx := context.Background()
	key := session.Key{AppName: testApp, UserID: testUser, SessionID: testSession}
	if _, err := w.CreateSession(ctx, key, nil); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	if err := w.UpdateSessionState(ctx, key, session.StateMap{"plain": []byte("x")}); err != nil {
		t.Fatalf("UpdateSessionState: %v", err)
	}
	if err := w.UpdateSessionState(ctx, key, session.StateMap{"temp": []byte("y")}); err != nil {
		t.Fatalf("temp-prefixed keys must be allowed: %v", err)
	}

	for _, reserved := range []string{session.StateAppPrefix + "a", session.StateUserPrefix + "u"} {
		err := w.UpdateSessionState(ctx, key, session.StateMap{reserved: []byte("z")})
		if err == nil {
			t.Fatalf("reserved key %q accepted", reserved)
		}
	}

	sess, err := w.GetSession(ctx, key)
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}
	if v, ok := sess.GetState("plain"); !ok || string(v) != "x" {
		t.Fatalf("direct state update lost: %q ok=%v", v, ok)
	}
}

func TestSharedStateWritesAreRejectedAndBlockCommit(t *testing.T) {
	w := NewWorkspace(Options{})
	ctx := context.Background()
	if _, err := w.CreateSession(ctx, session.Key{AppName: testApp, UserID: testUser, SessionID: testSession}, nil); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	err := w.UpdateAppState(ctx, testApp, session.StateMap{"foo": []byte("bar")})
	if !errors.Is(err, ErrUnsupportedSharedState) {
		t.Fatalf("UpdateAppState error: %v, want ErrUnsupportedSharedState", err)
	}
	if _, snapErr := w.Snapshot(); !errors.Is(snapErr, ErrUnsupportedSharedState) {
		t.Fatalf("Snapshot must surface the sticky failure, got: %v", snapErr)
	}

	// A read of the same shared scope must not error just because this
	// milestone does not back it: the framework calls these unconditionally
	// when building a prompt.
	states, err := w.ListAppStates(ctx, testApp)
	if err != nil {
		t.Fatalf("ListAppStates: %v", err)
	}
	if len(states) != 0 {
		t.Fatalf("ListAppStates not empty: %v", states)
	}
}

func TestSealBeatsALiveContextAndIsNotUndoable(t *testing.T) {
	w := NewWorkspace(Options{})
	ctx := context.Background()
	key := session.Key{AppName: testApp, UserID: testUser, SessionID: testSession}
	sess, err := w.CreateSession(ctx, key, nil)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	w.Seal("lease lost")

	// The framework's post-cancel persistence path calls with a live context,
	// exactly like this one: only the seal, not ctx, can stop it.
	if err := w.AppendEvent(context.Background(), sess, userEvent("evt-late", "too late")); !errors.Is(err, ErrSealed) {
		t.Fatalf("AppendEvent after Seal: %v, want ErrSealed", err)
	}
	if err := w.UpdateSessionState(context.Background(), key, session.StateMap{"x": []byte("y")}); !errors.Is(err, ErrSealed) {
		t.Fatalf("UpdateSessionState after Seal: %v, want ErrSealed", err)
	}
	if _, err := w.Snapshot(); !errors.Is(err, ErrSealed) {
		t.Fatalf("Snapshot after Seal: %v, want ErrSealed", err)
	}
	if !w.Sealed() {
		t.Fatal("Sealed() false after Seal")
	}
	// A second Seal must not overwrite the first reason or "unseal" anything.
	w.Seal("ignored")
	if err := w.AppendEvent(context.Background(), sess, userEvent("evt-late2", "still too late")); !errors.Is(err, ErrSealed) {
		t.Fatalf("AppendEvent after second Seal: %v, want ErrSealed", err)
	}
}

func TestCancelledContextAloneIsNotConfusedWithSeal(t *testing.T) {
	w := NewWorkspace(Options{})
	key := session.Key{AppName: testApp, UserID: testUser, SessionID: testSession}
	sess, err := w.CreateSession(context.Background(), key, nil)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = w.AppendEvent(ctx, sess, userEvent("evt-cancel", "hello"))
	if errors.Is(err, ErrSealed) {
		t.Fatal("a cancelled context must not report as sealed: seal is a different gate")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("AppendEvent with cancelled ctx: %v, want context.Canceled", err)
	}
	if w.Sealed() {
		t.Fatal("a rejected-by-context write must not seal the workspace")
	}
	// A live context after a cancelled one still works: the earlier rejection
	// was the caller's problem, not a sticky failure.
	if err := w.AppendEvent(context.Background(), sess, userEvent("evt-ok", "hello again")); err != nil {
		t.Fatalf("AppendEvent with a live ctx after a cancelled one: %v", err)
	}
}

func TestSnapshotSealsAndFreezesTheJournal(t *testing.T) {
	w := NewWorkspace(Options{})
	ctx := context.Background()
	sess, err := w.CreateSession(ctx, session.Key{AppName: testApp, UserID: testUser, SessionID: testSession}, nil)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := w.AppendEvent(ctx, sess, userEvent("evt-1", "one")); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}

	prepared, err := w.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(prepared.Events) != 1 || prepared.Events[0].ID != "evt-1" {
		t.Fatalf("unexpected journal: %+v", prepared.Events)
	}
	if prepared.State == nil {
		t.Fatal("Snapshot must always carry a state map")
	}
	// Snapshot itself seals: a commit retry must not keep running the attempt.
	if err := w.AppendEvent(ctx, sess, userEvent("evt-2", "two")); !errors.Is(err, ErrSealed) {
		t.Fatalf("AppendEvent after Snapshot: %v, want ErrSealed", err)
	}
	if _, err := w.Snapshot(); !errors.Is(err, ErrSealed) {
		t.Fatalf("second Snapshot: %v, want ErrSealed", err)
	}
}

func TestSummaryIntentsAreRecordedWithoutExecutingSummarization(t *testing.T) {
	base := session.NewSession(testApp, testUser, testSession)
	base.SummariesMu.Lock()
	base.Summaries[session.SummaryFilterKeyAllContents] = &session.Summary{Summary: "from base"}
	base.SummariesMu.Unlock()

	w := NewWorkspace(Options{Base: base})
	ctx := context.Background()
	sess, err := w.GetSession(ctx, session.Key{AppName: testApp, UserID: testUser, SessionID: testSession})
	if err != nil {
		t.Fatalf("GetSession: %v", err)
	}

	if err := w.EnqueueSummaryJob(ctx, sess, "user-messages", true); err != nil {
		t.Fatalf("EnqueueSummaryJob: %v", err)
	}
	if err := w.CreateSessionSummary(ctx, sess, "", false); err != nil {
		t.Fatalf("CreateSessionSummary: %v", err)
	}

	prepared, err := w.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(prepared.Summary) != 2 {
		t.Fatalf("intents recorded: %d, want 2", len(prepared.Summary))
	}
	if prepared.Summary[0].FilterKey != "user-messages" || !prepared.Summary[0].Force {
		t.Fatalf("first intent lost its arguments: %+v", prepared.Summary[0])
	}

	text, ok := w.GetSessionSummaryText(ctx, sess)
	if !ok || text != "from base" {
		t.Fatalf("GetSessionSummaryText must only surface committed summaries, got %q ok=%v", text, ok)
	}
}

func TestDeleteSessionBlocksFurtherReadsAndWritesButKeepsIntent(t *testing.T) {
	w := NewWorkspace(Options{})
	ctx := context.Background()
	key := session.Key{AppName: testApp, UserID: testUser, SessionID: testSession}
	sess, err := w.CreateSession(ctx, key, nil)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := w.AppendEvent(ctx, sess, userEvent("evt-1", "one")); err != nil {
		t.Fatalf("AppendEvent: %v", err)
	}
	if err := w.DeleteSession(ctx, key); err != nil {
		t.Fatalf("DeleteSession: %v", err)
	}

	got, err := w.GetSession(ctx, key)
	if err != nil {
		t.Fatalf("GetSession after delete: %v", err)
	}
	if got != nil {
		t.Fatal("GetSession must report a deleted session as absent")
	}
	if err := w.AppendEvent(ctx, sess, userEvent("evt-2", "two")); !errors.Is(err, ErrSessionNotFound) {
		t.Fatalf("AppendEvent after delete: %v, want ErrSessionNotFound", err)
	}

	prepared, err := w.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if !prepared.Deleted {
		t.Fatal("Deleted intent lost")
	}
	if len(prepared.Events) != 1 || prepared.Events[0].ID != "evt-1" {
		t.Fatalf("journal should keep what happened before the delete, got %+v", prepared.Events)
	}
}

func TestSnapshotCarriesFinalStateAsDeepCopy(t *testing.T) {
	w := NewWorkspace(Options{})
	ctx := context.Background()
	key := session.Key{AppName: testApp, UserID: testUser, SessionID: testSession}
	if _, err := w.CreateSession(ctx, key, session.StateMap{"counter": []byte("1")}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if err := w.UpdateSessionState(ctx, key, session.StateMap{"counter": []byte("2")}); err != nil {
		t.Fatalf("UpdateSessionState: %v", err)
	}

	prepared, err := w.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	buf := prepared.State["counter"]
	buf[0] = 'X'

	// Mutating the snapshot's buffer must not be visible through a later read
	// of the same workspace — Snapshot hands out a copy, not an alias.
	if _, err := w.Snapshot(); !errors.Is(err, ErrSealed) {
		t.Fatalf("second Snapshot: %v, want ErrSealed", err)
	}
}
