package sessionturn

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestSessionServiceDatabaseIdentity(t *testing.T) {
	cfg, err := pgxpool.ParseConfig("postgres://fixture-role:fixture-credential@db.internal:5432/agents?search_path=runtime")
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	store, err := NewPostgres(pool)
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewSessionService(store)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := service.DatabaseIdentity(), store.DatabaseIdentity(); got == "" || got != want {
		t.Fatalf("SessionService database identity = %q, want %q", got, want)
	}
	if (*SessionService)(nil).DatabaseIdentity() != "" {
		t.Fatal("nil SessionService returned a database identity")
	}
}

func TestOpenSessionServiceDoesNotLeakDSN(t *testing.T) {
	const secret = "credential-that-must-not-appear"
	_, err := OpenSessionService(
		context.Background(),
		"postgres://user:"+secret+"@%zz/not-a-database",
	)
	if err == nil {
		t.Fatal("OpenSessionService with invalid DSN unexpectedly succeeded")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("OpenSessionService error leaked DSN credential: %v", err)
	}
}

func TestEventPageUsesNewestOffsetAndChronologicalOrder(t *testing.T) {
	events := []event.Event{{ID: "1"}, {ID: "2"}, {ID: "3"}, {ID: "4"}}
	page := eventPage(events, &session.EventPage{Offset: 1, Limit: 2})
	if len(page) != 2 || page[0].ID != "2" || page[1].ID != "3" {
		t.Fatalf("eventPage = %+v, want IDs [2 3]", page)
	}
	page[0].ID = "changed"
	if events[1].ID != "2" {
		t.Fatal("eventPage returned caller-shared slice storage")
	}
	if got := eventPage(events, &session.EventPage{Offset: len(events), Limit: 2}); len(got) != 0 {
		t.Fatalf("out-of-range page = %+v, want empty", got)
	}
}

func TestScopedStateKeyNormalization(t *testing.T) {
	if _, err := normalizeAppState(session.StateMap{
		session.StateUserPrefix + "wrong": []byte("x"),
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("normalizeAppState(user prefix) = %v, want ErrInvalidRequest", err)
	}
	if _, err := normalizeAppState(session.StateMap{
		session.StateTempPrefix + "wrong": []byte("x"),
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("normalizeAppState(temp prefix) = %v, want ErrInvalidRequest", err)
	}
	if _, err := normalizeUserState(session.StateMap{
		session.StateAppPrefix + "wrong": []byte("x"),
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("normalizeUserState(app prefix) = %v, want ErrInvalidRequest", err)
	}
	if _, err := normalizeUserState(session.StateMap{
		"": []byte("x"),
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("normalizeUserState(empty key) = %v, want ErrInvalidRequest", err)
	}
	if _, err := normalizeAppState(session.StateMap{
		"same":                          []byte("one"),
		session.StateAppPrefix + "same": []byte("two"),
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("normalizeAppState(duplicate normalized key) = %v, want ErrInvalidRequest", err)
	}
	normalized, err := normalizeUserState(session.StateMap{
		session.StateUserPrefix + "theme": []byte("dark"),
	})
	if err != nil || string(normalized["theme"]) != "dark" {
		t.Fatalf("normalizeUserState = %+v, %v", normalized, err)
	}
}

func TestEqualStateDistinguishesNilAndEmptyValues(t *testing.T) {
	if equalState(
		session.StateMap{"key": nil},
		session.StateMap{"key": []byte{}},
	) {
		t.Fatal("equalState treated nil and empty state values as equal")
	}
	if !equalState(
		session.StateMap{"key": []byte{}},
		session.StateMap{"key": []byte{}},
	) {
		t.Fatal("equalState rejected equal non-nil empty values")
	}
}

func TestStagedTurnDeepCopiesEventAndRejectsScopedState(t *testing.T) {
	key := testKey("unit-staged")
	svc := &SessionService{}
	sess := session.NewSession(key.AppName, key.UserID, key.SessionID)
	turn := &stagedTurn{
		service: svc,
		key:     key,
		phase:   turnPhaseActive,
		session: sess,
		overlay: session.StateMap{},
	}
	ctx := context.WithValue(context.Background(), turnContextKey{}, turn)

	evt := validSessionTurnEvent("event-1", model.RoleUser, "hello")
	evt.StateDelta = map[string][]byte{"answer": []byte("42")}
	evt.Extensions = map[string]json.RawMessage{"test/v1": json.RawMessage(`{"ok":true}`)}
	if err := svc.AppendEvent(ctx, sess, evt); err != nil {
		t.Fatal(err)
	}

	evt.ID = "mutated"
	evt.StateDelta["answer"][0] = '9'
	evt.Response.Choices[0].Message.Content = "mutated"
	evt.Extensions["test/v1"][2] = 'X'
	if len(turn.events) != 1 {
		t.Fatalf("buffered events = %d, want 1", len(turn.events))
	}
	stored := turn.events[0]
	if stored.ID != "event-1" || stored.Response.Choices[0].Message.Content != "hello" {
		t.Fatalf("buffered event changed through caller: %+v", stored)
	}
	if got, _ := sess.GetState("answer"); string(got) != "42" {
		t.Fatalf("staged state = %q, want 42", got)
	}

	err := svc.UpdateSessionState(ctx, key, session.StateMap{
		session.StateAppPrefix + "forbidden": []byte("x"),
	})
	if !errors.Is(err, ErrScopedStateUnsupported) {
		t.Fatalf("UpdateSessionState(scoped) = %v, want ErrScopedStateUnsupported", err)
	}
	if !errors.Is(turn.Err(), ErrScopedStateUnsupported) {
		t.Fatalf("Turn.Err = %v, want sticky scoped-state error", turn.Err())
	}
}

func TestStagedTurnChecksExactKeyAndClosesWithoutCancelContext(t *testing.T) {
	key := testKey("unit-scope")
	svc := &SessionService{}
	sess := session.NewSession(key.AppName, key.UserID, key.SessionID)
	turn := &stagedTurn{
		service: svc,
		key:     key,
		phase:   turnPhaseActive,
		session: sess,
		overlay: session.StateMap{},
	}
	ctx := context.WithValue(context.Background(), turnContextKey{}, turn)
	wrong := key
	wrong.SessionID = "another-session"
	if _, err := svc.GetSession(ctx, wrong); !errors.Is(err, ErrTurnScopeMismatch) {
		t.Fatalf("GetSession(wrong key) = %v, want ErrTurnScopeMismatch", err)
	}
	if !errors.Is(turn.Err(), ErrTurnScopeMismatch) {
		t.Fatalf("Turn.Err = %v, want sticky key mismatch", turn.Err())
	}

	cleanTurn := &stagedTurn{
		service: svc,
		key:     key,
		phase:   turnPhaseActive,
		session: session.NewSession(key.AppName, key.UserID, key.SessionID),
		overlay: session.StateMap{},
	}
	cleanCtx := context.WithValue(context.Background(), turnContextKey{}, cleanTurn)
	cleanTurn.Abort()
	err := svc.AppendEvent(
		context.WithoutCancel(cleanCtx),
		cleanTurn.session,
		validSessionTurnEvent("late", model.RoleAssistant, "late"),
	)
	if !errors.Is(err, ErrTurnScopeClosed) {
		t.Fatalf("late AppendEvent = %v, want ErrTurnScopeClosed", err)
	}
}

func TestStagedTurnDetectsDirectOverlayMutation(t *testing.T) {
	key := testKey("unit-overlay")
	sess := session.NewSession(key.AppName, key.UserID, key.SessionID)
	sess.SetState(session.StateAppPrefix+"config", []byte("original"))
	turn := &stagedTurn{
		key:     key,
		phase:   turnPhaseActive,
		session: sess,
		overlay: session.StateMap{session.StateAppPrefix + "config": []byte("original")},
	}
	sess.SetState(session.StateAppPrefix+"config", []byte("changed"))
	turn.mu.Lock()
	_, err := turn.stateForCommitLocked()
	turn.mu.Unlock()
	if !errors.Is(err, ErrScopedStateUnsupported) {
		t.Fatalf("stateForCommitLocked = %v, want ErrScopedStateUnsupported", err)
	}
}

func validSessionTurnEvent(id string, role model.Role, content string) *event.Event {
	return &event.Event{
		ID:           id,
		InvocationID: "invocation-" + id,
		Author:       string(role),
		Timestamp:    time.Now().UTC(),
		Response: &model.Response{Choices: []model.Choice{{
			Message: model.Message{Role: role, Content: content},
		}}},
	}
}
