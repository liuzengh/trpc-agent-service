package storage_test

import (
	"context"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// The decorator passes every call through to the wrapped service; the metrics
// it records on the way land on the no-op provider in tests.
func TestMetricsSessionServiceDelegatesAllOps(t *testing.T) {
	ctx := context.Background()
	inner := sessioninmemory.NewSessionService()
	svc := &storage.MetricsSessionService{Backend: "redis", Inner: inner}

	key := session.Key{AppName: "a1", UserID: "u1", SessionID: "s1"}
	if _, err := svc.CreateSession(ctx, key, session.StateMap{"k": []byte("v")}); err != nil {
		t.Fatal(err)
	}
	got, err := svc.GetSession(ctx, key)
	if err != nil || got == nil {
		t.Fatalf("decorated get must reach the inner service: %v", err)
	}
	list, err := svc.ListSessions(ctx, session.UserKey{AppName: "a1", UserID: "u1"})
	if err != nil || len(list) != 1 {
		t.Fatalf("decorated list = %v, %v", list, err)
	}

	userKey := session.UserKey{AppName: "a1", UserID: "u1"}
	if err := svc.UpdateAppState(ctx, "a1", session.StateMap{"ak": []byte("av")}); err != nil {
		t.Fatal(err)
	}
	appState, err := svc.ListAppStates(ctx, "a1")
	if err != nil || string(appState["ak"]) != "av" {
		t.Fatalf("app state not delegated: %v, %v", appState, err)
	}
	if err := svc.DeleteAppState(ctx, "a1", "ak"); err != nil {
		t.Fatal(err)
	}
	if err := svc.UpdateUserState(ctx, userKey, session.StateMap{"uk": []byte("uv")}); err != nil {
		t.Fatal(err)
	}
	userState, err := svc.ListUserStates(ctx, userKey)
	if err != nil || string(userState["uk"]) != "uv" {
		t.Fatalf("user state not delegated: %v, %v", userState, err)
	}
	if err := svc.DeleteUserState(ctx, userKey, "uk"); err != nil {
		t.Fatal(err)
	}
	if err := svc.UpdateSessionState(ctx, key, session.StateMap{"sk": []byte("sv")}); err != nil {
		t.Fatal(err)
	}
	if err := svc.AppendEvent(ctx, got, textEvent("m1", "user", "hi")); err != nil {
		t.Fatal(err)
	}
	if err := svc.CreateSessionSummary(ctx, got, "", true); err != nil {
		t.Fatal(err)
	}
	if err := svc.EnqueueSummaryJob(ctx, got, "", true); err != nil {
		t.Fatal(err)
	}
	// Summary reads carry no error: the result label is always "ok".
	if _, ok := svc.GetSessionSummaryText(ctx, got); ok {
		t.Fatal("no summary expected on a fresh session")
	}

	if err := svc.DeleteSession(ctx, key); err != nil {
		t.Fatal(err)
	}
	if gone, err := inner.GetSession(ctx, key); err != nil || gone != nil {
		t.Fatalf("decorated delete must reach the inner service: %v", err)
	}
	if err := svc.Close(); err != nil {
		t.Fatal(err)
	}
}

// errSessionService fails one op; the decorator must propagate the error
// (recording it with result=error) instead of swallowing or masking it.
type errSessionService struct {
	*sessioninmemory.SessionService
	fail error
}

func (s *errSessionService) AppendEvent(context.Context, *session.Session, *event.Event, ...session.Option) error {
	return s.fail
}

func TestMetricsSessionServicePropagatesErrors(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("backend down")
	svc := &storage.MetricsSessionService{Backend: "postgres", Inner: &errSessionService{
		SessionService: sessioninmemory.NewSessionService(),
		fail:           boom,
	}}
	key := session.Key{AppName: "a1", UserID: "u1", SessionID: "s1"}
	sess, err := svc.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.AppendEvent(ctx, sess, textEvent("m1", "user", "hi")); !errors.Is(err, boom) {
		t.Fatalf("inner error must propagate unchanged, got %v", err)
	}
}

// UnwrapSessionService strips the decorator and passes undecorated services
// through — the migrator's concrete-type assertions depend on it.
func TestUnwrapSessionService(t *testing.T) {
	inner := sessioninmemory.NewSessionService()
	wrapped := &storage.MetricsSessionService{Backend: "redis", Inner: inner}
	if got := storage.UnwrapSessionService(wrapped); got != inner {
		t.Fatalf("unwrap must return the inner service, got %T", got)
	}
	if got := storage.UnwrapSessionService(inner); got != inner {
		t.Fatalf("undecorated service must pass through, got %T", got)
	}
}
