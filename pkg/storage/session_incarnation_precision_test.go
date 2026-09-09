package storage

import (
	"context"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/session"
)

// The PostgreSQL SDK returns its original time.Now value from CreateSession,
// then replaces it with the microsecond-precision SQL column on GetSession.
type databasePrecisionSessionService struct {
	session.Service
	stored  *session.Session
	updates int
}

func (s *databasePrecisionSessionService) CreateSession(_ context.Context, key session.Key, state session.StateMap, _ ...session.Option) (*session.Session, error) {
	created := session.NewSession(key.AppName, key.UserID, key.SessionID,
		session.WithSessionState(state),
		session.WithSessionCreatedAt(time.Date(2026, 9, 7, 0, 0, 0, 123456789, time.UTC)))
	s.stored = created.Clone()
	s.stored.CreatedAt = s.stored.CreatedAt.Truncate(time.Microsecond)
	return created, nil
}

func (s *databasePrecisionSessionService) GetSession(context.Context, session.Key, ...session.Option) (*session.Session, error) {
	if s.stored == nil {
		return nil, nil
	}
	return s.stored.Clone(), nil
}

func (s *databasePrecisionSessionService) UpdateSessionState(_ context.Context, _ session.Key, state session.StateMap) error {
	s.updates++
	for key, value := range state {
		s.stored.SetState(key, value)
	}
	return nil
}

func TestFirstSessionIncarnationSurvivesDatabaseTimestampPrecision(t *testing.T) {
	key := session.Key{AppName: "tsa1:8:tenant-a:support", UserID: "user-a", SessionID: "session-a"}
	inner := &databasePrecisionSessionService{}
	service, err := NewStrictFencedSessionService(inner, &countingFenceAuthorizer{}, "tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	ctx, _ := incarnationLeaseContext(t, key)
	initialState := session.StateMap{"business": []byte("value")}
	created, err := service.CreateSession(ctx, key, initialState)
	if err != nil {
		t.Fatalf("first production Session creation rejected after SQL timestamp normalization: %v", err)
	}
	id, err := SessionIncarnationID(created)
	if err != nil || id == "" || id != SessionIncarnationFromContext(ctx) {
		t.Fatalf("created incarnation=%q context=%q err=%v", id, SessionIncarnationFromContext(ctx), err)
	}
	if _, mutated := initialState[SessionIncarnationStateKey]; mutated || inner.updates != 0 {
		t.Fatal("incarnation creation changed caller state or required a second write")
	}
	read, err := service.GetSession(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	storedID, err := SessionIncarnationID(read)
	if err != nil || storedID != id || string(read.State["business"]) != "value" {
		t.Fatalf("persisted incarnation=%q expected=%q state=%v err=%v", storedID, id, read.State, err)
	}
}
