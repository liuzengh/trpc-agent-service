package sessionturn

import (
	"errors"
	"strings"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestDeriveTurnIDIsStableAndScoped(t *testing.T) {
	t.Parallel()
	key := session.Key{AppName: "app", UserID: "user", SessionID: "session"}
	first, err := DeriveTurnID(key, "inbox-42")
	if err != nil {
		t.Fatal(err)
	}
	second, err := DeriveTurnID(key, "inbox-42")
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("turn IDs differ: %q != %q", first, second)
	}
	if !strings.HasPrefix(first, "turn_") || len(first) != len("turn_")+sha256HexLength {
		t.Fatalf("unexpected derived ID %q", first)
	}

	otherSession := key
	otherSession.SessionID = "session-2"
	third, err := DeriveTurnID(otherSession, "inbox-42")
	if err != nil {
		t.Fatal(err)
	}
	if third == first {
		t.Fatal("different session keys must derive different turn IDs")
	}
	fourth, err := DeriveTurnID(key, "inbox-43")
	if err != nil {
		t.Fatal(err)
	}
	if fourth == first {
		t.Fatal("different idempotency keys must derive different turn IDs")
	}
}

const sha256HexLength = 64

func TestDeriveTurnIDRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		key  session.Key
		opID string
	}{
		{name: "app", key: session.Key{UserID: "u", SessionID: "s"}, opID: "op"},
		{name: "user", key: session.Key{AppName: "a", SessionID: "s"}, opID: "op"},
		{name: "session", key: session.Key{AppName: "a", UserID: "u"}, opID: "op"},
		{name: "operation", key: session.Key{AppName: "a", UserID: "u", SessionID: "s"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DeriveTurnID(tt.key, tt.opID)
			if !errors.Is(err, ErrInvalidRequest) {
				t.Fatalf("error = %v, want ErrInvalidRequest", err)
			}
		})
	}
}

func TestSnapshotTRPCSessionCopiesTopLevelData(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC().Round(time.Microsecond)
	snapshot := Snapshot{
		Key:       session.Key{AppName: "app", UserID: "user", SessionID: "session"},
		State:     session.StateMap{"answer": []byte("42")},
		Events:    []event.Event{{ID: "event-1", Author: "agent"}},
		CreatedAt: now.Add(-time.Minute),
		UpdatedAt: now,
	}

	converted := snapshot.TRPCSession()
	converted.SetState("answer", []byte("changed"))
	converted.Events[0].Author = "changed"
	if got := string(snapshot.State["answer"]); got != "42" {
		t.Fatalf("snapshot state was aliased: %q", got)
	}
	if got := snapshot.Events[0].Author; got != "agent" {
		t.Fatalf("snapshot event slice was aliased: %q", got)
	}
	if converted.AppName != snapshot.Key.AppName || converted.UserID != snapshot.Key.UserID || converted.ID != snapshot.Key.SessionID {
		t.Fatalf("converted key = %s/%s/%s", converted.AppName, converted.UserID, converted.ID)
	}
	if !converted.CreatedAt.Equal(snapshot.CreatedAt) || !converted.UpdatedAt.Equal(snapshot.UpdatedAt) {
		t.Fatalf("converted timestamps = %s/%s", converted.CreatedAt, converted.UpdatedAt)
	}
}

func TestRequestValidationWithoutDatabase(t *testing.T) {
	t.Parallel()
	store := &Postgres{}
	validKey := session.Key{AppName: "app", UserID: "user", SessionID: "session"}

	if _, err := NewPostgres(nil); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("NewPostgres(nil) = %v, want ErrInvalidRequest", err)
	}
	if _, err := store.Begin(t.Context(), BeginRequest{Key: validKey}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Begin(empty turn) = %v, want ErrInvalidRequest", err)
	}
	if _, err := store.Commit(t.Context(), CommitRequest{Handle: Handle{
		Key: validKey, TurnID: "turn", ExpectedVersion: -1, FencingToken: 1,
	}}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Commit(negative version) = %v, want ErrInvalidRequest", err)
	}
	if err := store.Abort(t.Context(), AbortRequest{
		Key: validKey, TurnID: "turn", FencingToken: 0,
	}); !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("Abort(zero fence) = %v, want ErrInvalidRequest", err)
	}
}
