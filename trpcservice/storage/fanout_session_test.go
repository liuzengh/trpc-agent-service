package storage_test

import (
	"context"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/session"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

// Fanout writes land on both backends while reads come from the primary only.
func TestFanoutSessionService(t *testing.T) {
	ctx := context.Background()
	primary := sessioninmemory.NewSessionService()
	secondary := sessioninmemory.NewSessionService()
	fo := &storage.FanoutSessionService{Primary: primary, Secondary: secondary}

	key := session.Key{AppName: "a1", UserID: "u1", SessionID: "s1"}
	if _, err := fo.CreateSession(ctx, key, session.StateMap{}); err != nil {
		t.Fatal(err)
	}
	// Both backends got the session.
	for name, svc := range map[string]session.Service{"primary": primary, "secondary": secondary} {
		got, err := svc.GetSession(ctx, key)
		if err != nil || got == nil {
			t.Fatalf("%s backend missing the session: %v", name, err)
		}
	}
	// Reads come from the primary: write directly to secondary and confirm the
	// fanout read does NOT see it.
	if _, err := secondary.CreateSession(ctx,
		session.Key{AppName: "a1", UserID: "u1", SessionID: "s2"}, session.StateMap{}); err != nil {
		t.Fatal(err)
	}
	got, err := fo.GetSession(ctx, session.Key{AppName: "a1", UserID: "u1", SessionID: "s2"})
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatal("fanout reads must not see secondary-only sessions")
	}
}
