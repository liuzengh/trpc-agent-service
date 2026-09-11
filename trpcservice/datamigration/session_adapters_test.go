package datamigration

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"trpc.group/trpc-go/trpc-agent-go/session"
	redissession "trpc.group/trpc-go/trpc-agent-go/session/redis"
)

func TestRedisSessionSourceScansHashIdxAndSeparatesScopedState(t *testing.T) {
	server := miniredis.RunT(t)
	url := "redis://" + server.Addr() + "/0"
	writer, err := redissession.NewService(
		redissession.WithRedisClientURL(url),
		redissession.WithKeyPrefix("tenant-prefix"),
		redissession.WithCompatMode(redissession.CompatModeLegacy),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	ctx := context.Background()
	key := session.Key{AppName: "assistant", UserID: "user-a", SessionID: "session-a"}
	if _, err := writer.CreateSession(ctx, key, session.StateMap{"local": []byte("value")}); err != nil {
		t.Fatal(err)
	}
	if err := writer.UpdateAppState(ctx, key.AppName, session.StateMap{"theme": []byte("dark")}); err != nil {
		t.Fatal(err)
	}
	if err := writer.UpdateUserState(ctx, session.UserKey{AppName: key.AppName, UserID: key.UserID}, session.StateMap{"locale": []byte("zh")}); err != nil {
		t.Fatal(err)
	}

	source, err := NewRedisSessionSource(url, "tenant-prefix", key.AppName, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	objects, next, done, err := source.Scan(ctx, "", 10)
	if err != nil || done || next != "z:0" || len(objects) != 1 {
		t.Fatalf("scan objects=%d next=%q done=%v err=%v", len(objects), next, done, err)
	}
	object := objects[0]
	if string(object.State["local"]) != "value" || string(object.AppState["theme"]) != "dark" || string(object.UserState["locale"]) != "zh" {
		t.Fatalf("unexpected migration object: %+v", object)
	}
	if _, exists := object.State[session.StateAppPrefix+"theme"]; exists {
		t.Fatal("application projection leaked into session-local state")
	}
	objects, _, done, err = source.Scan(ctx, next, 10)
	if err != nil || !done || len(objects) != 0 {
		t.Fatalf("legacy scan objects=%d done=%v err=%v", len(objects), done, err)
	}
}

func TestSessionCursorRejectsMalformedCheckpoint(t *testing.T) {
	for _, value := range []string{"bad", "x:1", "h:-1", "h:1:2"} {
		if _, _, err := parseSessionScanCursor(value); err == nil {
			t.Fatalf("cursor %q was accepted", value)
		}
	}
}
