package storage

import (
	"context"
	"os"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/session"
)

// TestRedisIntegrationRoundTrip runs against a real Redis (e.g. the local
// docker container from docs/spec-storage-redis.md §4):
//
//	REDIS_TEST_URL=redis://127.0.0.1:6379 go test ./trpcservice/storage/ -v
//
// Without the variable the test skips, so machines and CI without Redis stay
// green. It verifies the restart-survival claim on the real server: write
// with one service instance, read back with a fresh one, then clean up.
func TestRedisIntegrationRoundTrip(t *testing.T) {
	url := os.Getenv("REDIS_TEST_URL")
	if url == "" {
		t.Skip("REDIS_TEST_URL not set; skipping real-redis integration test")
	}
	sc := SessionConfig{
		Backend:    "redis",
		RedisURL:   url,
		KeyPrefix:  "itest:",
		SessionTTL: time.Hour,
	}

	svc, err := NewSessionService(sc)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	ctx := context.Background()
	key := session.Key{AppName: "itest", UserID: "u1", SessionID: "itest:webchat:u1"}
	sess, err := svc.CreateSession(ctx, key, session.StateMap{})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	appendTestEvent(t, svc, sess, "itest-ev1", "hello from integration")
	if err := svc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	svc2, err := NewSessionService(sc)
	if err != nil {
		t.Fatalf("second instance: %v", err)
	}
	defer svc2.Close()

	got, err := svc2.GetSession(ctx, key)
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}
	if got == nil || len(got.Events) != 1 || got.Events[0].ID != "itest-ev1" {
		t.Fatalf("session did not survive a service restart on real redis: %+v", got)
	}
	if err := svc2.DeleteSession(ctx, key); err != nil {
		t.Fatalf("cleanup delete: %v", err)
	}
}
