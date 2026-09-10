package redis_test

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	runtimestorage "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage"
	redisstore "github.com/XnLemon/trpc-agent-service/trpcservice/runtime/storage/redis"
)

// TestRedisRuntimeLiveReconnect is opt-in so the default suite remains
// hermetic. It proves committed runtime state survives client recreation
// against an externally managed Redis service.
func TestRedisRuntimeLiveReconnect(t *testing.T) {
	addr := strings.TrimSpace(os.Getenv("REDIS_RUNTIME_TEST_ADDR"))
	if addr == "" {
		t.Skip("REDIS_RUNTIME_TEST_ADDR is not configured")
	}
	db := 0
	if raw := strings.TrimSpace(os.Getenv("REDIS_RUNTIME_TEST_DB")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			t.Fatalf("invalid REDIS_RUNTIME_TEST_DB")
		}
		db = parsed
	}
	config := redisstore.Config{Addr: addr, Password: os.Getenv("REDIS_RUNTIME_TEST_PASSWORD"), DB: db, KeyPrefix: "trpc:test:redis-live"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := redisstore.NewFromConfig(ctx, config)
	if err != nil {
		t.Fatalf("open live redis: %v", err)
	}
	tenantID := "t_redis_live_" + strconv.FormatInt(time.Now().UnixNano(), 10)
	sessionID := "session"
	eventID := "event"
	if _, err := store.CreateSession(ctx, tenantID, sessionID, map[string]any{"live": true}); err != nil {
		_ = store.Close()
		t.Fatalf("create live session: %v", err)
	}
	if _, _, err := store.RecordMessage(ctx, runtimestorage.MessageEventInput{TenantID: tenantID, SessionID: sessionID, BindingID: "binding", ExternalMessageID: "external", EventID: eventID}); err != nil {
		_ = store.Close()
		t.Fatalf("record live event: %v", err)
	}
	if _, err := store.EnqueueReply(ctx, runtimestorage.ReplyOutbox{TenantID: tenantID, ReplyID: "reply", EventID: eventID, SegmentIndex: 0, SegmentCount: 1, Payload: "live"}); err != nil {
		_ = store.Close()
		t.Fatalf("write live reply: %v", err)
	}
	if _, err := store.PutMemory(ctx, runtimestorage.MemoryInput{TenantID: tenantID, MemoryID: "memory", UserID: "user", Content: "live"}); err != nil {
		_ = store.Close()
		t.Fatalf("write live memory: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close live redis: %v", err)
	}

	reopened, err := redisstore.NewFromConfig(ctx, config)
	if err != nil {
		t.Fatalf("reopen live redis: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	if _, err := reopened.GetSession(ctx, tenantID, sessionID); err != nil {
		t.Fatalf("reopened session: %v", err)
	}
	if _, err := reopened.GetMessage(ctx, tenantID, eventID); err != nil {
		t.Fatalf("reopened event: %v", err)
	}
	if _, err := reopened.GetMemory(ctx, tenantID, "memory"); err != nil {
		t.Fatalf("reopened memory: %v", err)
	}
	if _, err := reopened.GetReply(ctx, tenantID, "reply", 0); err != nil {
		t.Fatalf("reopened reply: %v", err)
	}
}
