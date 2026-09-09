package executor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

func TestRuntimePhase2RealRedisCatalogSmoke(t *testing.T) {
	redisURL := os.Getenv("REDIS_SMOKE_URL")
	if redisURL == "" {
		t.Skip("set REDIS_SMOKE_URL to run the real Redis Phase 2 smoke test")
	}
	var requestsMu sync.Mutex
	var messageCounts []int
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode model request: %v", err)
			return
		}
		messages, _ := request["messages"].([]any)
		requestsMu.Lock()
		messageCounts = append(messageCounts, len(messages))
		requestsMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"phase2-real","object":"chat.completion","created":1,"model":"model-a","choices":[{"index":0,"message":{"role":"assistant","content":"phase2 real redis ok"},"finish_reason":"stop"}]}`))
	}))
	defer modelServer.Close()

	prefix := "trpc-agent-service:phase2-real:" + time.Now().UTC().Format("20060102150405.000000000")
	catalog := multiTenantCatalog(modelServer.URL)
	catalog.StorageProfiles[0].KeyPrefix = prefix
	cfg, err := config.NewCatalogConfig(catalog, []byte("01234567890123456789012345678901"), config.NewStaticCredentialResolver(map[string]string{
		"env:MODEL_A": "model-key-a", "env:MODEL_B": "model-key-b", "env:REDIS_A": redisURL,
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer deleteKeysWithPrefix(t, redisURL, prefix)

	first, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	request := Request{
		Channel: "demo", BindingID: "binding-a", MessageID: "real-1",
		ExternalUserID: "real-user", ConversationID: "real-conversation", Text: "first",
		RequestID: "request-1", TraceID: "trace-1",
	}
	if _, err := first.Handle(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	backend, err := first.backendForBinding(context.Background(), "demo", "binding-a")
	if err != nil {
		t.Fatal(err)
	}
	wantPrefix := prefix + ":tenant:tenant-a:profile:redis-v1:official-v1"
	if got := backend.(*storage.RedisBackend).Prefix(); got != wantPrefix {
		t.Fatalf("Phase 2 Redis prefix = %q, want %q", got, wantPrefix)
	}

	options, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	control := redis.NewClient(options)
	if err := control.Do(context.Background(), "CLIENT", "PAUSE", 500, "ALL").Err(); err != nil {
		control.Close()
		t.Fatalf("pause real Redis: %v", err)
	}
	failedCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	if err := first.Ready(failedCtx); err == nil {
		cancel()
		control.Close()
		t.Fatal("runtime stayed ready while real Redis was paused")
	}
	cancel()
	time.Sleep(600 * time.Millisecond)
	if err := first.Ready(context.Background()); err != nil {
		control.Close()
		t.Fatalf("runtime did not recover after real Redis pause: %v", err)
	}
	if err := control.Close(); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	request.MessageID = "real-2"
	request.RequestID = "request-2"
	request.Text = "second"
	if _, err := second.Handle(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	requestsMu.Lock()
	defer requestsMu.Unlock()
	if len(messageCounts) != 2 || messageCounts[1] <= messageCounts[0] {
		t.Fatalf("expected cross-Runtime Redis history, message counts = %v", messageCounts)
	}
}

func TestRuntimeRealRedisSmoke(t *testing.T) {
	redisURL := os.Getenv("REDIS_SMOKE_URL")
	if redisURL == "" {
		t.Skip("set REDIS_SMOKE_URL to run the real Redis smoke test")
	}
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"smoke","object":"chat.completion","created":1,"model":"mock","choices":[{"index":0,"message":{"role":"assistant","content":"redis smoke ok"},"finish_reason":"stop"}]}`))
	}))
	defer modelServer.Close()

	prefix := "trpc-agent-service:phase1-smoke:" + time.Now().UTC().Format("20060102150405.000000000")
	cfg := testRuntimeConfig(redisURL, modelServer.URL, prefix)
	first, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	defer deleteKeysWithPrefix(t, redisURL, prefix)

	if err := first.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	reply, err := first.Handle(context.Background(), Request{
		BindingID: cfg.BindingID, MessageID: "smoke-message", ExternalUserID: "smoke-user",
		ConversationID: "smoke-conversation", Text: "ping", RequestID: "request", TraceID: "trace",
	})
	if err != nil {
		t.Fatal(err)
	}
	if reply.Text != "redis smoke ok" {
		t.Fatalf("reply = %q", reply.Text)
	}
	firstBackend, err := first.backendForBinding(context.Background(), "demo", cfg.BindingID)
	if err != nil {
		t.Fatal(err)
	}
	memoryKey := memory.UserKey{AppName: cfg.AppName, UserID: "smoke-user"}
	if err := firstBackend.Memory().AddMemory(context.Background(), memoryKey, "real redis memory", []string{"smoke"}); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := second.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	secondBackend, err := second.backendForBinding(context.Background(), "demo", cfg.BindingID)
	if err != nil {
		t.Fatal(err)
	}
	sess, err := secondBackend.Session().GetSession(context.Background(), session.Key{
		AppName:   cfg.AppName,
		UserID:    identity.RunnerUserID(cfg.IdentitySecret, cfg.BindingID, "smoke-user"),
		SessionID: identity.SessionID(cfg.IdentitySecret, cfg.BindingID, "smoke-conversation"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if sess == nil || len(sess.Events) == 0 {
		t.Fatalf("expected session events from real Redis, got %#v", sess)
	}
	memories, err := secondBackend.Memory().SearchMemories(context.Background(), memoryKey, "redis")
	if err != nil {
		t.Fatal(err)
	}
	if len(memories) != 1 {
		t.Fatalf("expected memory from real Redis, got %#v", memories)
	}
}

func TestRuntimeDeepSeekSmoke(t *testing.T) {
	if os.Getenv("DEEPSEEK_SMOKE") != "1" {
		t.Skip("set DEEPSEEK_SMOKE=1 to run the real model smoke test")
	}
	redisURL := os.Getenv("REDIS_SMOKE_URL")
	apiKey := os.Getenv("DEEPSEEK_API_KEY")
	modelName := os.Getenv("MODEL_NAME")
	baseURL := os.Getenv("MODEL_BASE_URL")
	if redisURL == "" || apiKey == "" || modelName == "" || baseURL == "" {
		t.Fatal("REDIS_SMOKE_URL, DEEPSEEK_API_KEY, MODEL_NAME and MODEL_BASE_URL are required")
	}
	prefix := "trpc-agent-service:deepseek-smoke:" + time.Now().UTC().Format("20060102150405.000000000")
	cfg := testRuntimeConfig(redisURL, baseURL, prefix)
	cfg.ModelName = modelName
	cfg.ModelAPIKey = apiKey
	cfg.ModelRequestTimeout = 60 * time.Second
	runtime, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	defer deleteKeysWithPrefix(t, redisURL, prefix)
	reply, err := runtime.Handle(context.Background(), Request{
		BindingID: cfg.BindingID, MessageID: "deepseek-smoke", ExternalUserID: "smoke-user",
		ConversationID: "smoke-conversation", Text: "Reply with exactly: phase1-ok", RequestID: "request", TraceID: "trace",
	})
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(reply.Text) == "" {
		t.Fatal("empty DeepSeek response")
	}
}

func testRuntimeConfig(redisURL, modelBaseURL, prefix string) config.Config {
	return config.Config{
		ModelName: "mock-model", ModelBaseURL: modelBaseURL, ModelAPIKeyEnv: "MOCK_KEY", ModelAPIKey: "test-key",
		ModelRequestTimeout: 5 * time.Second, ModelMaxOutput: 128,
		IdentitySecret: []byte("01234567890123456789012345678901"), RedisURL: redisURL, RedisKeyPrefix: prefix,
		BindingID: "demo-binding", TenantID: "tenant-demo", AgentAppID: "assistant", ConfigVersion: "v1",
		AppName: "tenant/tenant-demo/app/assistant",
	}
}

func deleteKeysWithPrefix(t *testing.T, redisURL, prefix string) {
	t.Helper()
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Errorf("parse redis cleanup URL: %v", err)
		return
	}
	client := redis.NewClient(options)
	defer client.Close()
	ctx := context.Background()
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, prefix+"*", 100).Result()
		if err != nil {
			t.Errorf("scan smoke keys: %v", err)
			return
		}
		if len(keys) > 0 {
			if err := client.Del(ctx, keys...).Err(); err != nil {
				t.Errorf("delete smoke keys: %v", err)
				return
			}
		}
		cursor = next
		if cursor == 0 {
			return
		}
	}
}
