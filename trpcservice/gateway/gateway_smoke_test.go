package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/executor"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/messaging"
	"github.com/liuzengh/trpc-agent-service/trpcservice/routing"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

func TestRedis7GatewayWorkerRuntimeSmoke(t *testing.T) {
	rawURL := os.Getenv("PHASE3_REDIS_SMOKE_URL")
	if rawURL == "" {
		t.Skip("PHASE3_REDIS_SMOKE_URL is not set")
	}
	redisURL, err := config.NormalizeRedisURL(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode model request: %v", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"phase3-e2e","object":"chat.completion","created":1,"model":"` + body.Model + `","choices":[{"index":0,"message":{"role":"assistant","content":"phase3 redis e2e ok"},"finish_reason":"stop"}]}`))
	}))
	defer modelServer.Close()

	catalog := gatewayCatalog()
	catalog.ConfigVersions[0].Model.BaseURL = modelServer.URL
	runtimeConfig, err := config.NewCatalogConfig(
		catalog,
		[]byte("01234567890123456789012345678901"),
		config.NewStaticCredentialResolver(map[string]string{"env:MODEL": "mock-key"}),
	)
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := executor.New(runtimeConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	repository, err := tenant.NewPresetRepository(catalog)
	if err != nil {
		t.Fatal(err)
	}
	router, err := routing.New(repository, runtimeConfig.IdentitySecret)
	if err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("phase3-gateway-worker-smoke:%d", time.Now().UnixNano())
	store, err := messaging.NewStore(config.MessagingConfig{
		RedisURL: redisURL, KeyPrefix: prefix,
		LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond,
		InitialBackoff: 20 * time.Millisecond, MaxBackoff: 100 * time.Millisecond, MaxAttempts: 3,
		InboxRetention: time.Minute, ReplyWaitTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	cleanup := newRedisCleanup(t, redisURL, prefix+":reliable-v1:*")
	defer cleanup()

	gatewayService, err := New(router, store, "smoke-gateway")
	if err != nil {
		t.Fatal(err)
	}
	workerService, err := worker.New(store, runtime, "smoke-worker")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gatewayDone := make(chan error, 1)
	workerDone := make(chan error, 1)
	go func() { gatewayDone <- gatewayService.Run(ctx) }()
	go func() { workerDone <- workerService.Run(ctx) }()
	waitForSmokeReady(t, gatewayService, workerService)

	inbound := message.InboundMessage{
		Channel: "demo", BindingID: "binding", MessageID: "redis-e2e-message",
		ExternalUserID: "redis-e2e-user", ConversationID: "redis-e2e-conversation", Text: "hello",
		RequestID: "redis-e2e-request", TraceID: "redis-e2e-trace", ReceivedAt: time.Now().UTC(),
	}
	reply, err := gatewayService.Handle(context.Background(), inbound)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Text != "phase3 redis e2e ok" || reply.TraceID != inbound.TraceID || reply.RequestID != inbound.RequestID {
		t.Fatalf("unexpected reply: %#v", reply)
	}
	duplicate := inbound
	duplicate.RequestID = "redis-e2e-request-duplicate"
	duplicateReply, err := gatewayService.Handle(context.Background(), duplicate)
	if err != nil || duplicateReply.Text != reply.Text || duplicateReply.RequestID != duplicate.RequestID {
		t.Fatalf("cached reply = (%#v, %v)", duplicateReply, err)
	}

	cancel()
	if err := <-gatewayDone; err != nil {
		t.Fatal(err)
	}
	if err := <-workerDone; err != nil {
		t.Fatal(err)
	}
}

func TestRedis7Phase5OutboundRecoverySmoke(t *testing.T) {
	rawURL := os.Getenv("PHASE5_REDIS_SMOKE_URL")
	if rawURL == "" {
		t.Skip("PHASE5_REDIS_SMOKE_URL is not set")
	}
	redisURL, err := config.NormalizeRedisURL(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	prefix := fmt.Sprintf("phase5-outbound-smoke:%d", time.Now().UnixNano())
	store, err := messaging.NewStore(config.MessagingConfig{
		RedisURL: redisURL, KeyPrefix: prefix,
		LeaseDuration: time.Second, HeartbeatInterval: 100 * time.Millisecond,
		InitialBackoff: 20 * time.Millisecond, MaxBackoff: 100 * time.Millisecond, MaxAttempts: 3,
		InboxRetention: time.Minute, ReplyWaitTimeout: time.Second,
		OutboundMaxAttempts: 5, OutboundInitialBackoff: 300 * time.Millisecond, OutboundMaxBackoff: 300 * time.Millisecond,
		OutboundSendTimeout: 100 * time.Millisecond, OutboundClaimIdle: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	defer newRedisCleanup(t, redisURL, prefix+":reliable-v1:*")()
	catalog := gatewayCatalog()
	catalog.ChannelBindings[0] = tenant.ChannelBinding{
		ID: "telegram-binding", Channel: "telegram", ExternalAccountID: "123", CredentialRef: "env:TELEGRAM_TOKEN",
		TenantID: "tenant", AgentAppID: "app", Enabled: true,
	}
	repository, err := tenant.NewPresetRepository(catalog)
	if err != nil {
		t.Fatal(err)
	}
	router, err := routing.New(repository, []byte("01234567890123456789012345678901"))
	if err != nil {
		t.Fatal(err)
	}

	firstAdapter := &recordingAdapter{name: "telegram-binding", failFor: 100}
	first, cancelFirst, firstDone := startGatewayWithAdapter(t, store, router, "phase5-smoke-first", firstAdapter)
	task := submitAndFinishIMTask(t, first, store, "phase5-restart", true)
	waitOutboundAttempts(t, store, task.TaskID, 1)
	cancelFirst()
	_ = first.Close()
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}

	time.Sleep(75 * time.Millisecond)
	secondAdapter := &recordingAdapter{name: "telegram-binding"}
	second, cancelSecond, secondDone := startGatewayWithAdapter(t, store, router, "phase5-smoke-second", secondAdapter)
	state := waitOutboundTerminal(t, store, task.TaskID)
	cancelSecond()
	_ = second.Close()
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
	secondAttempts, _ := secondAdapter.snapshot()
	if state.Status != "succeeded" || state.Attempts != 2 || secondAttempts != 1 {
		t.Fatalf("real Redis recovered outbound state=%#v second attempts=%d", state, secondAttempts)
	}
}

func waitForSmokeReady(t *testing.T, gatewayService *Service, workerService *worker.Worker) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if gatewayService.Ready(context.Background()) == nil && workerService.Ready(context.Background()) == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("Gateway and Worker did not become ready")
}

func newRedisCleanup(t *testing.T, redisURL, pattern string) func() {
	t.Helper()
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatal(err)
	}
	client := redis.NewClient(options)
	return func() {
		defer client.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		var cursor uint64
		for {
			keys, next, err := client.Scan(ctx, cursor, pattern, 100).Result()
			if err != nil {
				t.Logf("scan Gateway/Worker smoke keys: %v", err)
				return
			}
			if len(keys) > 0 {
				if err := client.Del(ctx, keys...).Err(); err != nil {
					t.Logf("delete Gateway/Worker smoke keys: %v", err)
				}
			}
			cursor = next
			if cursor == 0 {
				return
			}
		}
	}
}
