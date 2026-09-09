//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/sha1"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redisv8 "github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/channel"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/gateway"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/health"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/pipeline"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/reliable"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/storage"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/telemetry"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/tenant"
	"trpc.group/trpc-go/trpc-agent-go/enterprise/pkg/worker"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

const (
	wecomE2ECorp       = "ww0123456789abcdef"
	wecomE2EUser       = "employee-42"
	wecomE2EPrompt     = "Please remember that I prefer email receipts."
	wecomE2EMemory     = "The user prefers email receipts."
	wecomE2EReply      = "Your receipt preference has been recorded."
	wecomE2EModelKey   = "local-e2e-model-key-never-use-live"
	wecomE2ECorpSecret = "local-e2e-corp-secret-never-use-live"
)

// This suite exercises production protocol, queue, Worker and Runner code with
// real PostgreSQL/Redis in both Session/Memory backend combinations. Only the
// external OpenAI and WeCom HTTP APIs are local
// protocol fixtures. LocalClient deliberately excludes the Worker HTTP process
// boundary, execution-record admission/fencing and real IM account acceptance.
func TestWeComEncryptedCallbackRunnerDeliveryE2E(t *testing.T) {
	// These tests deliberately run sequentially: each owns the process-wide
	// tracing provider and HTTP fixture transport for the duration of its run.
	for _, backends := range []wecomE2EBackends{{"redis", "postgres"}, {"postgres", "redis"}} {
		t.Run(backends.session+"_session_"+backends.memory+"_memory", func(t *testing.T) {
			for _, scenario := range []string{"success", "expired_token_recovers_without_reexecuting_runner", "active_context_cancellation_drains"} {
				t.Run(scenario, func(t *testing.T) { runWeComCallbackE2E(t, backends, scenario) })
			}
		})
	}
}

type wecomE2EBackends struct{ session, memory string }

func runWeComCallbackE2E(t *testing.T, backends wecomE2EBackends, scenario string) {
	t.Helper()
	expireFirstToken := scenario == "expired_token_recovers_without_reexecuting_runner"
	cancelDuringModel := scenario == "active_context_cancellation_drains"
	db := openDatabase(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	redisURL := os.Getenv("TEST_REDIS_URL")
	if redisURL == "" {
		t.Fatal("TEST_REDIS_URL is required for the WeCom integration suite")
	}
	opts, err := redisv8.ParseURL(redisURL)
	if err != nil {
		t.Fatal("TEST_REDIS_URL is invalid")
	}
	rdb := redisv8.NewClient(opts)
	t.Cleanup(func() { _ = rdb.Close() })
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatal("WeCom integration Redis is unavailable")
	}

	recorder := tracetest.NewSpanRecorder()
	tracing := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(recorder))
	previousTracing, previousPropagation := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(tracing)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		_ = tracing.Shutdown(context.Background())
		otel.SetTracerProvider(previousTracing)
		otel.SetTextMapPropagator(previousPropagation)
	})

	provider := &wecomE2EProvider{
		expireFirstToken: expireFirstToken, blockModel: cancelDuringModel,
		errors: make(chan string, 16), modelStarted: make(chan struct{}),
		modelCancelled: make(chan struct{}), stop: make(chan struct{}),
	}
	providerHTTP := httptest.NewServer(provider)
	t.Cleanup(providerHTTP.Close)
	t.Cleanup(func() { close(provider.stop) })
	localTransport := &http.Transport{Proxy: nil}
	t.Cleanup(localTransport.CloseIdleConnections)
	endpoint, err := url.Parse(providerHTTP.URL)
	if err != nil {
		t.Fatal(err)
	}
	// Both production clients keep their normal provider URLs and validation.
	// The test transport routes only these two hosts to loopback and refuses all
	// other egress, so configured test credentials can never reach live APIs.
	previousTransport := http.DefaultTransport
	http.DefaultTransport = wecomE2ETransport{target: endpoint, transport: localTransport}
	t.Cleanup(func() { http.DefaultTransport = previousTransport })
	callbackClient := &http.Client{Transport: localTransport, Timeout: 5 * time.Second}

	manifest := `[
		{"id":"wecom-e2e-redis","backend":"redis","connectionEnv":"TEST_REDIS_URL","allowInsecure":true},
		{"id":"wecom-e2e-postgres","backend":"postgres","connectionEnv":"TEST_DATABASE_URL","allowInsecure":true}
	]`
	profiles, err := storage.LoadBackendProfiles(manifest, os.LookupEnv)
	if err != nil {
		t.Fatal("load WeCom integration backend profile")
	}
	validator, err := storage.LoadBackendProfileValidator(manifest)
	if err != nil {
		t.Fatal(err)
	}
	repo, err := tenant.NewSQLRepository("postgres", os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal("open WeCom integration tenant repository")
	}
	tenants := tenant.NewService(repo, "local-e2e-encryption-master-key", tenant.WithStorageConfigValidator(
		func(_ context.Context, tenantID string, config tenant.StorageConfig) error {
			return validator.ValidateTenantStorage(tenantID, config)
		}))
	t.Cleanup(func() { _ = tenants.Close() })
	aesKey := []byte("0123456789abcdef0123456789abcdef")
	binding := tenant.ChannelBinding{
		Type: "wework", AccountID: "wecom-e2e-" + uuid.NewString(), AgentApp: "support", AppID: "1000002",
		Token: "local-e2e-callback-token", Secret: wecomE2ECorpSecret,
		EncodingAESKey: strings.TrimSuffix(base64.StdEncoding.EncodeToString(aesKey), "="),
		WebhookKey:     uuid.NewString(), Config: map[string]string{"corp_id": wecomE2ECorp},
		AccessPolicy: tenant.ChannelAccessPolicy{AllowDirectMessages: true, AllowedUsers: []string{wecomE2EUser}},
	}
	tn, err := tenants.CreateTenant(tenant.ContextWithAuditActor(ctx, "wecom-e2e"), "WeCom callback integration", tenant.TenantConfig{
		Agents:     []tenant.AgentConfig{{Name: "support", Type: "llm", DefaultModel: "gpt-4o-mini", MaxLLMCalls: 2, Tools: []string{memory.AddToolName}}},
		Models:     []tenant.ModelConfig{{Provider: "openai", ModelName: "gpt-4o-mini", APIKey: wecomE2EModelKey, MaxTokens: 256}},
		ToolPolicy: tenant.ToolPolicy{Mode: "whitelist", Allowed: []string{memory.AddToolName}}, Channels: []tenant.ChannelBinding{binding},
		Storage: tenant.StorageConfig{SessionBackend: backends.session, SessionProfile: "wecom-e2e-" + backends.session, MemoryBackend: backends.memory, MemoryProfile: "wecom-e2e-" + backends.memory},
	})
	if err != nil {
		t.Fatalf("create tenant through production service: %v", err)
	}
	t.Cleanup(func() { cleanupWeComE2E(t, db, tn.ID) })

	adapter := storage.NewMultiTenantStorageAdapterImplWithOptions(storage.StorageCacheOptions{BackendProfiles: profiles})
	t.Cleanup(func() { _ = adapter.Close() })
	collector := telemetry.NewCollectorWithAuditSink(telemetry.NewSQLAuditWriter(db))
	executionTimeout := 5 * time.Second
	if cancelDuringModel {
		// Longer than the shutdown bound: a passing cancellation test cannot
		// accidentally rely on the independent Worker timeout to end the call.
		executionTimeout = 30 * time.Second
	}
	agentWorker, err := worker.NewWorkerWithOptions(tn, adapter, rdb, worker.Options{Collector: collector, ExecutionTimeout: executionTimeout})
	if err != nil {
		t.Fatalf("construct real Worker and Runner: %v", err)
	}
	t.Cleanup(func() { _ = agentWorker.Close() })
	store := reliable.NewPostgresStore(db)
	registry := channel.NewAdapterRegistry()
	registry.Register(channel.ChannelTypeWeWork, channel.NewWeWorkAdapter())
	ingress := gateway.NewDurableServer(tenants, registry, rdb, health.New(health.WithRedis(rdb)), store, collector)
	callback := httptest.NewServer(telemetry.PublicHTTPMiddleware("gateway.wecom.callback", http.HandlerFunc(ingress.HandleWebhook)))
	t.Cleanup(callback.Close)

	callbackURL, body := wecomE2ECallback(t, callback.URL, binding, aesKey, wecomE2ECorp, wecomE2EUser, "provider-message-1")
	badSignature, _ := url.Parse(callbackURL)
	query := badSignature.Query()
	query.Set("msg_signature", strings.Repeat("0", 40))
	badSignature.RawQuery = query.Encode()
	if status := wecomE2EPost(t, callbackClient, badSignature.String(), body); status != http.StatusUnauthorized {
		t.Fatalf("invalid signature status=%d, want 401", status)
	}
	wrongReceiverURL, wrongReceiverBody := wecomE2ECallback(t, callback.URL, binding, aesKey, "wwfedcba9876543210", wecomE2EUser, "wrong-receiver")
	if status := wecomE2EPost(t, callbackClient, wrongReceiverURL, wrongReceiverBody); status != http.StatusBadRequest {
		t.Fatalf("wrong receiver status=%d, want 400", status)
	}
	deniedURL, deniedBody := wecomE2ECallback(t, callback.URL, binding, aesKey, wecomE2ECorp, "unauthorized-employee", "denied-user")
	if status := wecomE2EPost(t, callbackClient, deniedURL, deniedBody); status != http.StatusOK {
		t.Fatalf("denied identity status=%d, want acknowledgement 200", status)
	}
	assertWeComE2ECount(t, db, `SELECT count(*) FROM inbox_messages WHERE tenant_id=$1`, tn.ID, 0)

	var duplicates sync.WaitGroup
	statuses := make(chan int, 6)
	for attempt := 0; attempt < cap(statuses); attempt++ {
		duplicates.Add(1)
		go func() {
			defer duplicates.Done()
			statuses <- wecomE2EPost(t, callbackClient, callbackURL, body)
		}()
	}
	duplicates.Wait()
	close(statuses)
	for status := range statuses {
		if status != http.StatusOK {
			t.Fatalf("duplicate callback status=%d, want 200", status)
		}
	}
	assertWeComE2ECount(t, db, `SELECT count(*) FROM inbox_messages WHERE tenant_id=$1`, tn.ID, 1)
	var sessionID, ownerID, traceParent string
	if err := db.QueryRowContext(ctx, `SELECT session_id,session_owner_id,trace_parent FROM inbox_messages WHERE tenant_id=$1`, tn.ID).Scan(&sessionID, &ownerID, &traceParent); err != nil {
		t.Fatal(err)
	}
	traceParts := strings.Split(traceParent, "-")
	if len(traceParts) != 4 || len(traceParts[1]) != 32 || traceParts[1] == strings.Repeat("0", 32) {
		t.Fatal("Gateway did not persist a valid trace carrier")
	}
	traceID := traceParts[1]
	provider.traceID.Store(traceID)
	appName, err := storage.TenantScopedAppName(tn, "support")
	if err != nil {
		t.Fatal(err)
	}
	memoryActor, err := channel.MemoryActorID(tn.ID, binding.Type, binding.AccountID, wecomE2EUser)
	if err != nil {
		t.Fatal(err)
	}
	memoryKey := memory.UserKey{AppName: appName, UserID: memoryActor}
	defer func() {
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = adapter.DeleteSession(cleanup, tn, session.Key{AppName: "support", UserID: ownerID, SessionID: sessionID})
		memories, err := adapter.MemoryService(cleanup, tn)
		if err == nil {
			_ = memories.ClearMemories(cleanup, memoryKey)
		}
	}()

	invocationCancels := make(chan context.CancelFunc, 1)
	var workerClient worker.Client = worker.NewLocalClient(agentWorker)
	if cancelDuringModel {
		workerClient = &wecomE2EInterruptibleClient{Client: workerClient, cancels: invocationCancels}
	}
	consumer, err := pipeline.NewConsumer(store, tenants, workerClient, pipeline.ConsumerConfig{
		Owner: "wecom-e2e-consumer", Concurrency: 2, PollInterval: 5 * time.Millisecond,
		ProcessTimeout: 45 * time.Second, LeaseDuration: 60 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := pipeline.NewDelivery(store, tenants, registry, pipeline.DeliveryConfig{
		Owner: "wecom-e2e-delivery", Concurrency: 2, PollInterval: 5 * time.Millisecond,
		DeliveryTimeout: 3 * time.Second, LeaseDuration: 10 * time.Second, RetryBase: 10 * time.Millisecond, RetryMaximum: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 2)
	go func() { done <- consumer.Run(runCtx) }()
	go func() { done <- delivery.Run(runCtx) }()
	var drainOnce sync.Once
	drain := func() {
		drainOnce.Do(func() {
			stop()
			for i := 0; i < 2; i++ {
				select {
				case err := <-done:
					if err != nil {
						t.Errorf("pipeline shutdown: %v", err)
					}
				case <-time.After(15 * time.Second):
					t.Error("pipeline goroutines did not drain after cancellation")
				}
			}
		})
	}
	defer drain()
	if cancelDuringModel {
		select {
		case <-provider.modelStarted:
		case message := <-provider.errors:
			t.Fatal(message)
		case <-ctx.Done():
			t.Fatal("Runner did not enter the model fixture before its deadline")
		}
		// Intake cancellation stops new claims, but intentionally drains the
		// claimed invocation. Independently cancel that invocation at the client
		// boundary only after the real Runner has entered the provider request.
		stop()
		cancelInvocation := <-invocationCancels
		cancelInvocation()
		drain()
		select {
		case <-provider.modelCancelled:
		case <-time.After(time.Second):
			t.Fatal("Worker context cancellation did not cancel the active model request")
		}
		if provider.modelCalls.Load() != 1 || provider.sendAttempts.Load() != 0 || provider.tokenCalls.Load() != 0 {
			t.Fatal("cancelled execution retried the model or attempted IM delivery")
		}
		assertWeComE2ECount(t, db, `SELECT count(*) FROM outbox_messages WHERE tenant_id=$1`, tn.ID, 0)
		assertWeComE2ECount(t, db, `SELECT count(*) FROM inbox_messages WHERE tenant_id=$1 AND attempt_count=1 AND status<>'COMPLETED'`, tn.ID, 1)
		assertWeComE2ECount(t, db, `SELECT count(*) FROM audit_logs WHERE tenant_id=$1 AND tool_name='memory_add'`, tn.ID, 0)
		memories, err := adapter.MemoryService(ctx, tn)
		if err != nil {
			t.Fatal(err)
		}
		entries, err := memories.ReadMemories(ctx, memoryKey, 10)
		if err != nil || len(entries) != 0 {
			t.Fatalf("cancelled model invocation left personal Memory: count=%d err=%v", len(entries), err)
		}
		t.Logf("active Runner context cancellation ended model HTTP and drained pipeline goroutines: Session=%s Memory=%s", backends.session, backends.memory)
		return
	}
	waitWeComE2EDelivery(t, ctx, db, tn.ID, provider.errors)
	if status := wecomE2EPost(t, callbackClient, callbackURL, body); status != http.StatusOK {
		t.Fatalf("completed-message redelivery status=%d, want 200", status)
	}
	drain()
	assertWeComE2ECount(t, db, `SELECT count(*) FROM inbox_messages WHERE tenant_id=$1 AND status='COMPLETED' AND attempt_count=1`, tn.ID, 1)
	assertWeComE2ECount(t, db, `SELECT count(*) FROM outbox_messages WHERE tenant_id=$1 AND status='REPLIED'`, tn.ID, 1)
	assertWeComE2ECount(t, db, `SELECT count(*) FROM inbox_messages WHERE tenant_id=$1`, tn.ID, 1)
	wantAttempts := int64(1)
	if expireFirstToken {
		wantAttempts = 2
	}
	if provider.modelCalls.Load() != 2 || provider.sendAttempts.Load() != wantAttempts || provider.tokenCalls.Load() != wantAttempts || provider.accepted.Load() != 1 {
		t.Fatalf("model=%d sends=%d tokens=%d accepted=%d, want 2/%d/%d/1", provider.modelCalls.Load(), provider.sendAttempts.Load(), provider.tokenCalls.Load(), provider.accepted.Load(), wantAttempts, wantAttempts)
	}
	var outboxTrace string
	var outboxAttempts int64
	if err := db.QueryRowContext(ctx, `SELECT trace_parent,attempt_count FROM outbox_messages WHERE tenant_id=$1`, tn.ID).Scan(&outboxTrace, &outboxAttempts); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(outboxTrace, "-"+traceID+"-") || outboxAttempts != wantAttempts {
		t.Fatalf("Outbox trace/attempts mismatch: attempts=%d", outboxAttempts)
	}

	observer := storage.NewMultiTenantStorageAdapterImplWithOptions(storage.StorageCacheOptions{BackendProfiles: profiles})
	t.Cleanup(func() { _ = observer.Close() })
	sess, err := observer.GetSession(ctx, tn, session.Key{AppName: "support", UserID: ownerID, SessionID: sessionID})
	if err != nil {
		t.Fatalf("read shared Session after Runner: %v", err)
	}
	userEvents, toolEvents, finalEvents := 0, 0, 0
	for _, item := range sess.Events {
		if item.Response == nil {
			continue
		}
		for _, choice := range item.Response.Choices {
			switch choice.Message.Role {
			case model.RoleUser:
				if choice.Message.Content == wecomE2EPrompt {
					userEvents++
				}
			case model.RoleTool:
				toolEvents++
			case model.RoleAssistant:
				if choice.Message.Content == wecomE2EReply {
					finalEvents++
				}
			}
		}
	}
	if userEvents != 1 || toolEvents != 1 || finalEvents != 1 {
		t.Fatalf("Session user/tool/final events=%d/%d/%d, want 1/1/1", userEvents, toolEvents, finalEvents)
	}
	memories, err := observer.MemoryService(ctx, tn)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := memories.ReadMemories(ctx, memoryKey, 10)
	if err != nil || len(entries) != 1 || entries[0].Memory == nil || entries[0].Memory.Memory != wecomE2EMemory {
		t.Fatalf("governed memory_add did not persist exactly one expected memory: count=%d err=%v", len(entries), err)
	}
	otherAccountActor, err := channel.MemoryActorID(tn.ID, binding.Type, "another-account", wecomE2EUser)
	if err != nil {
		t.Fatal(err)
	}
	for _, otherActor := range []string{wecomE2EUser, otherAccountActor} {
		unrelated, err := memories.ReadMemories(ctx, memory.UserKey{AppName: appName, UserID: otherActor}, 10)
		if err != nil || len(unrelated) != 0 {
			t.Fatalf("personal Memory leaked outside the authenticated channel-account scope: count=%d err=%v", len(unrelated), err)
		}
	}
	assertWeComE2ECount(t, db, `SELECT count(*) FROM audit_logs WHERE tenant_id=$1 AND tool_name='memory_add' AND trace_id=$2`, tn.ID, 2, traceID)
	for _, decision := range []string{"tool_allowed", "tool_succeeded"} {
		assertWeComE2ECount(t, db, `SELECT count(*) FROM audit_logs WHERE tenant_id=$1 AND tool_name='memory_add' AND trace_id=$2 AND decision=$3`, tn.ID, 1, traceID, decision)
	}
	seen := make(map[string]bool)
	for _, span := range recorder.Ended() {
		if span.SpanContext().TraceID().String() == traceID {
			seen[span.Name()] = true
		}
	}
	for _, name := range []string{"gateway.wecom.callback", "inbox.process", "worker.process", "runner.run", "tool.invoke", "outbox.deliver"} {
		if !seen[name] {
			t.Errorf("trace did not include %s", name)
		}
	}
	select {
	case message := <-provider.errors:
		t.Fatal(message)
	default:
	}
	t.Logf("local protocol E2E: encrypted callback, PostgreSQL Inbox/Outbox, real Runner memory_add, Session=%s Memory=%s, delivery attempts=%d, trace=%s", backends.session, backends.memory, wantAttempts, traceID)
}

type wecomE2EInterruptibleClient struct {
	worker.Client
	cancels chan context.CancelFunc
}

func (client *wecomE2EInterruptibleClient) ProcessMessage(ctx context.Context, request *worker.Request) (*worker.Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	select {
	case client.cancels <- cancel:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return client.Client.ProcessMessage(ctx, request)
}

type wecomE2ETransport struct {
	target    *url.URL
	transport http.RoundTripper
}

func (transport wecomE2ETransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Scheme != "https" || (request.URL.Host != "api.openai.com" && request.URL.Host != "qyapi.weixin.qq.com") {
		return nil, fmt.Errorf("WeCom E2E refused non-fixture HTTP egress")
	}
	local := request.Clone(request.Context())
	local.URL.Scheme, local.URL.Host = transport.target.Scheme, transport.target.Host
	local.Host = request.URL.Host
	return transport.transport.RoundTrip(local)
}

type wecomE2EProvider struct {
	expireFirstToken bool
	blockModel       bool
	traceID          atomic.Value
	deliveryKey      atomic.Value
	modelCalls       atomic.Int64
	tokenCalls       atomic.Int64
	sendAttempts     atomic.Int64
	accepted         atomic.Int64
	errors           chan string
	modelStarted     chan struct{}
	modelCancelled   chan struct{}
	stop             chan struct{}
}

func (p *wecomE2EProvider) reject(w http.ResponseWriter, message string) {
	select {
	case p.errors <- message:
	default:
	}
	http.Error(w, "fixture protocol mismatch", http.StatusBadRequest)
}

func (p *wecomE2EProvider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Host == "api.openai.com" && r.URL.Path == "/v1/chat/completions" {
		p.serveModel(w, r)
		return
	}
	traceID, _ := p.traceID.Load().(string)
	if r.Host != "qyapi.weixin.qq.com" || traceID == "" || !strings.Contains(r.Header.Get("traceparent"), "-"+traceID+"-") {
		p.reject(w, "WeCom provider request lost trusted host or trace")
		return
	}
	switch r.URL.Path {
	case "/cgi-bin/gettoken":
		if r.Method != http.MethodGet || r.URL.Query().Get("corpid") != wecomE2ECorp || r.URL.Query().Get("corpsecret") != wecomE2ECorpSecret {
			p.reject(w, "WeCom token request did not use the selected binding")
			return
		}
		call := p.tokenCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"errcode": 0, "access_token": fmt.Sprintf("local-token-%d", call), "expires_in": 7200})
	case "/cgi-bin/message/send":
		var body struct {
			ToUser  string `json:"touser"`
			MsgType string `json:"msgtype"`
			AgentID int    `json:"agentid"`
			Text    struct {
				Content string `json:"content"`
			} `json:"text"`
		}
		if r.Method != http.MethodPost || json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&body) != nil || body.ToUser != wecomE2EUser || body.AgentID != 1000002 || body.MsgType != "text" || body.Text.Content != wecomE2EReply || r.Header.Get(channel.DeliveryIdempotencyKeyHeader) == "" {
			p.reject(w, "WeCom send request lost reply content, binding, or delivery identity")
			return
		}
		attempt := p.sendAttempts.Add(1)
		key := r.Header.Get(channel.DeliveryIdempotencyKeyHeader)
		if attempt == 1 {
			p.deliveryKey.Store(key)
		} else if original, _ := p.deliveryKey.Load().(string); key != original {
			p.reject(w, "WeCom retry changed the durable delivery identity")
			return
		}
		if p.expireFirstToken && attempt == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{"errcode": 42001, "errmsg": "access_token expired"})
			return
		}
		if r.URL.Query().Get("access_token") != fmt.Sprintf("local-token-%d", attempt) {
			p.reject(w, "WeCom retry did not refresh the expired token")
			return
		}
		p.accepted.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"errcode": 0, "errmsg": "ok", "msgid": "local-receipt-1"})
	default:
		p.reject(w, "unexpected WeCom protocol endpoint")
	}
}

func (p *wecomE2EProvider) serveModel(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Stream   bool `json:"stream"`
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
		Tools []struct {
			Function struct {
				Name string `json:"name"`
			} `json:"function"`
		} `json:"tools"`
	}
	if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer "+wecomE2EModelKey || json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&request) != nil {
		p.reject(w, "model fixture received invalid OpenAI request")
		return
	}
	if len(request.Tools) != 1 || request.Tools[0].Function.Name != memory.AddToolName {
		p.reject(w, "Runner did not expose exactly the tenant-allowed memory tool")
		return
	}
	call := p.modelCalls.Add(1)
	if p.blockModel {
		if call != 1 {
			p.reject(w, "cancelled execution reexecuted the model")
			return
		}
		close(p.modelStarted)
		select {
		case <-r.Context().Done():
			close(p.modelCancelled)
		case <-p.stop:
		}
		return
	}
	message := map[string]any{"role": "assistant"}
	finish := "stop"
	if call == 1 {
		found := false
		for _, candidate := range request.Messages {
			if candidate.Role == "user" && strings.Contains(string(candidate.Content), wecomE2EPrompt) {
				found = true
			}
		}
		if !found {
			p.reject(w, "decrypted WeCom text did not reach the model")
			return
		}
		arguments, _ := json.Marshal(map[string]any{"memory": wecomE2EMemory, "topics": []string{"receipts"}})
		message["tool_calls"] = []any{map[string]any{"index": 0, "id": "call-memory-1", "type": "function", "function": map[string]any{"name": memory.AddToolName, "arguments": string(arguments)}}}
		finish = "tool_calls"
	} else if call == 2 {
		found := false
		for _, candidate := range request.Messages {
			if candidate.Role == "tool" && strings.Contains(string(candidate.Content), wecomE2EMemory) {
				found = true
			}
		}
		if !found {
			p.reject(w, "framework memory tool result did not reach the second model call")
			return
		}
		message["content"] = wecomE2EReply
	} else {
		p.reject(w, "duplicate callback reexecuted the model")
		return
	}
	choice := map[string]any{"index": 0, "finish_reason": finish, "message": message}
	response := map[string]any{"id": fmt.Sprintf("completion-%d", call), "object": "chat.completion", "created": time.Now().Unix(), "model": "gpt-4o-mini", "choices": []any{choice}, "usage": map[string]int{"prompt_tokens": 20, "completion_tokens": 12, "total_tokens": 32}}
	if request.Stream {
		w.Header().Set("Content-Type", "text/event-stream")
		delete(choice, "message")
		choice["delta"] = message
		response["object"] = "chat.completion.chunk"
		encoded, _ := json.Marshal(response)
		_, _ = fmt.Fprintf(w, "data: %s\n\ndata: [DONE]\n\n", encoded)
		return
	}
	_ = json.NewEncoder(w).Encode(response)
}

func wecomE2ECallback(t *testing.T, baseURL string, binding tenant.ChannelBinding, key []byte, receiver, user, id string) (string, []byte) {
	t.Helper()
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	message, err := xml.Marshal(struct {
		XMLName  xml.Name `xml:"xml"`
		Receiver string   `xml:"ToUserName"`
		User     string   `xml:"FromUserName"`
		Created  string   `xml:"CreateTime"`
		Type     string   `xml:"MsgType"`
		Content  string   `xml:"Content"`
		ID       string   `xml:"MsgId"`
		AgentID  string   `xml:"AgentID"`
	}{Receiver: receiver, User: user, Created: timestamp, Type: "text", Content: wecomE2EPrompt, ID: id, AgentID: binding.AppID})
	if err != nil {
		t.Fatal(err)
	}
	plain := bytes.Repeat([]byte{0x42}, 16)
	plain = binary.BigEndian.AppendUint32(plain, uint32(len(message)))
	plain = append(plain, message...)
	plain = append(plain, receiver...)
	padding := 32 - len(plain)%32
	plain = append(plain, bytes.Repeat([]byte{byte(padding)}, padding)...)
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	encrypted := make([]byte, len(plain))
	cipher.NewCBCEncrypter(block, key[:aes.BlockSize]).CryptBlocks(encrypted, plain)
	encoded := base64.StdEncoding.EncodeToString(encrypted)
	nonce := "local-e2e-nonce"
	parts := []string{binding.Token, timestamp, nonce, encoded}
	sort.Strings(parts)
	digest := sha1.Sum([]byte(strings.Join(parts, "")))
	query := url.Values{"token": {binding.WebhookKey}, "timestamp": {timestamp}, "nonce": {nonce}, "msg_signature": {hex.EncodeToString(digest[:])}}
	envelope, err := xml.Marshal(struct {
		XMLName xml.Name `xml:"xml"`
		Encrypt string   `xml:"Encrypt"`
	}{Encrypt: encoded})
	if err != nil {
		t.Fatal(err)
	}
	return baseURL + "/webhook?" + query.Encode(), envelope
}

func wecomE2EPost(t *testing.T, client *http.Client, endpoint string, body []byte) int {
	t.Helper()
	response, err := client.Post(endpoint, "application/xml", bytes.NewReader(body))
	if err != nil {
		t.Errorf("local encrypted callback request failed")
		return 0
	}
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	return response.StatusCode
}

func assertWeComE2ECount(t *testing.T, db *sql.DB, query, tenantID string, want int, extra ...any) {
	t.Helper()
	args := append([]any{tenantID}, extra...)
	var count int
	if err := db.QueryRow(query, args...).Scan(&count); err != nil || count != want {
		t.Fatalf("durable invariant %q: count=%d want=%d err=%v", query, count, want, err)
	}
}

func waitWeComE2EDelivery(t *testing.T, ctx context.Context, db *sql.DB, tenantID string, protocolErrors <-chan string) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var complete bool
		if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM outbox_messages WHERE tenant_id=$1 AND status='REPLIED')`, tenantID).Scan(&complete); err != nil {
			t.Fatal(err)
		}
		if complete {
			return
		}
		select {
		case message := <-protocolErrors:
			t.Fatal(message)
		case <-ctx.Done():
			t.Fatal("local WeCom pipeline did not complete before its deadline")
		case <-ticker.C:
		}
	}
}

func cleanupWeComE2E(t *testing.T, db *sql.DB, tenantID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, query := range []string{
		`DELETE FROM outbox_messages WHERE tenant_id=$1`,
		`DELETE FROM inbox_messages WHERE tenant_id=$1`,
		`DELETE FROM audit_logs WHERE tenant_id=$1`,
		`DELETE FROM control_plane_audit WHERE tenant_id=$1`,
		`DELETE FROM tenants WHERE id=$1`,
	} {
		if _, err := db.ExecContext(ctx, query, tenantID); err != nil {
			t.Errorf("clean up scoped WeCom fixture: %v", err)
		}
	}
}
