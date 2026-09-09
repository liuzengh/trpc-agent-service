package executor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	frameworkmemory "trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/session"

	"github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/message"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestRuntimeRoutesAndIsolatesRedisAndInMemoryTenants(t *testing.T) {
	redisServer := miniredis.RunT(t)
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode model request: %v", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"multi","object":"chat.completion","created":1,"model":"` + body.Model + `","choices":[{"index":0,"message":{"role":"assistant","content":"reply-` + body.Model + `"},"finish_reason":"stop"}]}`))
	}))
	defer modelServer.Close()

	catalog := multiTenantCatalog(modelServer.URL)
	cfg, err := config.NewCatalogConfig(catalog, []byte("01234567890123456789012345678901"), config.NewStaticCredentialResolver(map[string]string{
		"env:MODEL_A": "model-key-a", "env:MODEL_B": "model-key-b",
		"env:REDIS_A": "redis://" + redisServer.Addr() + "/0",
	}))
	if err != nil {
		t.Fatal(err)
	}
	runtime, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if err := runtime.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}

	request := func(bindingID string) Reply {
		reply, requestErr := runtime.Handle(context.Background(), Request{
			Channel: "demo", BindingID: bindingID, MessageID: "message-1",
			ExternalUserID: "same-user", ConversationID: "same-conversation",
			Text: "hello", RequestID: "request", TraceID: "trace",
		})
		if requestErr != nil {
			t.Fatalf("Handle(%q) error = %v", bindingID, requestErr)
		}
		return reply
	}
	replyA := request("binding-a")
	replyB := request("binding-b")
	if replyA.Text != "reply-model-a" || replyB.Text != "reply-model-b" {
		t.Fatalf("unexpected replies: A=%#v B=%#v", replyA, replyB)
	}

	backendA, err := runtime.backendForBinding(context.Background(), "demo", "binding-a")
	if err != nil {
		t.Fatal(err)
	}
	backendB, err := runtime.backendForBinding(context.Background(), "demo", "binding-b")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := backendA.(*storage.RedisBackend); !ok {
		t.Fatalf("tenant A backend = %T", backendA)
	}
	if _, ok := backendB.(*storage.InMemoryBackend); !ok {
		t.Fatalf("tenant B backend = %T", backendB)
	}
	keyA := session.Key{
		AppName:   tenant.AppName("tenant-a", "assistant"),
		UserID:    identity.RunnerUserID(cfg.IdentitySecret, "binding-a", "same-user"),
		SessionID: identity.SessionID(cfg.IdentitySecret, "binding-a", "same-conversation"),
	}
	keyB := session.Key{
		AppName:   tenant.AppName("tenant-b", "assistant"),
		UserID:    identity.RunnerUserID(cfg.IdentitySecret, "binding-b", "same-user"),
		SessionID: identity.SessionID(cfg.IdentitySecret, "binding-b", "same-conversation"),
	}
	if current, err := backendA.Session().GetSession(context.Background(), keyA); err != nil || current == nil {
		t.Fatalf("tenant A session = (%#v, %v)", current, err)
	}
	if current, err := backendB.Session().GetSession(context.Background(), keyB); err != nil || current == nil {
		t.Fatalf("tenant B session = (%#v, %v)", current, err)
	}
	if current, err := backendA.Session().GetSession(context.Background(), keyB); err != nil || current != nil {
		t.Fatalf("tenant A could read tenant B session: (%#v, %v)", current, err)
	}
	if current, err := backendB.Session().GetSession(context.Background(), keyA); err != nil || current != nil {
		t.Fatalf("tenant B could read tenant A session: (%#v, %v)", current, err)
	}
	memoryKeyA := frameworkmemory.UserKey{AppName: keyA.AppName, UserID: keyA.UserID}
	memoryKeyB := frameworkmemory.UserKey{AppName: keyB.AppName, UserID: keyB.UserID}
	if err := backendA.Memory().AddMemory(context.Background(), memoryKeyA, "tenant-a-memory", nil); err != nil {
		t.Fatal(err)
	}
	if err := backendB.Memory().AddMemory(context.Background(), memoryKeyB, "tenant-b-memory", nil); err != nil {
		t.Fatal(err)
	}
	if entries, err := backendA.Memory().SearchMemories(context.Background(), memoryKeyB, "tenant-b-memory"); err != nil || len(entries) != 0 {
		t.Fatalf("tenant A could read tenant B memory: (%#v, %v)", entries, err)
	}
	if entries, err := backendB.Memory().SearchMemories(context.Background(), memoryKeyA, "tenant-a-memory"); err != nil || len(entries) != 0 {
		t.Fatalf("tenant B could read tenant A memory: (%#v, %v)", entries, err)
	}

	redisServer.Close()
	if err := runtime.Ready(context.Background()); err == nil {
		t.Fatal("strict readiness succeeded with tenant A Redis down")
	}
	request("binding-b")
	if _, err := runtime.Handle(context.Background(), Request{
		Channel: "demo", BindingID: "binding-a", MessageID: "message-2",
		ExternalUserID: "same-user", ConversationID: "same-conversation", Text: "hello",
	}); !errors.Is(err, ErrDependencyUnavailable) {
		t.Fatalf("tenant A Redis outage error = %v", err)
	}
	if err := redisServer.Restart(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Ready(context.Background()); err != nil {
		t.Fatalf("runtime did not recover after Redis restart: %v", err)
	}
	if _, err := runtime.Handle(context.Background(), Request{Channel: "demo", BindingID: "missing"}); !errors.Is(err, ErrUnknownBinding) {
		t.Fatalf("unknown binding error = %v", err)
	}
}

func TestRuntimeStorageProfileVersionSwitchIsCold(t *testing.T) {
	redisServer := miniredis.RunT(t)
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
		_, _ = w.Write([]byte(`{"id":"cold","object":"chat.completion","created":1,"model":"model","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`))
	}))
	defer modelServer.Close()

	catalog := coldSwitchCatalog(modelServer.URL)
	baseRepository, err := tenant.NewPresetRepository(catalog)
	if err != nil {
		t.Fatal(err)
	}
	repository := &switchingRepository{Repository: baseRepository, version: "v1"}
	credentials := config.NewStaticCredentialResolver(map[string]string{
		"env:MODEL_KEY": "model-key", "env:REDIS_URL": "redis://" + redisServer.Addr() + "/0",
	})
	backends, err := storage.NewBackendProvider(repository, credentials)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := agent.NewRunnerRegistry(repository, backends, credentials, agent.DefaultCacheConfig(), newRunner)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{
		identitySecret: []byte("01234567890123456789012345678901"),
		repository:     repository, backends: backends, registry: registry,
	}
	defer runtime.Close()

	request := Request{
		Channel: "demo", BindingID: "binding-a", ExternalUserID: "user",
		ConversationID: "conversation", Text: "hello", TraceID: "trace",
	}
	request.MessageID, request.RequestID = "v1", "v1"
	if _, err := runtime.Handle(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	repository.SetVersion("v2")
	request.MessageID, request.RequestID = "v2", "v2"
	if _, err := runtime.Handle(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	requestsMu.Lock()
	defer requestsMu.Unlock()
	if len(messageCounts) != 2 || messageCounts[1] != messageCounts[0] {
		t.Fatalf("storage profile switch inherited old session, message counts = %v", messageCounts)
	}
}

func TestRuntimeSwitchesActiveConfigWithoutInterruptingOldRequest(t *testing.T) {
	startedV1 := make(chan struct{})
	releaseV1 := make(chan struct{})
	var startOnce sync.Once
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode model request: %v", err)
			return
		}
		if body.Model == "model-v1" {
			startOnce.Do(func() { close(startedV1) })
			<-releaseV1
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"switch","object":"chat.completion","created":1,"model":"` + body.Model + `","choices":[{"index":0,"message":{"role":"assistant","content":"` + body.Model + `"},"finish_reason":"stop"}]}`))
	}))
	defer modelServer.Close()

	catalog := versionSwitchCatalog(modelServer.URL)
	baseRepository, err := tenant.NewPresetRepository(catalog)
	if err != nil {
		t.Fatal(err)
	}
	repository := &switchingRepository{Repository: baseRepository, version: "v1"}
	credentials := config.NewStaticCredentialResolver(map[string]string{"env:MODEL_KEY": "model-key"})
	backends, err := storage.NewBackendProvider(repository, credentials)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := agent.NewRunnerRegistry(repository, backends, credentials, agent.DefaultCacheConfig(), newRunner)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{
		identitySecret: []byte("01234567890123456789012345678901"),
		repository:     repository, backends: backends, registry: registry,
	}
	defer runtime.Close()

	v1Done := make(chan Reply, 1)
	v1Err := make(chan error, 1)
	go func() {
		reply, err := runtime.Handle(context.Background(), Request{
			Channel: "demo", BindingID: "binding-a", MessageID: "v1", ExternalUserID: "user",
			ConversationID: "conversation", Text: "hello", RequestID: "v1", TraceID: "trace-v1",
		})
		v1Done <- reply
		v1Err <- err
	}()
	select {
	case <-startedV1:
	case <-time.After(2 * time.Second):
		t.Fatal("v1 request did not start")
	}
	repository.SetVersion("v2")
	v2Reply, err := runtime.Handle(context.Background(), Request{
		Channel: "demo", BindingID: "binding-a", MessageID: "v2", ExternalUserID: "user",
		ConversationID: "conversation", Text: "hello", RequestID: "v2", TraceID: "trace-v2",
	})
	if err != nil {
		t.Fatal(err)
	}
	if v2Reply.Text != "model-v2" {
		t.Fatalf("v2 reply = %#v", v2Reply)
	}
	close(releaseV1)
	if err := <-v1Err; err != nil {
		t.Fatal(err)
	}
	if reply := <-v1Done; reply.Text != "model-v1" {
		t.Fatalf("v1 reply = %#v", reply)
	}
	if err := registry.Drain(context.Background(), agent.CacheKey{TenantID: "tenant-a", AgentAppID: "assistant", ConfigVersion: "v1"}); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeExecuteUsesTaskConfigVersionAfterActiveVersionChanges(t *testing.T) {
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode model request: %v", err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"fixed","object":"chat.completion","created":1,"model":"` + body.Model + `","choices":[{"index":0,"message":{"role":"assistant","content":"` + body.Model + `"},"finish_reason":"stop"}]}`))
	}))
	defer modelServer.Close()

	baseRepository, err := tenant.NewPresetRepository(versionSwitchCatalog(modelServer.URL))
	if err != nil {
		t.Fatal(err)
	}
	repository := &switchingRepository{Repository: baseRepository, version: "v1"}
	credentials := config.NewStaticCredentialResolver(map[string]string{"env:MODEL_KEY": "model-key"})
	backends, err := storage.NewBackendProvider(repository, credentials)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := agent.NewRunnerRegistry(repository, backends, credentials, agent.DefaultCacheConfig(), newRunner)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &Runtime{repository: repository, backends: backends, registry: registry}
	defer runtime.Close()

	task := message.ExecutionTask{
		SchemaVersion: message.TaskSchemaVersion, TaskID: "task-fixed-v1", Channel: "demo", ChannelBindingID: "binding-a", ExternalAccountID: "demo-account",
		TenantID: "tenant-a", AgentAppID: "assistant", ConfigVersion: "v1", RunnerUserID: "user-fixed", SessionID: "session-fixed",
		PlatformMessageID: "message-fixed", ActorUserID: "actor-fixed", ConversationID: "conversation-fixed", ConversationType: message.ConversationDirect,
		Text: "hello", RequestID: "request-fixed", TraceID: "trace-fixed",
		ReceivedAt: time.Now().UTC(), Attempt: 1,
	}
	task.PayloadDigest = task.CanonicalDigest()
	repository.SetVersion("v2")
	reply, err := runtime.Execute(context.Background(), task)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Text != "model-v1" {
		t.Fatalf("Execute() reply = %#v, want model-v1", reply)
	}
}

type switchingRepository struct {
	tenant.Repository
	mu      sync.RWMutex
	version string
}

func (r *switchingRepository) SetVersion(version string) {
	r.mu.Lock()
	r.version = version
	r.mu.Unlock()
}

func (r *switchingRepository) GetAgentApp(ctx context.Context, tenantID, appID string) (tenant.AgentApp, error) {
	app, err := r.Repository.GetAgentApp(ctx, tenantID, appID)
	if err != nil {
		return tenant.AgentApp{}, err
	}
	r.mu.RLock()
	app.ActiveConfigVersion = r.version
	r.mu.RUnlock()
	return app, nil
}

func multiTenantCatalog(modelURL string) tenant.Catalog {
	return tenant.Catalog{
		Tenants: []tenant.Tenant{{ID: "tenant-a", Enabled: true}, {ID: "tenant-b", Enabled: true}},
		StorageProfiles: []tenant.StorageProfile{
			{TenantID: "tenant-a", ID: "redis-v1", Kind: tenant.StorageKindRedis, CredentialRef: "env:REDIS_A", KeyPrefix: "multi"},
			{TenantID: "tenant-b", ID: "memory-v1", Kind: tenant.StorageKindInMemory},
		},
		AgentApps: []tenant.AgentApp{
			{TenantID: "tenant-a", ID: "assistant", Enabled: true, ActiveConfigVersion: "v1"},
			{TenantID: "tenant-b", ID: "assistant", Enabled: true, ActiveConfigVersion: "v1"},
		},
		ConfigVersions: []tenant.ConfigVersion{
			{TenantID: "tenant-a", AgentAppID: "assistant", Version: "v1", StorageProfileID: "redis-v1", Instruction: "tenant A", Model: modelConfig("model-a", modelURL, "env:MODEL_A")},
			{TenantID: "tenant-b", AgentAppID: "assistant", Version: "v1", StorageProfileID: "memory-v1", Instruction: "tenant B", Model: modelConfig("model-b", modelURL, "env:MODEL_B")},
		},
		ChannelBindings: []tenant.ChannelBinding{
			{ID: "binding-a", Channel: "demo", ExternalAccountID: "binding-a", TenantID: "tenant-a", AgentAppID: "assistant", Enabled: true},
			{ID: "binding-b", Channel: "demo", ExternalAccountID: "binding-b", TenantID: "tenant-b", AgentAppID: "assistant", Enabled: true},
		},
	}
}

func versionSwitchCatalog(modelURL string) tenant.Catalog {
	return tenant.Catalog{
		Tenants:         []tenant.Tenant{{ID: "tenant-a", Enabled: true}},
		StorageProfiles: []tenant.StorageProfile{{TenantID: "tenant-a", ID: "memory-v1", Kind: tenant.StorageKindInMemory}},
		AgentApps:       []tenant.AgentApp{{TenantID: "tenant-a", ID: "assistant", Enabled: true, ActiveConfigVersion: "v1"}},
		ConfigVersions: []tenant.ConfigVersion{
			{TenantID: "tenant-a", AgentAppID: "assistant", Version: "v1", StorageProfileID: "memory-v1", Instruction: "v1", Model: modelConfig("model-v1", modelURL, "env:MODEL_KEY")},
			{TenantID: "tenant-a", AgentAppID: "assistant", Version: "v2", StorageProfileID: "memory-v1", Instruction: "v2", Model: modelConfig("model-v2", modelURL, "env:MODEL_KEY")},
		},
		ChannelBindings: []tenant.ChannelBinding{{
			ID: "binding-a", Channel: "demo", ExternalAccountID: "binding-a",
			TenantID: "tenant-a", AgentAppID: "assistant", Enabled: true,
		}},
	}
}

func coldSwitchCatalog(modelURL string) tenant.Catalog {
	return tenant.Catalog{
		Tenants: []tenant.Tenant{{ID: "tenant-a", Enabled: true}},
		StorageProfiles: []tenant.StorageProfile{
			{TenantID: "tenant-a", ID: "memory-v1", Kind: tenant.StorageKindInMemory},
			{TenantID: "tenant-a", ID: "redis-v2", Kind: tenant.StorageKindRedis, CredentialRef: "env:REDIS_URL", KeyPrefix: "cold-switch"},
		},
		AgentApps: []tenant.AgentApp{{TenantID: "tenant-a", ID: "assistant", Enabled: true, ActiveConfigVersion: "v1"}},
		ConfigVersions: []tenant.ConfigVersion{
			{TenantID: "tenant-a", AgentAppID: "assistant", Version: "v1", StorageProfileID: "memory-v1", Instruction: "v1", Model: modelConfig("model", modelURL, "env:MODEL_KEY")},
			{TenantID: "tenant-a", AgentAppID: "assistant", Version: "v2", StorageProfileID: "redis-v2", Instruction: "v2", Model: modelConfig("model", modelURL, "env:MODEL_KEY")},
		},
		ChannelBindings: []tenant.ChannelBinding{{
			ID: "binding-a", Channel: "demo", ExternalAccountID: "binding-a",
			TenantID: "tenant-a", AgentAppID: "assistant", Enabled: true,
		}},
	}
}

func modelConfig(name, baseURL, credentialRef string) tenant.ModelConfig {
	return tenant.ModelConfig{
		Name: name, BaseURL: baseURL, CredentialRef: credentialRef,
		RequestTimeout: 5 * time.Second, MaxOutputTokens: 128,
	}
}
