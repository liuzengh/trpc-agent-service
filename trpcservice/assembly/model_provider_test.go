package assembly

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/credential"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func mustAssemblyModelCatalog(t *testing.T, providers []config.ModelProviderConfig) *config.ModelCatalog {
	t.Helper()
	catalog, err := config.NewModelCatalog(providers)
	if err != nil {
		t.Fatalf("NewModelCatalog() error = %v", err)
	}
	return catalog
}

func TestManagedModelProviderBuildsOpenAIAndHunyuanAndFailover(t *testing.T) {
	secrets, err := credential.NewEnvironmentSecretResolver(func(key string) string {
		switch key {
		case "OPENAI_KEY":
			return "test-openai-key"
		case "HUNYUAN_SID":
			return "test-secret-id"
		case "HUNYUAN_SKEY":
			return "test-secret-key"
		default:
			return ""
		}
	})
	if err != nil {
		t.Fatalf("NewEnvironmentSecretResolver() error = %v", err)
	}

	providers := []config.ModelProviderConfig{
		{
			ID:        "openai-main",
			Type:      config.ModelProviderOpenAI,
			BaseURL:   "https://api.openai.com/v1",
			APIKeyRef: "env:OPENAI_KEY",
			Models:    []config.ModelPricingConfig{{Name: "gpt-4o"}, {Name: "gpt-4o-mini"}},
		},
		{
			ID:           "hunyuan-main",
			Type:         config.ModelProviderHunyuan,
			SecretIDRef:  "env:HUNYUAN_SID",
			SecretKeyRef: "env:HUNYUAN_SKEY",
			Models:       []config.ModelPricingConfig{{Name: "hunyuan-pro"}},
		},
	}

	mp, err := NewManagedModelProvider(secrets, mustAssemblyModelCatalog(t, providers))
	if err != nil {
		t.Fatalf("NewManagedModelProvider() error = %v", err)
	}

	// 1. Single OpenAI Model
	openaiCfg := config.TenantConfig{
		TenantID: "acme", AppCode: "bot1", Status: config.AgentActive, ConfigVersion: 1,
		Model: config.ModelConfig{ProviderID: "openai-main", Name: "gpt-4o"},
	}
	m1, err := mp.Model(context.Background(), openaiCfg)
	if err != nil {
		t.Fatalf("mp.Model(openaiCfg) error = %v", err)
	}
	if m1 == nil {
		t.Fatal("mp.Model(openaiCfg) returned nil")
	}

	// 2. Single Hunyuan Model
	hunyuanCfg := config.TenantConfig{
		TenantID: "acme", AppCode: "bot2", Status: config.AgentActive, ConfigVersion: 1,
		Model: config.ModelConfig{ProviderID: "hunyuan-main", Name: "hunyuan-pro"},
	}
	m2, err := mp.Model(context.Background(), hunyuanCfg)
	if err != nil {
		t.Fatalf("mp.Model(hunyuanCfg) error = %v", err)
	}
	if m2 == nil {
		t.Fatal("mp.Model(hunyuanCfg) returned nil")
	}

	// 3. Failover model (OpenAI primary + Hunyuan fallback)
	failoverCfg := config.TenantConfig{
		TenantID: "acme", AppCode: "bot3", Status: config.AgentActive, ConfigVersion: 1,
		Model: config.ModelConfig{
			ProviderID: "openai-main",
			Name:       "gpt-4o",
			FailoverCandidates: []config.ModelCandidate{
				{ProviderID: "hunyuan-main", Name: "hunyuan-pro"},
			},
		},
	}
	m3, err := mp.Model(context.Background(), failoverCfg)
	if err != nil {
		t.Fatalf("mp.Model(failoverCfg) error = %v", err)
	}
	if m3 == nil {
		t.Fatal("mp.Model(failoverCfg) returned nil")
	}

	// 4. Unknown provider
	unknownCfg := config.TenantConfig{
		TenantID: "acme", AppCode: "bot4", Status: config.AgentActive, ConfigVersion: 1,
		Model: config.ModelConfig{ProviderID: "non-existent", Name: "gpt-4o"},
	}
	if _, err := mp.Model(context.Background(), unknownCfg); err == nil {
		t.Fatal("mp.Model(unknownCfg) error = nil, want unknown provider error")
	}

	// 5. Unmanaged model name
	unmanagedModelCfg := config.TenantConfig{
		TenantID: "acme", AppCode: "bot5", Status: config.AgentActive, ConfigVersion: 1,
		Model: config.ModelConfig{ProviderID: "openai-main", Name: "unmanaged-model"},
	}
	if _, err := mp.Model(context.Background(), unmanagedModelCfg); err == nil {
		t.Fatal("mp.Model(unmanagedModelCfg) error = nil, want unmanaged model error")
	}
}

func TestModelAcceptsNonTextInputRequiresExplicitConfiguredCapability(t *testing.T) {
	provider := config.ModelProviderConfig{Models: []config.ModelPricingConfig{
		{Name: "text-only"},
		{Name: "vision", Capabilities: &config.ModelCapabilities{Input: &config.ModelInputCapabilities{Image: true}}},
		{Name: "audio", Capabilities: &config.ModelCapabilities{Input: &config.ModelInputCapabilities{Audio: true}}},
	}}
	if modelAcceptsNonTextInput(provider, "text-only") {
		t.Fatal("text-only model unexpectedly accepts non-text input")
	}
	if !modelAcceptsNonTextInput(provider, "vision") || !modelAcceptsNonTextInput(provider, "audio") {
		t.Fatal("explicit multimodal capability was not honored")
	}
	if modelAcceptsNonTextInput(provider, "discovered-only") {
		t.Fatal("unconfigured discovered model must not gain multimodal capability")
	}
}

func TestManagedModelProviderPersistsEachInvocationModelCall(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/chat/completions" {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(map[string]any{
			"id": "response-1", "object": "chat.completion", "created": 1, "model": "support-model",
			"choices": []any{map[string]any{
				"index": 0, "message": map[string]any{"role": "assistant", "content": "收到"}, "finish_reason": "stop",
			}},
		})
	}))
	t.Cleanup(server.Close)
	secrets, err := credential.NewEnvironmentSecretResolver(func(key string) string {
		if key == "MODEL_KEY" {
			return "test-key"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	audits := storage.NewMemoryStateStore()
	provider, err := NewManagedModelProvider(secrets, mustAssemblyModelCatalog(t, []config.ModelProviderConfig{{
		ID: "primary", Type: config.ModelProviderOpenAI, BaseURL: server.URL, APIKeyRef: "env:MODEL_KEY",
		Models: []config.ModelPricingConfig{{Name: "support-model"}},
	}}), WithModelAuditRecorder(audits))
	if err != nil {
		t.Fatal(err)
	}
	configured, err := provider.Model(context.Background(), config.TenantConfig{
		TenantID: "tenant-a", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Model: config.ModelConfig{ProviderID: "primary", Name: "support-model"},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := governance.WithInvocation(context.Background(), governance.Invocation{
		Execution: governance.ExecutionContext{
			TenantID: "tenant-a", AppCode: "support", Role: "member", TraceID: "trace-1", RequestID: "message-1", PolicyVersion: "1",
			Channel: "web", BindingID: "web-console", AgentName: "assistant", SessionID: "tenant-a/support/session/1",
		},
		Budget: governance.NewCallBudget(1),
	})
	responses, err := configured.GenerateContent(ctx, &model.Request{
		Messages: []model.Message{model.NewUserMessage("你好")},
	})
	if err != nil {
		t.Fatal(err)
	}
	for range responses {
	}
	events, err := audits.ListAudit(context.Background(), "tenant-a", "trace-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].Action != "model.requested" || events[1].Action != "model.completed" ||
		events[0].RequestID != "message-1" || events[1].LatencyMS < 0 {
		t.Fatalf("model audit events = %+v", events)
	}
}

func TestFactoryAppliesGenerationConfig(t *testing.T) {
	temp := 0.7
	maxTokens := 1024
	thinking := true

	modelInstance := &recordingModelProvider{}
	factory := NewFactoryWithModelProvider(modelInstance, nil, nil, nil, nil, nil, nil)
	tenantConfig := config.TenantConfig{
		TenantID: "acme", AppCode: "support", Status: config.AgentActive, ConfigVersion: 1,
		Model: config.ModelConfig{
			ProviderID: "primary",
			Name:       "support",
			Generation: &model.GenerationConfig{
				Temperature:     &temp,
				MaxTokens:       &maxTokens,
				ThinkingEnabled: &thinking,
			},
		},
	}

	runner, err := factory.Get(context.Background(), tenantConfig)
	if err != nil {
		t.Fatalf("Get() with generation config error = %v", err)
	}
	if runner == nil {
		t.Fatal("Get() returned nil runner")
	}
}

func TestManagedModelProviderBuildsZhipuAI(t *testing.T) {
	secrets, err := credential.NewEnvironmentSecretResolver(func(key string) string {
		if key == "ZHIPU_KEY" {
			return "test-zhipu-key"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("NewEnvironmentSecretResolver() error = %v", err)
	}

	providers := []config.ModelProviderConfig{
		{
			ID:        "zhipu-ai",
			Type:      config.ModelProviderOpenAI,
			BaseURL:   "https://open.bigmodel.cn/api/paas/v4/",
			APIKeyRef: "env:ZHIPU_KEY",
			Models:    []config.ModelPricingConfig{{Name: "glm-5.3-flash"}, {Name: "glm-4-plus"}},
		},
	}

	mp, err := NewManagedModelProvider(secrets, mustAssemblyModelCatalog(t, providers))
	if err != nil {
		t.Fatalf("NewManagedModelProvider() error = %v", err)
	}

	cfg := config.TenantConfig{
		TenantID: "acme", AppCode: "coding-bot", Status: config.AgentActive, ConfigVersion: 1,
		Model: config.ModelConfig{ProviderID: "zhipu-ai", Name: "glm-5.3-flash"},
	}
	m, err := mp.Model(context.Background(), cfg)
	if err != nil {
		t.Fatalf("mp.Model(cfg) error = %v", err)
	}
	if m == nil {
		t.Fatal("mp.Model(cfg) returned nil")
	}
}

func TestManagedModelProviderBuildsFrameworkHuggingFace(t *testing.T) {
	modelName := "meta-llama/Llama-3.1-8B-Instruct"
	secrets, err := credential.NewEnvironmentSecretResolver(func(key string) string {
		if key == "HF_KEY" {
			return "test-hf-key"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	providers := []config.ModelProviderConfig{{
		ID: "hf-main", Type: config.ModelProviderHuggingFace, APIKeyRef: "env:HF_KEY",
		Models: []config.ModelPricingConfig{{Name: modelName}},
	}}
	mp, err := NewManagedModelProvider(secrets, mustAssemblyModelCatalog(t, providers))
	if err != nil {
		t.Fatal(err)
	}
	m, err := mp.Model(context.Background(), config.TenantConfig{
		TenantID: "acme", AppCode: "hf", Status: config.AgentActive, ConfigVersion: 1,
		Model: config.ModelConfig{ProviderID: "hf-main", Name: modelName},
	})
	if err != nil {
		t.Fatalf("Model() error = %v", err)
	}
	if m == nil {
		t.Fatal("Model() returned nil")
	}
}

func TestManagedModelProviderSyncUsesSecretResolverAndSharedCatalog(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/models" {
			http.NotFound(w, r)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Bearer resolved-key" {
			t.Fatalf("Authorization = %q, want resolved secret", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"object": "list",
			"data":   []map[string]any{{"id": "discovered-model", "object": "model"}},
		})
	}))
	defer server.Close()

	secrets, err := credential.NewEnvironmentSecretResolver(func(key string) string {
		if key == "MODEL_KEY" {
			return "resolved-key"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	catalog := mustAssemblyModelCatalog(t, []config.ModelProviderConfig{{
		ID: "openai-main", Type: config.ModelProviderOpenAI, BaseURL: server.URL, APIKeyRef: "env:MODEL_KEY",
	}})
	provider, err := NewManagedModelProvider(secrets, catalog)
	if err != nil {
		t.Fatal(err)
	}
	configured, err := provider.SyncProvider(context.Background(), server.Client(), "openai-main")
	if err != nil {
		t.Fatalf("SyncProvider() error = %v", err)
	}
	if !configured {
		t.Fatal("SyncProvider() configured = false, want true")
	}
	if got := catalog.ListDiscoveredModels("openai-main"); len(got) != 1 || got[0] != "discovered-model" {
		t.Fatalf("catalog discovered models = %v", got)
	}
}

func TestManagedModelProviderSyncReportsSecretResolverFailure(t *testing.T) {
	secrets, err := credential.NewEnvironmentSecretResolver(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	provider, err := NewManagedModelProvider(secrets, mustAssemblyModelCatalog(t, []config.ModelProviderConfig{{
		ID: "openai-main", Type: config.ModelProviderOpenAI, BaseURL: "https://api.example.com/v1", APIKeyRef: "env:MODEL_KEY",
	}}))
	if err != nil {
		t.Fatal(err)
	}
	configured, err := provider.SyncProvider(context.Background(), http.DefaultClient, "openai-main")
	if err == nil {
		t.Fatal("SyncProvider() error = nil, want resolver failure surfaced")
	}
	if configured {
		t.Fatal("SyncProvider() configured = true, want false on resolver failure")
	}
}

func TestManagedModelProviderFailoverRecoversFromProviderFailures(t *testing.T) {
	tests := []struct {
		name       string
		primaryURL func(*testing.T) string
	}{
		{
			name: "rate limited",
			primaryURL: func(t *testing.T) string {
				server := httptest.NewServer(openAIErrorHandler(http.StatusTooManyRequests))
				t.Cleanup(server.Close)
				return server.URL
			},
		},
		{
			name: "service unavailable",
			primaryURL: func(t *testing.T) string {
				server := httptest.NewServer(openAIErrorHandler(http.StatusServiceUnavailable))
				t.Cleanup(server.Close)
				return server.URL
			},
		},
		{
			name: "network failure",
			primaryURL: func(t *testing.T) string {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				address := listener.Addr().String()
				if err := listener.Close(); err != nil {
					t.Fatal(err)
				}
				return "http://" + address
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/chat/completions" {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"id": "fallback", "object": "chat.completion", "created": 1699200000, "model": "fallback-model",
					"choices": []any{map[string]any{
						"index": 0, "message": map[string]any{"role": "assistant", "content": "fallback-ok"}, "finish_reason": "stop",
					}},
				})
			}))
			defer fallback.Close()

			secrets, err := credential.NewEnvironmentSecretResolver(func(string) string { return "test-key" })
			if err != nil {
				t.Fatal(err)
			}
			providers := []config.ModelProviderConfig{
				{ID: "primary", Type: config.ModelProviderOpenAI, BaseURL: tt.primaryURL(t), APIKeyRef: "env:PRIMARY_KEY", Models: []config.ModelPricingConfig{{Name: "primary-model"}}},
				{ID: "fallback", Type: config.ModelProviderOpenAI, BaseURL: fallback.URL, APIKeyRef: "env:FALLBACK_KEY", Models: []config.ModelPricingConfig{{Name: "fallback-model"}}},
			}
			provider, err := NewManagedModelProvider(secrets, mustAssemblyModelCatalog(t, providers))
			if err != nil {
				t.Fatal(err)
			}
			managed, err := provider.Model(context.Background(), config.TenantConfig{
				TenantID: "acme", AppCode: "failover", Status: config.AgentActive, ConfigVersion: 1,
				Model: config.ModelConfig{
					ProviderID: "primary", Name: "primary-model",
					FailoverCandidates: []config.ModelCandidate{{ProviderID: "fallback", Name: "fallback-model"}},
				},
			})
			if err != nil {
				t.Fatal(err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			defer cancel()
			responses, err := managed.GenerateContent(ctx, &model.Request{
				Messages:         []model.Message{model.NewUserMessage("hello")},
				GenerationConfig: model.GenerationConfig{Stream: false},
			})
			if err != nil {
				t.Fatalf("GenerateContent() error = %v", err)
			}
			var final *model.Response
			for response := range responses {
				final = response
			}
			if final == nil {
				t.Fatal("failover returned no response")
			}
			if final.Error != nil {
				t.Fatalf("failover returned error: %+v", final.Error)
			}
			if len(final.Choices) == 0 || final.Choices[0].Message.Content != "fallback-ok" {
				t.Fatalf("failover response = %+v, want fallback-ok", final)
			}
		})
	}
}

func openAIErrorHandler(status int) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{"message": "primary unavailable", "type": "server_error"},
		})
	})
}
