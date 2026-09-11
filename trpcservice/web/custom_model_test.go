package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	agentruntime "github.com/cyl6/trpc-agent-service/trpcservice/agent"
	"github.com/cyl6/trpc-agent-service/trpcservice/channels"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"github.com/cyl6/trpc-agent-service/trpcservice/coordination"
	"github.com/cyl6/trpc-agent-service/trpcservice/metrics"
	"github.com/cyl6/trpc-agent-service/trpcservice/tenant"
	"github.com/cyl6/trpc-agent-service/trpcservice/tenant/governance"
	"github.com/cyl6/trpc-agent-service/trpcservice/worker"
)

func customModelServer(t *testing.T, configPath string) *Server {
	t.Helper()
	t.Setenv("ADMIN_TOKEN", "admin-test-token")
	cfg := gatewayConfig(t)
	registry, err := tenant.NewRegistry(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return NewServer(registry, nil, nil, nil, metrics.NewMetrics(), "ADMIN_TOKEN", configPath, cfg)
}

func postCustomModel(t *testing.T, server *Server, payload string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants/custom-model", strings.NewReader(payload))
	req.Header.Set("Authorization", "Bearer admin-test-token")
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	server.Handler().ServeHTTP(res, req)
	return res
}

func getTenantList(t *testing.T, server *Server) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/admin/v1/tenants", nil)
	req.Header.Set("Authorization", "Bearer admin-test-token")
	res := httptest.NewRecorder()
	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("tenant list status=%d body=%s", res.Code, res.Body.String())
	}
	return res.Body.String()
}

func TestAddCustomModelRegistersTenant(t *testing.T) {
	server := customModelServer(t, "")
	res := postCustomModel(t, server, `{
		"api_format": "openai-chat-completions",
		"base_url": "https://api.openai.com/v1",
		"model_id": "gpt-4o-mini",
		"display_name": "我的测试模型",
		"api_key": "fixture-api-key"
	}`)
	if res.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", res.Code, res.Body.String())
	}
	body := res.Body.String()
	for _, want := range []string{`"tenant_id":"custom-gpt-4o-mini"`, `"display_name":"我的测试模型"`, `"provider":"openai"`, `"name":"gpt-4o-mini"`} {
		if !strings.Contains(body, want) {
			t.Fatalf("response missing %s: %s", want, body)
		}
	}
	if strings.Contains(body, "fixture-api-key") {
		t.Fatalf("api key leaked in response: %s", body)
	}

	// The tenant must appear in the admin list under its display name and the
	// API key must resolve through the runtime secret registry.
	list := getTenantList(t, server)
	for _, want := range []string{"custom-gpt-4o-mini", "我的测试模型"} {
		if !strings.Contains(list, want) {
			t.Fatalf("tenant list missing %s: %s", want, list)
		}
	}
	if strings.Contains(list, "fixture-api-key") {
		t.Fatalf("tenant list leaked api key: %s", list)
	}
	key, err := config.Secret("CUSTOM_MODEL_KEY_CUSTOM_GPT_4O_MINI")
	if err != nil || key != "fixture-api-key" {
		t.Fatalf("runtime secret not registered: key=%q err=%v", key, err)
	}

	// Registry lookup succeeds and points the OpenAI client at the base URL.
	tenantConfig, err := server.tenants.Tenant("custom-gpt-4o-mini")
	if err != nil {
		t.Fatalf("registered tenant not found: %v", err)
	}
	if tenantConfig.Model.BaseURL != "https://api.openai.com/v1" || tenantConfig.Model.APIKeyEnv != "CUSTOM_MODEL_KEY_CUSTOM_GPT_4O_MINI" {
		t.Fatalf("unexpected model config: %+v", tenantConfig.Model)
	}
	config.RegisterSecret("CUSTOM_MODEL_KEY_CUSTOM_GPT_4O_MINI", "")
}

func TestAddCustomModelFullURLStripsChatSuffix(t *testing.T) {
	server := customModelServer(t, "")
	res := postCustomModel(t, server, `{
		"base_url": "https://llm.example.com/api/v1/chat/completions",
		"full_url": true,
		"model_id": "qwen-max",
		"api_key": "fixture-api-key"
	}`)
	if res.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", res.Code, res.Body.String())
	}
	tenantConfig, err := server.tenants.Tenant("custom-qwen-max")
	if err != nil {
		t.Fatalf("tenant not found: %v", err)
	}
	if tenantConfig.Model.BaseURL != "https://llm.example.com/api/v1" {
		t.Fatalf("full url not normalized: %s", tenantConfig.Model.BaseURL)
	}
	config.RegisterSecret("CUSTOM_MODEL_KEY_CUSTOM_QWEN_MAX", "")
}

func TestAddCustomModelDefaultsDisplayNameToModelID(t *testing.T) {
	server := customModelServer(t, "")
	res := postCustomModel(t, server, `{
		"base_url": "https://api.example.com/v1",
		"model_id": "deepseek-chat",
		"api_key": "fixture-api-key"
	}`)
	if res.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", res.Code, res.Body.String())
	}
	if !strings.Contains(res.Body.String(), `"display_name":"deepseek-chat"`) {
		t.Fatalf("display name should default to model id: %s", res.Body.String())
	}
	config.RegisterSecret("CUSTOM_MODEL_KEY_CUSTOM_DEEPSEEK_CHAT", "")
}

func TestAddCustomModelRejectsInvalidInput(t *testing.T) {
	cases := map[string]string{
		"missing model id": `{"base_url":"https://api.example.com/v1","api_key":"fixture-api-key"}`,
		"missing api key":  `{"base_url":"https://api.example.com/v1","model_id":"m"}`,
		"missing base url": `{"model_id":"m","api_key":"fixture-api-key"}`,
		"relative url":     `{"base_url":"api.example.com/v1","model_id":"m","api_key":"fixture-api-key"}`,
		"url credentials":  `{"base_url":"https://user:fixture-credential@api.example.com/v1","model_id":"m","api_key":"fixture-api-key"}`,
		"url query":        `{"base_url":"https://api.example.com/v1?key=value","model_id":"m","api_key":"fixture-api-key"}`,
		"empty url query":  `{"base_url":"https://api.example.com/v1?","model_id":"m","api_key":"fixture-api-key"}`,
		"url fragment":     `{"base_url":"https://api.example.com/v1#fragment","model_id":"m","api_key":"fixture-api-key"}`,
		"empty fragment":   `{"base_url":"https://api.example.com/v1#","model_id":"m","api_key":"fixture-api-key"}`,
		"full url no path": `{"base_url":"https://api.example.com/v1","full_url":true,"model_id":"m","api_key":"fixture-api-key"}`,
		"full url no flag": `{"base_url":"https://api.example.com/v1/chat/completions","model_id":"m","api_key":"fixture-api-key"}`,
		"bad api format":   `{"api_format":"anthropic","base_url":"https://api.example.com/v1","model_id":"m","api_key":"fixture-api-key"}`,
		"long display":     `{"base_url":"https://api.example.com/v1","model_id":"m","api_key":"fixture-api-key","display_name":"0123456789012345678901234567890123"}`,
		"model id junk":    `{"base_url":"https://api.example.com/v1","model_id":"///","api_key":"fixture-api-key"}`,
		"invalid json":     `{"model_id":`,
		"multiple objects": `{"base_url":"https://api.example.com/v1","model_id":"m","api_key":"fixture-api-key"} {}`,
	}
	for name, payload := range cases {
		server := customModelServer(t, "")
		if res := postCustomModel(t, server, payload); res.Code != http.StatusBadRequest {
			t.Fatalf("%s: status=%d body=%s", name, res.Code, res.Body.String())
		}
	}
}

func TestAddCustomModelRejectsDuplicateTenant(t *testing.T) {
	server := customModelServer(t, "")
	payload := `{"base_url":"https://api.example.com/v1","model_id":"llama-3","api_key":"fixture-api-key"}`
	if res := postCustomModel(t, server, payload); res.Code != http.StatusCreated {
		t.Fatalf("first create status=%d body=%s", res.Code, res.Body.String())
	}
	if res := postCustomModel(t, server, payload); res.Code != http.StatusConflict {
		t.Fatalf("duplicate status=%d body=%s", res.Code, res.Body.String())
	}
	config.RegisterSecret("CUSTOM_MODEL_KEY_CUSTOM_LLAMA_3", "")
}

func TestAddCustomModelRequiresAdminToken(t *testing.T) {
	server := customModelServer(t, "")
	req := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants/custom-model", strings.NewReader(`{}`))
	res := httptest.NewRecorder()
	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d body=%s", res.Code, res.Body.String())
	}
}

func TestReloadKeepsCustomTenants(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(gatewayConfigYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	server := customModelServer(t, path)
	if res := postCustomModel(t, server, `{"base_url":"https://api.example.com/v1","model_id":"kimi","api_key":"fixture-api-key"}`); res.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", res.Code, res.Body.String())
	}

	req := httptest.NewRequest(http.MethodPost, "/admin/v1/reload", nil)
	req.Header.Set("Authorization", "Bearer admin-test-token")
	res := httptest.NewRecorder()
	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("reload status=%d body=%s", res.Code, res.Body.String())
	}

	list := getTenantList(t, server)
	if !strings.Contains(list, "custom-kimi") || !strings.Contains(list, "tenant-a") {
		t.Fatalf("reload dropped a tenant: %s", list)
	}
	config.RegisterSecret("CUSTOM_MODEL_KEY_CUSTOM_KIMI", "")
}

func TestCustomModelChatCallsConfiguredOpenAIEndpoint(t *testing.T) {
	t.Setenv("ADMIN_TOKEN", "admin-test-token")
	t.Setenv("SKILLS_ROOT", "")

	type capturedRequest struct {
		Authorization string
		Path          string
		Model         string `json:"model"`
		Messages      []struct {
			Role    string `json:"role"`
			Content any    `json:"content"`
		} `json:"messages"`
	}
	captured := make(chan capturedRequest, 1)
	modelServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request capturedRequest
		request.Authorization = r.Header.Get("Authorization")
		request.Path = r.URL.Path
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			http.Error(w, "invalid request", http.StatusBadRequest)
			return
		}
		captured <- request
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"id":"chatcmpl-integration","object":"chat.completion","created":1,
			"model":"integration-model",
			"choices":[{"index":0,"message":{"role":"assistant","content":"来自真实模型链路的回复"},"finish_reason":"stop"}],
			"usage":{"prompt_tokens":12,"completion_tokens":4,"total_tokens":16}
		}`))
	}))
	defer modelServer.Close()

	cfg := gatewayConfig(t)
	registry, err := tenant.NewRegistry(cfg)
	if err != nil {
		t.Fatal(err)
	}
	runtimes := agentruntime.NewManager()
	coordinator := coordination.NewInMemory()
	defer func() {
		_ = runtimes.Close()
		_ = coordinator.Close()
	}()
	workerService := worker.NewService(
		runtimes,
		coordinator,
		channels.NewRegistry(),
		governance.NewFilter(),
		nil,
		metrics.NewMetrics(),
		worker.Options{LockTTL: time.Minute, DedupTTL: time.Hour, RunTimeout: 5 * time.Second},
	)
	server := NewServer(
		registry,
		channels.NewRegistry(),
		nil,
		workerService,
		metrics.NewMetrics(),
		"ADMIN_TOKEN",
		"",
		cfg,
	)

	create := postCustomModel(t, server, `{
		"base_url": `+strconvQuote(modelServer.URL+"/v1")+`,
		"model_id": "integration-model",
		"api_key": "fixture-integration-key"
	}`)
	if create.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	defer config.RegisterSecret("CUSTOM_MODEL_KEY_CUSTOM_INTEGRATION_MODEL", "")

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/custom-integration-model",
		strings.NewReader(`{"message_id":"chat-1","user_id":"alice","conversation_id":"conversation-1","scope":"direct","text":"请回复测试内容"}`),
	)
	req.Header.Set("Authorization", "Bearer admin-test-token")
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	server.Handler().ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("chat status=%d body=%s", res.Code, res.Body.String())
	}
	if !strings.Contains(res.Body.String(), "来自真实模型链路的回复") ||
		!strings.Contains(res.Body.String(), `"prompt_tokens":12`) ||
		!strings.Contains(res.Body.String(), `"completion_tokens":4`) {
		t.Fatalf("chat response did not carry model output and usage: %s", res.Body.String())
	}
	if strings.Contains(res.Body.String(), `"outbound"`) {
		t.Fatalf("internal channel routing metadata leaked in chat response: %s", res.Body.String())
	}

	select {
	case request := <-captured:
		if request.Path != "/v1/chat/completions" {
			t.Fatalf("model endpoint path=%q", request.Path)
		}
		if request.Authorization != "Bearer fixture-integration-key" {
			t.Fatalf("model authorization header=%q", request.Authorization)
		}
		if request.Model != "integration-model" {
			t.Fatalf("model request model=%q", request.Model)
		}
		if len(request.Messages) == 0 {
			t.Fatal("model request contains no messages")
		}
	default:
		t.Fatal("configured model endpoint was not called")
	}
}

func strconvQuote(value string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}
