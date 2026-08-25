package agent

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func openAIConfig(tc tenant.TenantContext, spec AgentSpec, endpoint string) ModelConfig {
	return ModelConfig{
		TenantID: tc.TenantID, AgentAppID: tc.AgentAppID, ConfigVersion: tc.ConfigVersion,
		ConfigRef: spec.ModelConfigRef, Provider: spec.ModelProvider, Endpoint: endpoint,
		Model: "local-model", SecretRef: "secret/local",
	}
}

func TestOpenAIProviderFactoryLocalSuccess(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Header.Get("Authorization") != "Bearer local-secret" {
			t.Fatalf("missing local authorization header")
		}
		body, _ := io.ReadAll(r.Body)
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("invalid request: %v", err)
		}
		if payload["model"] != "local-model" {
			t.Fatalf("unexpected model: %#v", payload["model"])
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"local","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"local answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`)
	}))
	defer server.Close()

	tc := validContext()
	spec := validSpec()
	spec.ModelProvider = "openai"
	spec.ModelConfigRef = "ref-a"
	factory := OpenAIProviderFactory{
		Configs: ModelConfigResolverFunc(func(context.Context, tenant.TenantContext, AgentSpec) (ModelConfig, error) {
			return openAIConfig(tc, spec, server.URL), nil
		}),
		Secrets: SecretResolverFunc(func(context.Context, tenant.TenantContext, string) (string, error) { return "local-secret", nil }),
	}
	provider, err := factory.Build(context.Background(), tc, spec)
	if err != nil {
		t.Fatal(err)
	}
	result, err := provider.Complete(context.Background(), ProviderRequest{Messages: []Message{{Role: "user", Content: "hello"}}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Text != "local answer" || result.InputTokens != 3 || result.OutputTokens != 2 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if requests.Load() != 1 {
		t.Fatalf("expected one request, got %d", requests.Load())
	}
}

func TestOpenAIProviderPreservesResponseError(t *testing.T) {
	sentinelCode := "rate_limit"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"error":{"message":"busy","type":"rate_limit_error","code":"rate_limit"}}`)
	}))
	defer server.Close()

	tc := validContext()
	spec := validSpec()
	spec.ModelProvider = "openai"
	spec.ModelConfigRef = "ref-a"
	factory := OpenAIProviderFactory{
		Configs: ModelConfigResolverFunc(func(context.Context, tenant.TenantContext, AgentSpec) (ModelConfig, error) {
			return openAIConfig(tc, spec, server.URL), nil
		}),
		Secrets: SecretResolverFunc(func(context.Context, tenant.TenantContext, string) (string, error) { return "secret", nil }),
	}
	provider, err := factory.Build(context.Background(), tc, spec)
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Complete(context.Background(), ProviderRequest{Messages: []Message{{Role: "user", Content: "hello"}}})
	if err == nil || !errors.Is(err, ErrProviderFailure) {
		t.Fatalf("expected provider failure, got %v", err)
	}
	var responseErr *ProviderResponseError
	if !errors.As(err, &responseErr) || responseErr.Type == "" || responseErr.Code != sentinelCode || responseErr.Message != "busy" {
		t.Fatalf("response error chain lost: %T %+v", responseErr, err)
	}
}

func TestOpenAIProviderFactoryRejectsCrossTenantConfig(t *testing.T) {
	tc := validContext()
	spec := validSpec()
	spec.ModelProvider = "openai"
	spec.ModelConfigRef = "ref-a"
	factory := OpenAIProviderFactory{
		Configs: ModelConfigResolverFunc(func(context.Context, tenant.TenantContext, AgentSpec) (ModelConfig, error) {
			config := openAIConfig(tc, spec, "http://127.0.0.1")
			config.TenantID = "tenant-b"
			return config, nil
		}),
		Secrets: SecretResolverFunc(func(context.Context, tenant.TenantContext, string) (string, error) { return "secret", nil }),
	}
	if _, err := factory.Build(context.Background(), tc, spec); err == nil {
		t.Fatal("expected cross-tenant config to fail closed")
	}
}
