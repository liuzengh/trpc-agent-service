package modelprovider

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestDeepSeekOpenAICompatibleRequest(t *testing.T) {
	const testKey = "test-provider-secret"
	t.Setenv("TEST_DEEPSEEK_API_KEY", testKey)
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if !strings.HasSuffix(request.URL.Path, "/chat/completions") {
			return response(http.StatusNotFound, `{"error":"not found"}`), nil
		}
		if request.Header.Get("Authorization") != "Bearer "+testKey {
			return response(http.StatusUnauthorized, `{"error":"unauthorized"}`), nil
		}
		var body struct {
			Model    string `json:"model"`
			Messages []any  `json:"messages"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil || body.Model != "deepseek-test" || len(body.Messages) == 0 {
			return response(http.StatusBadRequest, `{"error":"bad request"}`), nil
		}
		return response(http.StatusOK, `{"id":"test-response","object":"chat.completion","created":1,"model":"deepseek-test","choices":[{"index":0,"message":{"role":"assistant","content":"pong"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`), nil
	})

	client, err := newOpenAICompatible(tenant.ModelProfile{Provider: ProviderDeepSeek, Name: "deepseek-test", BaseURL: "https://models.example.test", APIKey: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: "TEST_DEEPSEEK_API_KEY"}}, transport)
	if err != nil {
		t.Fatal(err)
	}
	responses, err := client.GenerateContent(context.Background(), &model.Request{Messages: []model.Message{model.NewUserMessage("ping")}})
	if err != nil {
		t.Fatal(err)
	}
	var reply string
	for response := range responses {
		if response.Error != nil {
			t.Fatalf("provider response error: %v", response.Error)
		}
		for _, choice := range response.Choices {
			reply += choice.Message.Content
		}
	}
	if reply != "pong" {
		t.Fatalf("reply=%q", reply)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func response(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d", status), Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}
}

func TestCredentialResolutionErrorIsRedacted(t *testing.T) {
	const lookupKey = "DO_NOT_LEAK_THIS_LOOKUP_KEY"
	_, err := New(tenant.ModelProfile{Provider: ProviderDeepSeek, Name: "deepseek-test", APIKey: tenant.SecretRef{Provider: tenant.SecretProviderEnv, Key: lookupKey}})
	if err == nil || strings.Contains(err.Error(), lookupKey) {
		t.Fatalf("error leaked credential metadata: %v", err)
	}
}

func TestScopedModelRejectsCrossTenantSecretBeforeResolution(t *testing.T) {
	ref := tenant.SecretRef{Provider: tenant.SecretProviderVault, Key: "tenant-b/app-a/model"}
	_, err := NewScoped(tenant.ModelProfile{Provider: ProviderDeepSeek, Name: "deepseek-test", APIKey: ref}, "tenant-a", "app-a")
	if err == nil || strings.Contains(err.Error(), ref.Key) {
		t.Fatalf("scoped model error = %v", err)
	}
}
