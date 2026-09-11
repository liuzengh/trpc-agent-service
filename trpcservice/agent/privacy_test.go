package agent

import (
	"context"
	"github.com/cyl6/trpc-agent-service/trpcservice/budget"
	"github.com/cyl6/trpc-agent-service/trpcservice/config"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestConfiguredModelPrivacyBeforeDispatchWithAndWithoutBudget(t *testing.T) {
	const fixtureCredential = "fixture-provider-credential"
	t.Setenv("MODEL_PRIVACY_KEY", fixtureCredential)
	var requests atomic.Int32
	bodies := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		body, _ := io.ReadAll(r.Body)
		bodies <- string(body)
		if r.Header.Get("Authorization") != "Bearer "+fixtureCredential {
			t.Error("provider authentication header changed")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"response","object":"chat.completion","model":"fixture","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}`))
	}))
	defer server.Close()
	for _, withBudget := range []bool{false, true} {
		tenant := config.TenantConfig{TenantID: "privacy-tenant", App: config.AppConfig{Name: "app"}, Model: config.ModelConfig{Provider: "openai", Name: "fixture", BaseURL: server.URL + "/v1", APIKeyEnv: "MODEL_PRIVACY_KEY", MaxTokens: 100}, Privacy: config.PrivacyPolicy{Input: "redact"}}
		var ledger budget.Ledger
		if withBudget {
			ledger = budget.NewMemory()
		}
		configured, err := buildConfiguredModelWithBudget(tenant, ledger)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		stream, err := configured.GenerateContent(ctx, model.NewRequest([]model.Message{model.NewUserMessage("alice@example.com " + fixtureCredential)}))
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		for response := range stream {
			if response.Error != nil {
				t.Fatal(response.Error)
			}
		}
		cancel()
		body := <-bodies
		if strings.Contains(body, "alice@example.com") || strings.Contains(body, fixtureCredential) {
			t.Fatal("sensitive content reached real model adapter")
		}
		tenant.Privacy.Input = "block"
		configured, err = buildConfiguredModelWithBudget(tenant, ledger)
		if err != nil {
			t.Fatal(err)
		}
		before := requests.Load()
		stream, err = configured.GenerateContent(context.Background(), model.NewRequest([]model.Message{model.NewUserMessage("alice@example.com")}))
		if err == nil || stream != nil || requests.Load() != before {
			t.Fatal("blocked request dispatched")
		}
	}
}
