package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
)

func TestBuildModelMock(t *testing.T) {
	selectedModel, err := BuildModel(config.ModelConfig{
		Provider: config.ModelProviderMock,
	})
	if err != nil {
		t.Fatalf("build mock model: %v", err)
	}
	if selectedModel.Info().Name != tutorialModelName {
		t.Fatalf("model name = %q", selectedModel.Info().Name)
	}
}

func TestBuildModelOpenAI(t *testing.T) {
	selectedModel, err := BuildModel(config.ModelConfig{
		Provider: config.ModelProviderOpenAI,
		Name:     "example-model",
		BaseURL:  "https://example.com/v1",
		APIKey:   "test-secret",
	})
	if err != nil {
		t.Fatalf("build OpenAI model: %v", err)
	}
	if selectedModel.Info().Name != "example-model" {
		t.Fatalf("model name = %q", selectedModel.Info().Name)
	}
}

func TestBuildModelRejectsInvalidConfig(t *testing.T) {
	tests := []struct {
		name      string
		config    config.ModelConfig
		wantError string
	}{
		{
			name:      "unknown provider",
			config:    config.ModelConfig{Provider: "unknown"},
			wantError: "unsupported",
		},
		{
			name: "missing model name",
			config: config.ModelConfig{
				Provider: config.ModelProviderOpenAI,
				APIKey:   "test-secret",
			},
			wantError: "model name",
		},
		{
			name: "missing API key",
			config: config.ModelConfig{
				Provider: config.ModelProviderOpenAI,
				Name:     "example-model",
			},
			wantError: "API key",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := BuildModel(test.config)
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Fatalf("error = %v, want substring %q", err, test.wantError)
			}
		})
	}
}

func TestOpenAICompatibleModelRunsThroughRunner(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("request path = %q", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-secret" {
			t.Errorf("Authorization = %q", got)
		}
		var request struct {
			Model    string `json:"model"`
			Messages []any  `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode model request: %v", err)
		}
		if request.Model != "example-model" || len(request.Messages) == 0 {
			t.Errorf("unexpected model request: %+v", request)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":      "chatcmpl-test",
			"object":  "chat.completion",
			"created": 1,
			"model":   "example-model",
			"choices": []map[string]any{{
				"index": 0,
				"message": map[string]any{
					"role":    "assistant",
					"content": "OpenAI-compatible adapter reply",
				},
				"finish_reason": "stop",
			}},
			"usage": map[string]any{
				"prompt_tokens":     5,
				"completion_tokens": 4,
				"total_tokens":      9,
			},
		})
	}))
	t.Cleanup(server.Close)

	selectedModel, err := BuildModel(config.ModelConfig{
		Provider: config.ModelProviderOpenAI,
		Name:     "example-model",
		BaseURL:  server.URL + "/v1",
		APIKey:   "test-secret",
	})
	if err != nil {
		t.Fatalf("build OpenAI-compatible model: %v", err)
	}
	runtime, err := NewRuntime(selectedModel, false)
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(); err != nil {
			t.Fatalf("close runtime: %v", err)
		}
	})

	result, err := runtime.Chat(context.Background(), "alice", "openai-test", "hello")
	if err != nil {
		t.Fatalf("chat through OpenAI-compatible model: %v", err)
	}
	if result.Reply != "OpenAI-compatible adapter reply" {
		t.Fatalf("reply = %q", result.Reply)
	}
}
