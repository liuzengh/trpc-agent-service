package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunChecksOpenAICompatibleModel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("authorization header was not set")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{
            "id":"check-1","object":"chat.completion","created":1,
            "model":"test-model","choices":[{"index":0,"message":{"role":"assistant","content":"OK"},"finish_reason":"stop"}],
            "usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}
        }`)
	}))
	t.Cleanup(server.Close)

	t.Setenv("TRPC_AGENT_MODEL_PROVIDER", "openai")
	t.Setenv("TRPC_AGENT_MODEL_NAME", "test-model")
	t.Setenv("OPENAI_API_KEY", "test-key")
	t.Setenv("OPENAI_BASE_URL", server.URL+"/v1")
	t.Setenv("TRPC_AGENT_MODEL_STREAM", "false")

	var output bytes.Buffer
	if err := run([]string{"-env-file=", "-timeout=2s"}, &output); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(output.String(), "model check passed") ||
		!strings.Contains(output.String(), "reply: OK") ||
		strings.Contains(output.String(), "test-key") {
		t.Fatalf("output = %q", output.String())
	}
}

func TestRunRejectsMockProvider(t *testing.T) {
	t.Setenv("TRPC_AGENT_MODEL_PROVIDER", "mock")
	t.Setenv("TRPC_AGENT_MODEL_NAME", "")
	t.Setenv("OPENAI_API_KEY", "")
	t.Setenv("OPENAI_BASE_URL", "")
	t.Setenv("TRPC_AGENT_MODEL_STREAM", "false")

	err := run([]string{"-env-file="}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "must be \"openai\"") {
		t.Fatalf("error = %v", err)
	}
}

func TestRunDoesNotExposeAPIKeyInProviderError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error": map[string]any{
				"message": "invalid token",
				"type":    "authentication_error",
			},
		})
	}))
	t.Cleanup(server.Close)

	t.Setenv("TRPC_AGENT_MODEL_PROVIDER", "openai")
	t.Setenv("TRPC_AGENT_MODEL_NAME", "test-model")
	t.Setenv("OPENAI_API_KEY", "never-print-this-key")
	t.Setenv("OPENAI_BASE_URL", server.URL+"/v1")
	t.Setenv("TRPC_AGENT_MODEL_STREAM", "false")

	err := run([]string{"-env-file=", "-timeout=2s"}, io.Discard)
	if err == nil || strings.Contains(err.Error(), "never-print-this-key") {
		t.Fatalf("error = %v", err)
	}
}
