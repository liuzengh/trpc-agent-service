package main

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	agentlog "trpc.group/trpc-go/trpc-agent-go/log"
)

func checkConfig(t *testing.T, base string) {
	t.Helper()
	t.Setenv("TRPC_AGENT_EMBEDDING_MODEL", "embedding-test")
	t.Setenv("TRPC_AGENT_EMBEDDING_BASE_URL", base)
	t.Setenv("TRPC_AGENT_EMBEDDING_API_KEY", "embedding-private-canary")
	t.Setenv("TRPC_AGENT_EMBEDDING_DIMENSIONS", "3")
	t.Setenv("OPENAI_API_KEY", "chat-private-canary")
	t.Setenv("OPENAI_BASE_URL", "http://127.0.0.1:1/do-not-use")
	t.Setenv("OPENAI_ORG_ID", "organization-private-canary")
	t.Setenv("OPENAI_PROJECT_ID", "project-private-canary")
}

func TestCheckUsesFrameworkEmbedderAndDedicatedCredentials(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "POST" || r.URL.Path != "/v1/embeddings" {
			t.Errorf("wrong method or endpoint")
		}
		if r.Header.Get("Authorization") != "Bearer embedding-private-canary" || r.Header.Get("OpenAI-Organization") != "" || r.Header.Get("OpenAI-Project") != "" {
			t.Error("credentials were inherited or missing")
		}
		var input struct {
			Model      string `json:"model"`
			Input      string `json:"input"`
			Dimensions int    `json:"dimensions"`
			Encoding   string `json:"encoding_format"`
		}
		if json.NewDecoder(r.Body).Decode(&input) != nil || input.Model != "embedding-test" || input.Input != sampleText || input.Dimensions != 3 || input.Encoding != "float" {
			t.Error("framework embedding request shape differs")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"object":"list","model":"embedding-test","data":[{"object":"embedding","index":0,"embedding":[0.1,0.2,0.3]}],"usage":{"prompt_tokens":3,"total_tokens":3}}`)
	}))
	defer server.Close()
	checkConfig(t, server.URL+"/v1")
	var output bytes.Buffer
	if err := run([]string{"-env-file=", "-timeout=2s"}, &output); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || !strings.Contains(output.String(), "embedding check passed") || strings.Contains(output.String(), "canary") || strings.Contains(output.String(), server.URL) {
		t.Fatal("unexpected output or request count")
	}
}

func TestCheckFailuresAreBoundedPrivateAndNotRetried(t *testing.T) {
	for _, tc := range []struct {
		name, body, want string
		status           int
	}{
		{"unauthorized", `{"error":{"message":"embedding-private-canary","type":"authentication_error"}}`, "401/403", 401},
		{"no_endpoint", `{"error":{"message":"embedding-private-canary"}}`, "404", 404},
		{"rate_limit", `{"error":{"message":"embedding-private-canary"}}`, "429", 429},
		{"provider_failure", `{"error":{"message":"embedding-private-canary"}}`, "HTTP 500", 500},
		{"malformed", "embedding-private-canary", "response invalid", 200},
		{"zero", `{"data":[{"index":0,"embedding":[0,0,0]}]}`, "nonzero", 200},
		{"dimensions", `{"data":[{"index":0,"embedding":[0.1,0.2]}]}`, "dimension mismatch", 200},
		{"oversize", strings.Repeat("x", maxResponseBytes+1), "4 MiB", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			checkConfig(t, server.URL+"/v1")
			core, logs := observer.New(zap.DebugLevel)
			original := agentlog.ContextDefault
			agentlog.ContextDefault = zap.New(core).Sugar()
			defer func() { agentlog.ContextDefault = original }()
			var output bytes.Buffer
			err := run([]string{"-env-file=", "-timeout=2s"}, &output)
			if err == nil || !strings.Contains(err.Error(), tc.want) || calls.Load() != 1 {
				t.Fatalf("classification/count mismatch: %v calls=%d", err, calls.Load())
			}
			if strings.Contains(err.Error()+output.String(), "canary") || strings.Contains(err.Error()+output.String(), server.URL) || logs.Len() != 0 {
				t.Fatal("provider failure escaped private diagnostic")
			}
		})
	}
}

func TestCheckRejectsRedirectWithoutForwardingCredentials(t *testing.T) {
	var leaked atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Add(1) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/private-canary", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	checkConfig(t, server.URL+"/v1")
	err := run([]string{"-env-file=", "-timeout=2s"}, io.Discard)
	if err == nil || leaked.Load() != 0 || strings.Contains(err.Error(), "canary") {
		t.Fatal("redirect followed or exposed")
	}
}

func TestCheckTimeoutCancelsRequest(t *testing.T) {
	stop := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case <-r.Context().Done():
		case <-stop:
		}
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(stop) })
	checkConfig(t, server.URL+"/v1")
	err := run([]string{"-env-file=", "-timeout=100ms"}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatal("timeout was not enforced", err)
	}
}

func TestValidateVector(t *testing.T) {
	for _, v := range [][]float64{nil, {0, 0}, {math.NaN(), 1}, {math.Inf(1), 1}, {math.MaxFloat64, math.MaxFloat64}} {
		if validateVector(v, 2) == nil {
			t.Fatal("invalid vector accepted")
		}
	}
	if validateVector([]float64{0.1, 0.2}, 2) != nil {
		t.Fatal("valid vector rejected")
	}
}

func TestInvalidFlagsDoNotEchoValues(t *testing.T) {
	for _, args := range [][]string{{"-unknown=private-canary"}, {"-timeout=private-canary"}, {"private-canary"}, {"-timeout=0"}, {"-timeout=3m"}} {
		err := run(args, io.Discard)
		if err == nil || strings.Contains(err.Error(), "canary") {
			t.Fatal("invalid flags accepted or exposed")
		}
	}
}
