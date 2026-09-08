package embedding

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	agentlog "trpc.group/trpc-go/trpc-agent-go/log"
)

func TestRemoteUsesRealEndpointAndDropsImplicitHeaders(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "chat-secret-canary")
	t.Setenv("OPENAI_BASE_URL", "https://not-used.invalid")
	t.Setenv("OPENAI_PROJECT_ID", "project-canary")
	t.Setenv("OPENAI_ORG_ID", "org-canary")
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/private-endpoint-canary/v1/embeddings" || r.Method != "POST" || r.Header.Get("Authorization") != "Bearer embedding-secret-canary" {
			t.Error("actual endpoint or explicit key missing")
		}
		if r.Header.Get("OpenAI-Organization") != "" || r.Header.Get("OpenAI-Project") != "" || r.Header.Get("Baggage") != "" {
			t.Error("implicit metadata leaked")
		}
		if r.Header.Get("Traceparent") == "" {
			t.Error("trace context not propagated")
		}
		var request struct {
			Input      string `json:"input"`
			Dimensions int    `json:"dimensions"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.Input != "synthetic text" || request.Dimensions != 3 {
			t.Error("framework request shape mismatch")
		}
		_, _ = io.WriteString(w, `{"model":"untrusted-model-canary","data":[{"index":0,"embedding":[0.1,0.2,0.3]}],"usage":{"prompt_tokens":4,"total_tokens":4},"private":"body-canary"}`)
	}))
	defer server.Close()
	client, err := NewRemote(Config{Model: "test-model", BaseURL: server.URL + "/private-endpoint-canary/v1", APIKey: "embedding-secret-canary", Dimensions: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	id, _ := trace.TraceIDFromHex("00112233445566778899aabbccddeeff")
	sid, _ := trace.SpanIDFromHex("0011223344556677")
	ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: id, SpanID: sid, TraceFlags: trace.FlagsSampled}))
	member, _ := baggage.NewMember("private", "baggage-canary")
	bag, _ := baggage.New(member)
	ctx = baggage.ContextWithBaggage(ctx, bag)
	vector, usage, err := client.GetEmbeddingWithUsage(ctx, "synthetic text")
	if err != nil || len(vector) != 3 || usage["prompt_tokens"] != int64(4) || calls.Load() != 1 {
		t.Fatal("embedding request failed", err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetEmbedding(ctx, "synthetic text"); err == nil || calls.Load() != 1 {
		t.Fatal("closed embedder made another request")
	}
}

func TestRemoteRejectsUnsafeResultsBeforeFrameworkLogging(t *testing.T) {
	for _, tc := range []struct {
		name, body, kind string
		status           int
	}{
		{"provider", `{"error":{"message":"unlabelled-secret-canary"}}`, "http", 401},
		{"malformed", "private-body-canary", "response_shape", 200},
		{"dimension", `{"data":[{"embedding":[1,0]}]}`, "dimensions", 200},
		{"zero", `{"data":[{"embedding":[0,0,0]}]}`, "vector", 200},
		{"float32_overflow", `{"data":[{"embedding":[1e100,0,0]}]}`, "vector", 200},
		{"float32_underflow", `{"data":[{"embedding":[1e-100,0,0]}]}`, "vector", 200},
		{"multiple", `{"data":[{"embedding":[1,0,0]},{"embedding":[1,0,0]}]}`, "response_shape", 200},
		{"usage", `{"data":[{"embedding":[1,0,0]}],"usage":{"prompt_tokens":-1}}`, "usage", 200},
		{"large", strings.Repeat("x", MaxResponseBytes+1), "response_limit", 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			client, err := NewRemote(Config{Model: "test-model", BaseURL: server.URL + "/private-path-canary", APIKey: "key-canary", Dimensions: 3})
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			core, logs := observer.New(zap.DebugLevel)
			previous := agentlog.ContextDefault
			agentlog.ContextDefault = zap.New(core).Sugar()
			defer func() { agentlog.ContextDefault = previous }()
			_, err = client.GetEmbedding(context.Background(), "private-input-canary")
			var provider *ProviderError
			if !errors.As(err, &provider) || provider.Kind != tc.kind || calls.Load() != 1 {
				t.Fatal("unexpected failure or retry", err)
			}
			raw, _ := json.Marshal(logs.All())
			if strings.Contains(string(raw)+err.Error(), "canary") || strings.Contains(string(raw)+err.Error(), server.URL) {
				t.Fatal("provider data or endpoint escaped into framework logs")
			}
		})
	}
}

func TestRemoteRedirectAndCancellationDoNotRetry(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	client, _ := NewRemote(Config{Model: "test", BaseURL: server.URL, APIKey: "key", Dimensions: 3})
	defer client.Close()
	if _, err := client.GetEmbedding(context.Background(), "test"); err == nil || redirected.Load() != 0 {
		t.Fatal("redirect followed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.GetEmbedding(ctx, "test"); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation not retained", err)
	}
}

func TestRemoteConcurrentCallsAndClose(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"data":[{"embedding":[1,0,0]}]}`)
	}))
	defer server.Close()
	client, _ := NewRemote(Config{Model: "test", BaseURL: server.URL, APIKey: "key", Dimensions: 3})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := client.GetEmbedding(ctx, "test"); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
}
