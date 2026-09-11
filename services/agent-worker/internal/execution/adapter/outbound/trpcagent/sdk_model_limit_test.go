package trpcagent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	openaioption "github.com/openai/openai-go/option"
	"trpc.group/trpc-go/trpc-agent-go/model"
	sdkopenai "trpc.group/trpc-go/trpc-agent-go/model/openai"
)

const outputLimitAnswer = "published generation preserved"

// This is a real HTTP/SSE fixture, not a callback mock. Response usage is small
// and explicit; output-budget values describe the request, not reported usage.
func outputLimitServer(t *testing.T, modelName string) (*httptest.Server, <-chan map[string]any) {
	t.Helper()
	requests := make(chan map[string]any, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" {
			t.Errorf("unexpected SDK request: %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer fixture-token" {
			t.Error("attempt credential was not preserved")
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		requests <- body
		writeOutputLimitResponse(t, w, modelName)
	}))
	return server, requests
}

func writeOutputLimitResponse(t *testing.T, w http.ResponseWriter, modelName string) {
	t.Helper()
	w.Header().Set("Content-Type", "text/event-stream")
	for _, chunk := range []map[string]any{
		{"id": "limit-response", "object": "chat.completion.chunk", "created": 1, "model": modelName,
			"choices": []any{map[string]any{"index": 0, "delta": map[string]string{"role": "assistant", "content": outputLimitAnswer}}}},
		{"id": "limit-response", "object": "chat.completion.chunk", "created": 1, "model": modelName,
			"choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": "stop"}},
			"usage":   map[string]int{"prompt_tokens": 37, "completion_tokens": 11, "total_tokens": 48}},
	} {
		raw, err := json.Marshal(chunk)
		if err != nil {
			t.Error(err)
		}
		fmt.Fprintf(w, "data: %s\n\n", raw)
	}
	fmt.Fprint(w, "data: [DONE]\n\n")
}

func assertOutputWire(t *testing.T, requests <-chan map[string]any, modelName string, want int64) map[string]any {
	t.Helper()
	if len(requests) != 1 {
		t.Fatalf("SDK HTTP calls=%d, want one", len(requests))
	}
	actual := <-requests
	if actual["model"] != modelName {
		t.Errorf("published model identity changed: actual=%v want=%s", actual["model"], modelName)
	}
	if actual["max_completion_tokens"] != float64(want) {
		t.Errorf("published output limit changed on real SDK wire: model=%s actual=%v want=%d", modelName, actual["max_completion_tokens"], want)
	}
	if _, exists := actual["max_tokens"]; exists {
		t.Error("OpenAI wire contains duplicate legacy max_tokens")
	}
	if actual["stream"] != true {
		t.Error("published streaming execution changed")
	}
	return actual
}

// WV-40 requires the effective published value to survive SDK conversion.
// Known-model caps in the unadapted SDK are characterization, not Worker policy.
func TestWorkerPreservesPublishedGenerationForKnownSDKModels(t *testing.T) {
	for _, tc := range []struct {
		name      string
		model     string
		published int64
		node      int64
		want      int64
	}{
		{name: "gpt4o-published18000", model: "gpt-4o", published: 18000, want: 18000},
		{name: "gpt4o-published300000-fallback", model: "gpt-4o", published: 300000, want: 300000},
		{name: "gpt4o-explicit18000", model: "gpt-4o", published: 300000, node: 18000, want: 18000},
		{name: "gpt41-published65536", model: "gpt-4.1", published: 65536, want: 65536},
		{name: "gpt41-explicit65536", model: "gpt-4.1", published: 300000, node: 65536, want: 65536},
		{name: "explicit-below-known-cap-is-not-raised", model: "gpt-4o", published: 300000, node: 12000, want: 12000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, requests := outputLimitServer(t, tc.model)
			defer server.Close()
			req := testRequest(server.URL)
			req.Model.Name = tc.model
			req.MaxOutputTokens = tc.published
			if tc.node != 0 {
				req.Model.MaxOutputTokens = &tc.node
			}
			temperature := 0.0
			req.Model.Temperature = &temperature
			frozen, err := json.Marshal(req)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result, err := testExecutor().Execute(ctx, req)
			if err != nil {
				t.Fatalf("actual Worker/pinned-SDK execution: %v", err)
			}
			if result.FinalText != outputLimitAnswer || len(result.Snapshot) == 0 {
				t.Fatal("SDK Final and attempt snapshot did not complete")
			}
			if result.Usage != (Usage{InputTokens: 37, OutputTokens: 11, TotalTokens: 48}) {
				t.Errorf("SDK usage silently altered: %+v", result.Usage)
			}
			actual := assertOutputWire(t, requests, tc.model, tc.want)
			if actual["temperature"] != float64(0) {
				t.Error("explicit generation temperature=0 changed")
			}
			after, err := json.Marshal(req)
			if err != nil || !bytes.Equal(frozen, after) {
				t.Error("same Attempt identity or published parameters mutated")
			}
			if !t.Failed() {
				t.Logf("WORKER_PUBLISHED_GENERATION=PASS sdk=%s model=%s execution=%d node=%d wire=%d one_call=true usage=37/11/48 max_tokens_absent=true same_attempt_unchanged=true", SDKVersion, tc.model, tc.published, tc.node, tc.want)
			}
		})
	}
}

// Keep the pre-adaptation SDK behavior visible independently. This model is
// instantiated directly with SDK public APIs, never through Worker Executor.
func TestUnadaptedSDKKnownModelClampCharacterization(t *testing.T) {
	server, requests := outputLimitServer(t, "gpt-4o")
	defer server.Close()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	native := sdkopenai.New("gpt-4o", sdkopenai.WithAPIKey("fixture-token"),
		sdkopenai.WithVariant(sdkopenai.VariantOpenAI), sdkopenai.WithBaseURL(server.URL+"/v1"),
		sdkopenai.WithOptimizeForCache(false), sdkopenai.WithEnableTokenTailoring(false),
		sdkopenai.WithOpenAIOptions(openaioption.WithMaxRetries(0), openaioption.WithHTTPClient(&http.Client{Transport: transport})))
	max := 18000
	request := &model.Request{Messages: []model.Message{model.NewUserMessage("Hello")},
		GenerationConfig: model.GenerationConfig{MaxTokens: &max, Stream: true}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	responses, err := native.GenerateContent(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	var usage *model.Usage
	var final string
	for response := range responses {
		if response.Error != nil {
			t.Fatal("native SDK returned a model error")
		}
		if response.Usage != nil {
			usage = response.Usage
		}
		if !response.IsPartial {
			for _, choice := range response.Choices {
				if choice.Message.Content != "" {
					final = choice.Message.Content
				}
			}
		}
	}
	if final != outputLimitAnswer || usage == nil || usage.PromptTokens != 37 || usage.CompletionTokens != 11 || usage.TotalTokens != 48 {
		t.Fatal("native SDK Final/usage characterization did not finish")
	}
	assertOutputWire(t, requests, "gpt-4o", 16384)
	if max != 18000 || *request.MaxTokens != 18000 {
		t.Error("characterization mutated the input instead of observing conversion")
	}
	if !t.Failed() {
		t.Logf("UNADAPTED_SDK_CLAMP=PASS sdk=%s model=gpt-4o input=18000 wire=16384; characterization only, not Worker target", SDKVersion)
	}
}

func TestWorkerPublishedLimitProviderRejectionIsNotRetriedOrConvertedToSuccess(t *testing.T) {
	requests := make(chan map[string]any, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		requests <- body
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, `{"error":{"message":"fixture rejects requested output limit","type":"invalid_request_error","code":"context_length_exceeded"}}`)
	}))
	defer server.Close()
	req := testRequest(server.URL)
	req.Model.Name = "gpt-4o"
	req.MaxOutputTokens = 300000
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := testExecutor().Execute(ctx, req)
	if !errors.Is(err, ErrModel) || errors.Is(err, ErrRetryableModel) {
		t.Fatalf("provider 400 must remain a nonretryable model error: %v", err)
	}
	if result.FinalText != "" || len(result.Snapshot) != 0 || result.Usage != (Usage{}) {
		t.Fatal("provider rejection was converted to a successful partial result")
	}
	if strings.Contains(err.Error(), "fixture rejects") {
		t.Error("raw provider error text escaped the typed adapter error")
	}
	assertOutputWire(t, requests, "gpt-4o", 300000)
	if req.MaxOutputTokens != 300000 || req.Model.Name != "gpt-4o" {
		t.Error("provider rejection mutated the published request")
	}
	if !t.Failed() {
		t.Log("WORKER_PROVIDER_REJECTION=PASS actual_HTTP=400 wire=300000 one_call=true no_lowered_retry=true ErrModel=true empty_result=true")
	}
}

func TestWorkerConcurrentPublishedLimitsRemainAttemptLocal(t *testing.T) {
	requests := make(chan map[string]any, 4)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		requests <- body
		entered <- struct{}{}
		select {
		case <-release:
			name, _ := body["model"].(string)
			writeOutputLimitResponse(t, w, name)
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first, second := testRequest(server.URL), testRequest(server.URL)
	first.RunID, first.AttemptID = "run-one", "attempt-one"
	first.Model.Name, first.MaxOutputTokens = "gpt-4o", 300000
	second.RunID, second.AttemptID = "run-two", "attempt-two"
	second.Model.Name, second.MaxOutputTokens = "gpt-4.1", 300000
	node := int64(65536)
	second.Model.MaxOutputTokens = &node
	type execution struct {
		result Result
		err    error
	}
	finished := make(chan execution, 2)
	executor := testExecutor()
	for _, request := range []Request{first, second} {
		go func(request Request) {
			result, err := executor.Execute(ctx, request)
			finished <- execution{result, err}
		}(request)
	}
	for range 2 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("both actual model HTTP requests did not overlap")
		}
	}
	releaseOnce.Do(func() { close(release) })
	for range 2 {
		select {
		case done := <-finished:
			if done.err != nil || done.result.FinalText != outputLimitAnswer || len(done.result.Snapshot) == 0 || done.result.Usage != (Usage{InputTokens: 37, OutputTokens: 11, TotalTokens: 48}) {
				t.Fatalf("concurrent real SDK execution did not finish: %v", done.err)
			}
		case <-ctx.Done():
			t.Fatal("concurrent SDK requests did not finish")
		}
	}
	if len(requests) != 2 {
		t.Fatalf("concurrent HTTP calls=%d, want two", len(requests))
	}
	seen := make(map[string]bool)
	for range 2 {
		actual := <-requests
		name, _ := actual["model"].(string)
		want, exists := map[string]float64{"gpt-4o": 300000, "gpt-4.1": 65536}[name]
		if !exists || seen[name] || actual["max_completion_tokens"] != want {
			t.Fatalf("Attempt-local generation crossed model boundaries: model=%s wire=%v", name, actual["max_completion_tokens"])
		}
		if _, duplicate := actual["max_tokens"]; duplicate {
			t.Error("concurrent request gained duplicate max_tokens")
		}
		seen[name] = true
	}
	if first.MaxOutputTokens != 300000 || second.MaxOutputTokens != 300000 || *second.Model.MaxOutputTokens != 65536 || first.AttemptID != "attempt-one" || second.AttemptID != "attempt-two" {
		t.Fatal("concurrent execution mutated Attempt-local inputs")
	}
	t.Log("WORKER_CONCURRENT_GENERATION=PASS overlapping_actual_HTTP=2 gpt-4o=300000 gpt-4.1=65536 one_call_per_attempt=true no_cross_attempt_mutation=true")
}
