package trpcagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func testRequest(endpoint string) Request {
	return Request{TenantID: "tenant-a", SessionID: "session-a", RunID: "run-a", AttemptID: "attempt-a", NodeID: "root", Instruction: "You are a helpful assistant.", InputText: "Hello", Model: Model{Endpoint: endpoint + "/v1", Name: "fixture-model", APIKey: "fixture-token"}, MaxOutputTokens: 20000}
}
func testExecutor() Executor { return Executor{CapacityBytes: 1024 * 1024, DrainTimeout: time.Second} }
func writeAnswer(w http.ResponseWriter, text string) {
	w.Header().Set("Content-Type", "text/event-stream")
	body, _ := json.Marshal(map[string]any{"id": "test-response", "object": "chat.completion.chunk", "created": 1, "model": "fixture-model", "choices": []any{map[string]any{"index": 0, "delta": map[string]string{"role": "assistant", "content": text}, "finish_reason": nil}}})
	fmt.Fprintf(w, "data: %s\n\n", body)
	fmt.Fprint(w, "data: {\"id\":\"test-response\",\"object\":\"chat.completion.chunk\",\"created\":1,\"model\":\"fixture-model\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":25000,\"completion_tokens\":20000,\"total_tokens\":45000}}\n\ndata: [DONE]\n\n")
}
func TestExecutorRealSDKGenerationAndAcceptedHistory(t *testing.T) {
	var calls atomic.Int32
	var requests []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("path=%s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer fixture-token" {
			t.Error("missing attempt credential")
		}
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		requests = append(requests, request)
		if calls.Add(1) == 1 {
			writeAnswer(w, "First answer")
		} else {
			writeAnswer(w, "Second answer")
		}
	}))
	defer server.Close()
	req := testRequest(server.URL)
	first, err := testExecutor().Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if first.FinalText != "First answer" || len(first.Snapshot) == 0 {
		t.Fatalf("result=%+v", first)
	}
	if first.Usage.OutputTokens != 20000 {
		t.Fatalf("usage=%+v", first.Usage)
	}
	req.RunID = "run-b"
	req.AttemptID = "attempt-b"
	req.InputText = "Continue"
	req.AcceptedSnapshot = first.Snapshot
	explicit := int64(18000)
	temp := 0.0
	req.Model.MaxOutputTokens = &explicit
	req.Model.Temperature = &temp
	second, err := testExecutor().Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if second.FinalText != "Second answer" {
		t.Fatal(second.FinalText)
	}
	for i, want := range []float64{20000, 18000} {
		// The SDK's pinned OpenAI variant maps GenerationConfig.MaxTokens to
		// max_completion_tokens. No cumulative 16,384 cap is introduced.
		if requests[i]["max_completion_tokens"] != want {
			t.Fatalf("request %d generation=%v", i, requests[i])
		}
		if requests[i]["stream"] != true {
			t.Fatal("expected provider streaming")
		}
		if tools, ok := requests[i]["tools"]; ok && tools != nil {
			t.Fatalf("undeclared tools=%v", tools)
		}
	}
	if _, exists := requests[0]["temperature"]; exists {
		t.Fatal("omitted temperature gained a default")
	}
	if requests[1]["temperature"] != float64(0) {
		t.Fatal("explicit temperature=0 lost")
	}
	messages, _ := json.Marshal(requests[1]["messages"])
	if !strings.Contains(string(messages), "First answer") || !strings.Contains(string(messages), "Hello") || !strings.Contains(string(messages), "Continue") {
		t.Fatalf("history=%s", messages)
	}
	if !strings.Contains(string(messages), `"role":"assistant"`) {
		t.Fatalf("assistant role rewritten: %s", messages)
	}
	var saved snapshot
	if err = json.Unmarshal(second.Snapshot, &saved); err != nil {
		t.Fatal(err)
	}
	if len(saved.Session.Summaries) != 0 || len(saved.Session.Events) < 4 {
		t.Fatalf("snapshot=%s", second.Snapshot)
	}
	if calls.Load() != 2 {
		t.Fatalf("unexpected model calls=%d", calls.Load())
	}
}
func TestExecutorStickyAppendErrorEvenAfterSDKFinal(t *testing.T) {
	for _, phase := range []string{"assistant", "completion"} {
		t.Run(phase, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { writeAnswer(w, "answer") }))
			defer server.Close()
			e := testExecutor()
			var triggered bool
			e.beforeAppend = func(evt *event.Event) error {
				if phase == "completion" && evt.IsRunnerCompletion() || phase == "assistant" && evt.Response != nil && len(evt.Choices) > 0 && evt.Choices[0].Message.Role == model.RoleAssistant {
					triggered = true
					return errors.New("injected persistence failure")
				}
				return nil
			}
			result, err := e.Execute(context.Background(), testRequest(server.URL))
			if !triggered || !errors.Is(err, ErrOverlay) || result.FinalText != "" || result.Snapshot != nil {
				t.Fatalf("triggered=%t result=%+v err=%v", triggered, result, err)
			}
		})
	}
}
func TestExecutorCancellationAndNoProviderRetry(t *testing.T) {
	t.Run("cancel propagates", func(t *testing.T) {
		started := make(chan struct{})
		canceled := make(chan struct{})
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			close(started)
			<-r.Context().Done()
			close(canceled)
		}))
		defer server.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		resultCh := make(chan error, 1)
		go func() { _, err := testExecutor().Execute(ctx, testRequest(server.URL)); resultCh <- err }()
		<-started
		cancel()
		select {
		case err := <-resultCh:
			if !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("SDK cancellation unbounded")
		}
		select {
		case <-canceled:
		case <-time.After(time.Second):
			t.Fatal("provider request detached")
		}
	})
	t.Run("no provider retry", func(t *testing.T) {
		var calls atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(429)
			fmt.Fprint(w, `{"error":{"message":"rate limit","type":"rate_limit_error"}}`)
		}))
		defer server.Close()
		result, err := testExecutor().Execute(context.Background(), testRequest(server.URL))
		if !errors.Is(err, ErrRetryableModel) || result.Snapshot != nil || calls.Load() != 1 {
			t.Fatalf("calls=%d result=%+v err=%v", calls.Load(), result, err)
		}
	})
}
func TestExecutorCapacityAndRejectedInput(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); writeAnswer(w, "answer") }))
	defer server.Close()
	for _, change := range []func(*Request){func(r *Request) { v := int64(20001); r.Model.MaxOutputTokens = &v }, func(r *Request) { r.AcceptedSnapshot = []byte(`{"version":"other"}`) }, func(r *Request) { r.Model.Endpoint = "https://model.invalid/v1?api_key=secret" }} {
		req := testRequest(server.URL)
		change(&req)
		if _, err := testExecutor().Execute(context.Background(), req); err == nil {
			t.Fatal("invalid request passed")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid input reached model")
	}
	e := testExecutor()
	e.CapacityBytes = 20
	if _, err := e.Execute(context.Background(), testRequest(server.URL)); err == nil {
		t.Fatal("capacity was silently ignored")
	}
}
func TestOverlayStateIsolationStickyErrorsAndNoSummaryTasks(t *testing.T) {
	s, err := newOverlay("tenant-a", "session-a", nil, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.UpdateSessionState(context.Background(), s.key, session.StateMap{"answer": []byte("value")}); err != nil {
		t.Fatal(err)
	}
	body, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	other, err := newOverlay("tenant-a", "session-a", body, 1024*1024)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = newOverlay("tenant-b", "session-a", body, 1024*1024); !errors.Is(err, ErrSnapshot) {
		t.Fatalf("tenant mismatch=%v", err)
	}
	if err = other.UpdateSessionState(context.Background(), other.key, session.StateMap{"answer": []byte("changed")}); err != nil {
		t.Fatal(err)
	}
	again, _ := s.Snapshot()
	if string(body) != string(again) {
		t.Fatal("attempt state leaked")
	}
	if err = s.EnqueueSummaryJob(context.Background(), s.stored, "", true); err != nil {
		t.Fatal(err)
	}
	if text, ok := s.GetSessionSummaryText(context.Background(), s.stored); text != "" || ok {
		t.Fatal("summary enabled")
	}
	if err = s.UpdateAppState(context.Background(), s.key.AppName, session.StateMap{"a": []byte("b")}); err == nil {
		t.Fatal("app write permitted")
	}
	if _, err = s.Snapshot(); !errors.Is(err, ErrOverlay) {
		t.Fatalf("sticky write failure=%v", err)
	}
}

func TestExecutorDoesNotInheritProcessCredentialsOrExecuteCode(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "process-key-must-not-leak")
	t.Setenv("OPENAI_ORG_ID", "process-org-must-not-leak")
	t.Setenv("OPENAI_PROJECT_ID", "process-project-must-not-leak")
	var calls atomic.Int32
	codeText := "```python\nprint('must remain text')\n```"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		for _, header := range []string{"Authorization", "OpenAI-Organization", "OpenAI-Project"} {
			if value := r.Header.Get(header); value != "" {
				t.Errorf("ambient %s=%q", header, value)
			}
		}
		writeAnswer(w, codeText)
	}))
	defer server.Close()
	req := testRequest(server.URL)
	req.Model.APIKey = ""
	result, err := testExecutor().Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if result.FinalText != codeText || calls.Load() != 1 {
		t.Fatalf("code transformed or caused extra call: result=%+v calls=%d", result, calls.Load())
	}
}

func TestExecutorLateStreamErrorDoesNotAcceptPartialFinal(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"id\":\"test-response\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"partial text\"}}]}\n\n")
		fmt.Fprint(w, "data: {\"error\":{\"message\":\"stream failed\",\"type\":\"server_error\"}}\n\n")
	}))
	defer server.Close()
	result, err := testExecutor().Execute(context.Background(), testRequest(server.URL))
	if err == nil || result.FinalText != "" || result.Snapshot != nil {
		t.Fatalf("late stream error accepted: result=%+v err=%v", result, err)
	}
}

func TestExecutorUndeclaredToolResponseNeverBecomesFinal(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"id\":\"test-response\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"tool_calls\":[{\"index\":0,\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"run_code\",\"arguments\":\"{}\"}}]}}]}\n\ndata: {\"id\":\"test-response\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\ndata: [DONE]\n\n")
	}))
	defer server.Close()
	result, err := testExecutor().Execute(context.Background(), testRequest(server.URL))
	if err == nil || result.Snapshot != nil || calls.Load() != 1 {
		t.Fatalf("undeclared tool response result=%+v calls=%d err=%v", result, calls.Load(), err)
	}
}

func TestExecutorRejectsInvalidReplyBeforeReturningSnapshot(t *testing.T) {
	for name, text := range map[string]string{"over-byte-boundary": strings.Repeat("a", 65537), "nul": "answer\x00invalid", "exact-byte-boundary": strings.Repeat("😀", 16384)} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { writeAnswer(w, text) }))
			defer server.Close()
			result, err := testExecutor().Execute(context.Background(), testRequest(server.URL))
			if name == "exact-byte-boundary" {
				if err != nil || result.FinalText != text || len(result.Snapshot) == 0 {
					t.Fatalf("existing wire boundary rejected: %v", err)
				}
				return
			}
			if !errors.Is(err, ErrFinal) || result.Snapshot != nil || result.FinalText != "" {
				t.Fatalf("bad Final reached staging: err=%v snapshot-bytes=%d", err, len(result.Snapshot))
			}
		})
	}
}
