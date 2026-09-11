package trpcagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestExecutorSummaryFixedModelAndNextRun(t *testing.T) {
	var primaryCalls, summaryCalls atomic.Int32
	var consumed atomic.Bool
	var disabledConsumed atomic.Bool
	var disabledHistory atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		if body["max_completion_tokens"] != float64(20000) {
			t.Errorf("published limit=%v", body["max_completion_tokens"])
		}
		if body["model"] == "summary-model" {
			summaryCalls.Add(1)
			if r.Header.Get("Authorization") != "Bearer summary-token" {
				t.Error("wrong summary credential")
			}
			if body["stream"] == true {
				t.Error("streaming summary")
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprint(w, `{"id":"summary","object":"chat.completion","model":"summary-model","choices":[{"index":0,"message":{"role":"assistant","content":"The secret remembered preference is green tea."},"finish_reason":"stop"}],"usage":{"prompt_tokens":11,"completion_tokens":7,"total_tokens":18}}`)
			return
		}
		if r.Header.Get("Authorization") != "Bearer fixture-token" {
			t.Error("wrong primary credential")
		}
		call := primaryCalls.Add(1)
		if call == 4 {
			messages, _ := json.Marshal(body["messages"])
			disabledConsumed.Store(strings.Contains(string(messages), "The secret remembered preference is green tea."))
			disabledHistory.Store(strings.Contains(string(messages), "Hello") && strings.Contains(string(messages), "A normal answer"))
		}
		if call == 3 || call == 5 {
			messages, _ := json.Marshal(body["messages"])
			consumed.Store(strings.Contains(string(messages), "The secret remembered preference is green tea."))
		}
		writeAnswer(w, "A normal answer")
	}))
	defer server.Close()
	req := testRequest(server.URL)
	seed, err := testExecutor().Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	req.AcceptedSnapshot = seed.Snapshot
	req.RunID = "summary-run"
	req.AttemptID = "summary-attempt"
	req.Summary = &SummaryConfig{Model: Model{Endpoint: server.URL + "/v1", Name: "summary-model", APIKey: "summary-token"}, EventThreshold: 1, AddSessionSummary: true}
	first, err := testExecutor().Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if summaryCalls.Load() != 1 {
		t.Fatalf("summary calls=%d", summaryCalls.Load())
	}
	if first.Usage != (Usage{InputTokens: 25011, OutputTokens: 20007, TotalTokens: 45018}) {
		t.Fatalf("combined usage=%+v", first.Usage)
	}
	var stored snapshot
	if err = json.Unmarshal(first.Snapshot, &stored); err != nil {
		t.Fatal(err)
	}
	if len(stored.Session.Summaries) != 1 {
		t.Fatalf("summaries=%v", stored.Session.Summaries)
	}
	req.AcceptedSnapshot = first.Snapshot
	req.RunID = "run-b"
	req.AttemptID = "attempt-b"
	req.InputText = "Continue"
	second, err := testExecutor().Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !consumed.Load() {
		t.Fatal("next run did not consume accepted summary")
	}
	before := summaryCalls.Load()
	req.AcceptedSnapshot = second.Snapshot
	enabled := req.Summary
	req.Summary = nil
	req.RunID = "summary-disabled-run"
	req.AttemptID = "summary-disabled-attempt"
	off, err := testExecutor().Execute(context.Background(), req)
	if err != nil || len(off.Snapshot) == 0 {
		t.Fatalf("disabling summary invalidated existing Session: %v", err)
	}
	if summaryCalls.Load() != before || disabledConsumed.Load() || !disabledHistory.Load() {
		t.Fatal("disabled summary still generated or consumed")
	}
	if err = json.Unmarshal(off.Snapshot, &stored); err != nil || len(stored.Session.Summaries) != 1 {
		t.Fatal("disabling summary destroyed accepted metadata", err)
	}
	consumed.Store(false)
	req.AcceptedSnapshot = off.Snapshot
	req.Summary = enabled
	req.RunID = "summary-reenabled-run"
	req.AttemptID = "summary-reenabled-attempt"
	resumed, err := testExecutor().Execute(context.Background(), req)
	if err != nil || len(resumed.Snapshot) == 0 || !consumed.Load() {
		t.Fatalf("reenabling summary lost continuity: %v", err)
	}
}

func TestExecutorSummaryFailureProducesNoCandidate(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["model"] == "summary-model" {
			calls.Add(1)
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, `{"error":{"message":"provider-private-secret","type":"server_error"}}`)
			return
		}
		writeAnswer(w, "Do not accept this answer")
	}))
	defer server.Close()
	req := testRequest(server.URL)
	seed, err := testExecutor().Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	req.AcceptedSnapshot = seed.Snapshot
	req.RunID = "summary-run"
	req.AttemptID = "summary-attempt"
	req.Summary = &SummaryConfig{Model: Model{Endpoint: server.URL + "/v1", Name: "summary-model"}, EventThreshold: 1, AddSessionSummary: true}
	result, err := testExecutor().Execute(context.Background(), req)
	if err == nil || len(result.Snapshot) != 0 || result.FinalText != "" {
		t.Fatalf("failure snapshotbytes=%d err=%v", len(result.Snapshot), err)
	}
	if !errors.Is(err, ErrRetryableModel) {
		t.Fatalf("summary provider misclassified: %v", err)
	}
	if strings.Contains(err.Error(), "provider-private-secret") {
		t.Fatal("provider body escaped")
	}
	if calls.Load() != 1 {
		t.Fatalf("unexpected retries=%d", calls.Load())
	}
}

func TestExecutorSummaryCancellationProducesNoCandidate(t *testing.T) {
	entered := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["model"] == "summary-model" {
			close(entered)
			<-r.Context().Done()
			return
		}
		writeAnswer(w, "answer")
	}))
	defer server.Close()
	req := testRequest(server.URL)
	seed, err := testExecutor().Execute(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	req.AcceptedSnapshot = seed.Snapshot
	req.RunID = "summary-run"
	req.AttemptID = "summary-attempt"
	req.Summary = &SummaryConfig{Model: Model{Endpoint: server.URL + "/v1", Name: "summary-model"}, EventThreshold: 1, AddSessionSummary: true}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		result, err := testExecutor().Execute(ctx, req)
		if len(result.Snapshot) != 0 || result.FinalText != "" {
			done <- fmt.Errorf("cancel produced candidate")
			return
		}
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("summary did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("summary did not join cancellation")
	}
}

type doneBeforeCloseModel struct{ responses chan *model.Response }

func (m *doneBeforeCloseModel) Info() model.Info { return model.Info{Name: "fixture"} }
func (m *doneBeforeCloseModel) GenerateContent(context.Context, *model.Request) (<-chan *model.Response, error) {
	return m.responses, nil
}
func TestSummaryUsageWaitsForOwnedModelClose(t *testing.T) {
	base := &doneBeforeCloseModel{responses: make(chan *model.Response)}
	m := &summaryUsageModel{Model: base}
	done := make(chan error, 1)
	go func() { _, err := m.GenerateContent(context.Background(), &model.Request{}); done <- err }()
	base.responses <- &model.Response{Done: true, Usage: &model.Usage{PromptTokens: 11, CompletionTokens: 7, TotalTokens: 18}}
	select {
	case <-done:
		t.Fatal("Done released model before channel close")
	case <-time.After(100 * time.Millisecond):
	}
	close(base.responses)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("model did not finish")
	}
	if got := m.usage(); got != (Usage{InputTokens: 11, OutputTokens: 7, TotalTokens: 18}) {
		t.Fatalf("usage=%+v", got)
	}
}

func TestExecutorSummaryRejectsInvalidFixedConfigBeforeHTTP(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); writeAnswer(w, "unexpected") }))
	defer server.Close()
	for _, tc := range []struct {
		name string
		edit func(*SummaryConfig)
	}{
		{"endpoint query", func(s *SummaryConfig) { s.Model.Endpoint += "?" }},
		{"endpoint fragment", func(s *SummaryConfig) { s.Model.Endpoint += "#" }},
		{"blank model", func(s *SummaryConfig) { s.Model.Name = " " }},
		{"zero threshold", func(s *SummaryConfig) { s.EventThreshold = 0 }},
		{"imprecise threshold", func(s *SummaryConfig) { s.EventThreshold = 9007199254740992 }},
		{"private output cap", func(s *SummaryConfig) { n := int64(16384); s.Model.MaxOutputTokens = &n }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := testRequest(server.URL)
			req.Summary = &SummaryConfig{Model: Model{Endpoint: server.URL + "/v1", Name: "summary-model"}, EventThreshold: 1}
			tc.edit(req.Summary)
			result, err := testExecutor().Execute(context.Background(), req)
			if err == nil || len(result.Snapshot) != 0 {
				t.Fatal("invalid summary config accepted")
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatal("invalid summary config reached HTTP")
	}
}
