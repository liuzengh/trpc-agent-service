package trpcagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSequenceNestedConsumesPriorLeafAndTerminalOnly(t *testing.T) {
	first, s1 := newMemoryHTTPFixture(t, nil, memoryHTTPRound{final: "FIRST_CANARY"})
	second, s2 := newMemoryHTTPFixture(t, nil, memoryHTTPRound{final: "SECOND_CANARY", before: requireSequenceContext("FIRST_CANARY")})
	third, s3 := newMemoryHTTPFixture(t, nil, memoryHTTPRound{final: "TERMINAL", before: requireSequenceContext("FIRST_CANARY", "SECOND_CANARY")})
	req := testRequest(s1.URL)
	req.Nodes = map[string]NodeConfig{
		req.NodeID: {Kind: "sequence", Children: []string{"first", "nested"}},
		"first":    {Kind: "llm", Model: req.Model},
		"nested":   {Kind: "sequence", Children: []string{"second", "third"}},
		"second":   {Kind: "llm", Model: testRequest(s2.URL).Model},
		"third":    {Kind: "llm", Model: testRequest(s3.URL).Model},
	}
	result, err := testExecutor().Execute(context.Background(), req)
	if err != nil || result.FinalText != "TERMINAL" || len(result.Snapshot) == 0 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	first.check(t, 1)
	second.check(t, 1)
	third.check(t, 1)
	// Fixture emits 7 prompt + 3 completion tokens per call.
	if result.Usage.TotalTokens != 30 {
		t.Fatalf("usage duplicated or missing: %+v", result.Usage)
	}
}
func requireSequenceContext(want ...string) func(map[string]any) error {
	return func(req map[string]any) error {
		messages, _ := req["messages"].([]any)
		for _, needle := range want {
			found := false
			for _, raw := range messages {
				m, _ := raw.(map[string]any)
				content, _ := m["content"].(string)
				if strings.Contains(content, needle) {
					found = true
				}
			}
			if !found {
				return fmt.Errorf("missing prior leaf context %s", needle)
			}
		}
		return nil
	}
}
func TestSequenceTerminalErrorDiscardsEarlierFinal(t *testing.T) {
	first, s1 := newMemoryHTTPFixture(t, nil, memoryHTTPRound{final: "NOT_A_FINAL"})
	s2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "unavailable", 503) }))
	defer s2.Close()
	req := testRequest(s1.URL)
	req.Nodes = map[string]NodeConfig{req.NodeID: {Kind: "sequence", Children: []string{"first", "last"}}, "first": {Kind: "llm", Model: req.Model}, "last": {Kind: "llm", Model: testRequest(s2.URL).Model}}
	result, err := testExecutor().Execute(context.Background(), req)
	if !errors.Is(err, ErrRetryableModel) || !reflect.DeepEqual(result, Result{}) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	first.check(t, 1)
}
func TestSequenceNodeToolsIsolatedAndRunBudgetShared(t *testing.T) {
	for _, limit := range []int64{1, 2} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			one, s1 := newMemoryHTTPFixture(t, []string{mcpSearchProvider}, memoryHTTPRound{tool: mcpSearchProvider, args: `{"q":"first"}`}, memoryHTTPRound{final: "first complete"})
			rounds := []memoryHTTPRound{{tool: mcpAProvider, args: `{"q":"last"}`}}
			if limit == 2 {
				rounds = append(rounds, memoryHTTPRound{final: "last complete"})
			}
			two, s2 := newMemoryHTTPFixture(t, []string{mcpAProvider}, rounds...)
			a, b := &executionMCPTool{}, &executionMCPTool{}
			req := testRequest(s1.URL)
			req.MaxToolCalls = limit
			req.Nodes = map[string]NodeConfig{req.NodeID: {Kind: "sequence", Children: []string{"first", "last"}}, "first": {Kind: "llm", Model: req.Model, Tools: []MCPToolConfig{{Resource: "search", Tool: a}}}, "last": {Kind: "llm", Model: testRequest(s2.URL).Model, Tools: []MCPToolConfig{{Resource: "a", Tool: b}}}}
			result, err := testExecutor().Execute(context.Background(), req)
			if limit == 1 {
				if !errors.Is(err, ErrMemoryTool) || !reflect.DeepEqual(result, Result{}) || b.calls.Load() != 0 {
					t.Fatalf("shared budget result=%+v err=%v calls=%d", result, err, b.calls.Load())
				}
			} else if err != nil || result.FinalText != "last complete" || b.calls.Load() != 1 {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if a.calls.Load() != 1 {
				t.Fatal("first tool not called once")
			}
			one.check(t, 2)
			if limit == 2 {
				two.check(t, 2)
			}
		})
	}
}
func TestSequenceNoImplicitTopLevelToolSelection(t *testing.T) {
	f, s := newMemoryHTTPFixture(t, nil, memoryHTTPRound{final: "explicit leaf only"})
	req := testRequest(s.URL)
	remote := &executionMCPTool{}
	req.Tools = []MCPToolConfig{{Resource: "search", Tool: remote}}
	req.MaxToolCalls = 3
	req.Nodes = map[string]NodeConfig{req.NodeID: {Kind: "sequence", Children: []string{"leaf"}}, "leaf": {Kind: "llm", Model: req.Model}}
	result, err := testExecutor().Execute(context.Background(), req)
	if err != nil || result.FinalText != "explicit leaf only" || remote.calls.Load() != 0 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	f.check(t, 1)
}
func TestSequenceRejectsInvalidTreeBeforeProvider(t *testing.T) {
	f, s := newMemoryHTTPFixture(t, nil)
	for _, kind := range []string{"cycle", "parallel", "loop", "missing", "reuse"} {
		t.Run(kind, func(t *testing.T) {
			req := testRequest(s.URL)
			req.Nodes = map[string]NodeConfig{req.NodeID: {Kind: kind}}
			if kind == "cycle" {
				req.Nodes[req.NodeID] = NodeConfig{Kind: "sequence", Children: []string{req.NodeID}}
			}
			if kind == "missing" {
				req.Nodes[req.NodeID] = NodeConfig{Kind: "sequence", Children: []string{"missing"}}
			}
			if kind == "reuse" {
				req.Nodes[req.NodeID] = NodeConfig{Kind: "sequence", Children: []string{"leaf", "leaf"}}
				req.Nodes["leaf"] = NodeConfig{Kind: "llm", Model: req.Model}
			}
			result, err := testExecutor().Execute(context.Background(), req)
			if err == nil || !reflect.DeepEqual(result, Result{}) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
	f.check(t, 0)
}

func TestSequenceFixedModelsCredentialsAndCancellation(t *testing.T) {
	var mu sync.Mutex
	var calls []string
	entered := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		name, _ := body["model"].(string)
		if r.Header.Get("Authorization") != "Bearer key-"+name {
			t.Error("node credential mismatch")
			http.Error(w, "unauthorized", 401)
			return
		}
		mu.Lock()
		calls = append(calls, name)
		mu.Unlock()
		if name == "first-model" {
			writeMemoryHTTPResponse(w, 0, "", "", "EARLY_NOT_FINAL", false)
			return
		}
		if name != "last-model" {
			t.Error("wrong node model")
			return
		}
		close(entered)
		<-r.Context().Done()
	}))
	defer server.Close()
	req := testRequest(server.URL)
	first, last := req.Model, req.Model
	first.Name = "first-model"
	first.APIKey = "key-first-model"
	last.Name = "last-model"
	last.APIKey = "key-last-model"
	req.Nodes = map[string]NodeConfig{req.NodeID: {Kind: "sequence", Children: []string{"first", "last"}}, "first": {Kind: "llm", Model: first}, "last": {Kind: "llm", Model: last}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		result, err := testExecutor().Execute(ctx, req)
		if !reflect.DeepEqual(result, Result{}) {
			done <- fmt.Errorf("cancel returned candidate: %+v", result)
			return
		}
		done <- err
	}()
	select {
	case <-entered:
		cancel()
	case <-time.After(5 * time.Second):
		t.Fatal("terminal model never entered")
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel err=%v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel did not drain")
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(calls, []string{"first-model", "last-model"}) {
		t.Fatalf("calls=%v", calls)
	}
}
func TestSequenceEmptyTerminalDoesNotReuseEarlierText(t *testing.T) {
	f, s := newMemoryHTTPFixture(t, nil, memoryHTTPRound{final: "EARLY_NOT_FINAL"}, memoryHTTPRound{})
	req := testRequest(s.URL)
	req.Nodes = map[string]NodeConfig{req.NodeID: {Kind: "sequence", Children: []string{"first", "last"}}, "first": {Kind: "llm", Model: req.Model}, "last": {Kind: "llm", Model: req.Model}}
	result, err := testExecutor().Execute(context.Background(), req)
	if !errors.Is(err, ErrFinal) || !reflect.DeepEqual(result, Result{}) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	f.check(t, 2)
}

func TestSequenceSummaryFilterAndExplicitConsumption(t *testing.T) {
	const summaryText = "ACCEPTED_SEQUENCE_SUMMARY_CANARY"
	var summaries int
	var mu sync.Mutex
	phase := 0
	consumed := map[string]bool{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if body["model"] == "summary-model" {
			summaries++
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"id":"summary","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":%q},"finish_reason":"stop"}],"usage":{"prompt_tokens":11,"completion_tokens":7,"total_tokens":18}}`, summaryText)
			return
		}
		name := body["model"].(string)
		if phase == 2 {
			messages, _ := body["messages"].([]any)
			for _, raw := range messages {
				m, _ := raw.(map[string]any)
				content, _ := m["content"].(string)
				if strings.Contains(content, summaryText) {
					consumed[name] = true
				}
			}
		}
		writeMemoryHTTPResponse(w, 0, "", "", "leaf answer", false)
	}))
	defer server.Close()
	req := testRequest(server.URL)
	first, last := req.Model, req.Model
	first.Name = "first-model"
	last.Name = "last-model"
	req.Nodes = map[string]NodeConfig{req.NodeID: {Kind: "sequence", Children: []string{"first", "last"}}, "first": {Kind: "llm", Model: first}, "last": {Kind: "llm", Model: last, AddSessionSummary: true}}
	req.Summary = &SummaryConfig{Model: Model{Endpoint: server.URL + "/v1", Name: "summary-model"}, EventThreshold: 1}
	for i := 0; i < 3; i++ {
		mu.Lock()
		phase = i
		mu.Unlock()
		req.RunID = fmt.Sprintf("run-%d", i)
		req.AttemptID = fmt.Sprintf("attempt-%d", i)
		result, err := testExecutor().Execute(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		req.AcceptedSnapshot = result.Snapshot
		var stored snapshot
		if err := json.Unmarshal(result.Snapshot, &stored); err != nil {
			t.Fatal(err)
		}
		for _, evt := range stored.Session.Events {
			if evt.FilterKey != sessionKey(req.TenantID, req.SessionID).AppName {
				t.Fatalf("clone changed filter: %q", evt.FilterKey)
			}
			if evt.Author == "first" && evt.Branch != "root/first" || evt.Author == "last" && evt.Branch != "root/last" {
				t.Fatalf("clone changed branch: %q", evt.Branch)
			}
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if summaries == 0 || !consumed["last-model"] || consumed["first-model"] {
		t.Fatalf("summaries=%d consumption=%v", summaries, consumed)
	}
}

func TestSequenceSharesOneMemoryAttemptWithLeafSelection(t *testing.T) {
	first, s1 := newMemoryHTTPFixture(t, []string{"memory_add"}, memoryHTTPRound{tool: "memory_add", args: `{"memory":"SEQUENCE_PRIVATE_MEMORY"}`}, memoryHTTPRound{final: "memory staged"})
	last, s2 := newMemoryHTTPFixture(t, []string{"memory_load"}, memoryHTTPRound{tool: "memory_load", args: `{"limit":10}`}, memoryHTTPRound{final: "memory consumed", before: memoryHTTPLoadCheck(0, []string{"SEQUENCE_PRIVATE_MEMORY"}, nil)})
	req := memoryHTTPRequest(s1.URL, []string{"memory_clear"})
	req.Nodes = map[string]NodeConfig{req.NodeID: {Kind: "sequence", Children: []string{"first", "last"}}, "first": {Kind: "llm", Model: req.Model, Memory: &MemorySelection{Tools: []string{"memory_add"}}}, "last": {Kind: "llm", Model: testRequest(s2.URL).Model, Memory: &MemorySelection{Tools: []string{"memory_load"}}}}
	result, err := testExecutor().Execute(context.Background(), req)
	if err != nil || result.FinalText != "memory consumed" || result.Memory == nil || len(result.Memory.Entries) != 1 || result.Memory.BaseRevision != 7 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	first.check(t, 2)
	last.check(t, 2)
}
