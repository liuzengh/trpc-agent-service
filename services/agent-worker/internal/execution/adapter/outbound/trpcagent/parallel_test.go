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
	"sync/atomic"
	"testing"
	"time"
	"trpc.group/trpc-go/trpc-agent-go/event"
)

func parallelRequest(endpoint string) Request {
	req := testRequest(endpoint)
	leaf := func(name string) NodeConfig { m := req.Model; m.Name = name; return NodeConfig{Kind: "llm", Model: m} }
	req.Nodes = map[string]NodeConfig{
		req.NodeID: {Kind: "sequence", Children: []string{"parallel", "aggregate"}},
		"parallel": {Kind: "parallel", Children: []string{"a", "b"}},
		"a":        {Kind: "sequence", Children: []string{"a1", "a2"}}, "b": {Kind: "sequence", Children: []string{"b1", "b2"}},
		"a1": leaf("a1"), "a2": leaf("a2"), "b1": leaf("b1"), "b2": leaf("b2"), "aggregate": leaf("aggregate"),
	}
	return req
}
func requestContains(body map[string]any, text string) bool {
	messages, _ := body["messages"].([]any)
	for _, raw := range messages {
		m, _ := raw.(map[string]any)
		content, _ := m["content"].(string)
		if strings.Contains(content, text) {
			return true
		}
	}
	return false
}
func TestParallelOverlapIsolationAndExplicitAggregate(t *testing.T) {
	for _, fast := range []string{"a", "b"} {
		t.Run("first-"+fast, func(t *testing.T) {
			entered := make(chan string, 2)
			release := make(chan struct{})
			fastSecond := make(chan struct{})
			var mu sync.Mutex
			var problems []string
			var calls []string
			problem := func(s string) { mu.Lock(); problems = append(problems, s); mu.Unlock() }
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					problem(err.Error())
					return
				}
				name, _ := body["model"].(string)
				mu.Lock()
				calls = append(calls, name)
				mu.Unlock()
				if strings.HasSuffix(name, "1") {
					entered <- name
					select {
					case <-release:
					case <-r.Context().Done():
						return
					}
					if !strings.HasPrefix(name, fast) {
						select {
						case <-fastSecond:
						case <-r.Context().Done():
							return
						}
					}
				}
				if strings.HasSuffix(name, "2") {
					own := name[:1]
					other := "a"
					if own == "a" {
						other = "b"
					}
					if !requestContains(body, own+"1_OUTPUT") {
						problem(name + " missing own prior output")
					}
					if requestContains(body, other+"1_OUTPUT") || requestContains(body, other+"2_OUTPUT") {
						problem(name + " read sibling output")
					}
				}
				if name == "aggregate" {
					for _, prior := range []string{"a1", "a2", "b1", "b2"} {
						if !requestContains(body, prior+"_OUTPUT") {
							problem("aggregate missing " + prior)
						}
					}
				}
				writeMemoryHTTPResponse(w, 0, "", "", name+"_OUTPUT", false)
			}))
			defer server.Close()
			req := parallelRequest(server.URL)
			executor := testExecutor()
			var signal sync.Once
			executor.beforeAppend = func(evt *event.Event) error {
				if evt != nil && evt.Author == fast+"2" && evt.Response != nil && !evt.IsPartial {
					for _, choice := range evt.Choices {
						if choice.Message.Content == fast+"2_OUTPUT" {
							signal.Do(func() { close(fastSecond) })
						}
					}
				}
				return nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			type outcome struct {
				result Result
				err    error
			}
			done := make(chan outcome, 1)
			go func() { r, e := executor.Execute(ctx, req); done <- outcome{r, e} }()
			for i := 0; i < 2; i++ {
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("two branch requests did not overlap")
				}
			}
			close(release)
			got := <-done
			if got.err != nil || got.result.FinalText != "aggregate_OUTPUT" || got.result.Usage.TotalTokens != 50 {
				t.Fatalf("result=%+v err=%v", got.result, got.err)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(problems) > 0 || len(calls) != 5 {
				t.Fatalf("calls=%v problems=%v", calls, problems)
			}
		})
	}
}
func TestParallelFailureCancelsSiblingAndNeverAggregates(t *testing.T) {
	for _, mode := range []string{"a", "b", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			entered := make(chan struct{}, 2)
			release := make(chan struct{})
			cancelled := make(chan string, 2)
			var aggregates atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]any
				_ = json.NewDecoder(r.Body).Decode(&body)
				name, _ := body["model"].(string)
				if name == "aggregate" {
					aggregates.Add(1)
					writeMemoryHTTPResponse(w, 0, "", "", "WRONG", false)
					return
				}
				entered <- struct{}{}
				select {
				case <-release:
				case <-r.Context().Done():
					cancelled <- name
					return
				}
				if mode != "deadline" && strings.HasPrefix(name, mode) {
					http.Error(w, "unavailable", 503)
					return
				}
				<-r.Context().Done()
				cancelled <- name
			}))
			defer server.Close()
			req := parallelRequest(server.URL)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				result, err := testExecutor().Execute(ctx, req)
				if !reflect.DeepEqual(result, Result{}) {
					done <- fmt.Errorf("failure returned candidate %+v", result)
					return
				}
				done <- err
			}()
			for i := 0; i < 2; i++ {
				select {
				case <-entered:
				case <-ctx.Done():
					t.Fatal("branches did not start")
				}
			}
			close(release)
			err := <-done
			want := ErrRetryableModel
			if mode == "deadline" {
				want = context.DeadlineExceeded
			}
			if !errors.Is(err, want) || aggregates.Load() != 0 {
				t.Fatalf("err=%v aggregate=%d", err, aggregates.Load())
			}
			count := 1
			if mode == "deadline" {
				count = 2
			}
			for i := 0; i < count; i++ {
				select {
				case <-cancelled:
				case <-time.After(time.Second):
					t.Fatal("sibling HTTP request not cancelled")
				}
			}
		})
	}
}
func TestParallelRejectsTerminalBranchSelection(t *testing.T) {
	for _, nested := range []bool{false, true} {
		req := parallelRequest("http://127.0.0.1:1")
		delete(req.Nodes, "aggregate")
		if nested {
			req.Nodes[req.NodeID] = NodeConfig{Kind: "sequence", Children: []string{"nested"}}
			req.Nodes["nested"] = NodeConfig{Kind: "sequence", Children: []string{"parallel"}}
		} else {
			req.Nodes[req.NodeID] = NodeConfig{Kind: "sequence", Children: []string{"parallel"}}
		}
		result, err := testExecutor().Execute(context.Background(), req)
		if err == nil || !reflect.DeepEqual(result, Result{}) {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	}
}

func TestParallelWaitsForCancelledToolToReturn(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(fmt.Sprint(timeout), func(t *testing.T) {
			toolEntered := make(chan struct{})
			toolCancelled := make(chan struct{})
			toolRelease := make(chan struct{})
			remote := &executionMCPTool{call: func(ctx context.Context, _ []byte, _ int32) (any, error) {
				close(toolEntered)
				<-ctx.Done()
				close(toolCancelled)
				<-toolRelease
				return nil, ctx.Err()
			}}
			f, s1 := newMemoryHTTPFixture(t, []string{mcpSearchProvider}, memoryHTTPRound{tool: mcpSearchProvider, args: `{"q":"blocking"}`})
			s2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-toolEntered:
				case <-r.Context().Done():
					return
				}
				http.Error(w, "unavailable", 503)
			}))
			defer s2.Close()
			req := testRequest(s1.URL)
			req.MaxToolCalls = 1
			req.Nodes = map[string]NodeConfig{req.NodeID: {Kind: "sequence", Children: []string{"parallel", "final"}}, "parallel": {Kind: "parallel", Children: []string{"a", "b"}}, "a": {Kind: "llm", Model: req.Model, Tools: []MCPToolConfig{{Resource: "search", Tool: remote}}}, "b": {Kind: "llm", Model: testRequest(s2.URL).Model}, "final": {Kind: "llm", Model: req.Model}}
			done := make(chan error, 1)
			executor := testExecutor()
			if timeout {
				executor.DrainTimeout = 10 * time.Millisecond
			}
			go func() { _, err := executor.Execute(context.Background(), req); done <- err }()
			select {
			case <-toolCancelled:
			case <-time.After(3 * time.Second):
				close(toolRelease)
				t.Fatal("tool never cancelled")
			}
			if timeout {
				select {
				case err := <-done:
					if !errors.Is(err, ErrDrain) {
						close(toolRelease)
						t.Fatalf("missing drain timeout: %v", err)
					}
				case <-time.After(time.Second):
					close(toolRelease)
					t.Fatal("drain exceeded bound")
				}
				close(toolRelease)
				f.check(t, 1)
				return
			}
			early := false
			select {
			case <-done:
				early = true
			case <-time.After(30 * time.Millisecond):
			}
			close(toolRelease)
			if early {
				t.Fatal("Executor returned before cancelled parallel tool completed")
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("unexpected success")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Executor did not finish after tool release")
			}
			f.check(t, 1)
		})
	}
}

// Real SDK tools in two parallel LLMAgents share the one Attempt view. The HTTP
// fixture only supplies model responses; it does not implement Memory CRUD or
// substitute Runner/agent behavior. Persistence into PG/Redis is not this test.
func TestParallelMemoryAddsShareAttemptWithoutLostWrites(t *testing.T) {
	const firstMemory = "PARALLEL_MEMORY_A_INDEPENDENT"
	const secondMemory = "PARALLEL_MEMORY_B_INDEPENDENT"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	barrier := func(map[string]any) error {
		entered <- struct{}{}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	first, s1 := newMemoryHTTPFixture(t, []string{"memory_add"},
		memoryHTTPRound{tool: "memory_add", args: `{"memory":"` + firstMemory + `"}`, before: barrier},
		memoryHTTPRound{final: "first branch done", before: memoryHTTPAddCheck(0, firstMemory)})
	second, s2 := newMemoryHTTPFixture(t, []string{"memory_add"},
		memoryHTTPRound{tool: "memory_add", args: `{"memory":"` + secondMemory + `"}`, before: barrier},
		memoryHTTPRound{final: "second branch done", before: memoryHTTPAddCheck(0, secondMemory)})
	aggregate, s3 := newMemoryHTTPFixture(t, []string{"memory_load"},
		memoryHTTPRound{tool: "memory_load", args: `{"limit":10}`},
		memoryHTTPRound{final: "both parallel memories loaded", before: memoryHTTPLoadCheck(0, []string{firstMemory, secondMemory}, nil)})
	req := memoryHTTPRequest(s1.URL, nil)
	req.MaxToolCalls = 3 // Two actual SDK adds plus the aggregate's actual SDK load.
	req.Nodes = map[string]NodeConfig{
		req.NodeID:  {Kind: "sequence", Children: []string{"parallel", "aggregate"}},
		"parallel":  {Kind: "parallel", Children: []string{"a", "b"}},
		"a":         {Kind: "llm", Model: req.Model, Memory: &MemorySelection{Tools: []string{"memory_add"}}},
		"b":         {Kind: "llm", Model: testRequest(s2.URL).Model, Memory: &MemorySelection{Tools: []string{"memory_add"}}},
		"aggregate": {Kind: "llm", Model: testRequest(s3.URL).Model, Memory: &MemorySelection{Tools: []string{"memory_load"}}},
	}
	type outcome struct {
		result Result
		err    error
	}
	done := make(chan outcome, 1)
	go func() { result, err := testExecutor().Execute(ctx, req); done <- outcome{result, err} }()
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("parallel memory model requests did not overlap")
		}
	}
	close(release)
	got := <-done
	if got.err != nil || got.result.FinalText != "both parallel memories loaded" || got.result.Memory == nil {
		t.Fatalf("result=%+v err=%v", got.result, got.err)
	}
	candidate := got.result.Memory
	if candidate.Scope != req.Memory.BoundKey || candidate.BaseRevision != 7 || len(candidate.Entries) != 2 {
		t.Fatalf("wrong candidate identity/count: %+v", candidate)
	}
	contents := map[string]bool{}
	ids := map[string]bool{}
	for _, entry := range candidate.Entries {
		if entry == nil || entry.Memory == nil || entry.ID == "" || ids[entry.ID] {
			t.Fatal("invalid/duplicate candidate entry")
		}
		ids[entry.ID] = true
		contents[entry.Memory.Memory] = true
	}
	if !contents[firstMemory] || !contents[secondMemory] {
		t.Fatalf("silent lost write: %v", contents)
	}
	if got.result.Usage.TotalTokens != 60 {
		t.Fatalf("usage duplicated/missing: %+v", got.result.Usage)
	}
	first.check(t, 2)
	second.check(t, 2)
	aggregate.check(t, 2)
}
