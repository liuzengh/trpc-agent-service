package trpcagent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

func loopRequest(endpoint string, iterations int64) Request {
	req := testRequest(endpoint)
	req.Nodes = map[string]NodeConfig{req.NodeID: {Kind: "loop", Body: "body", MaxIterations: iterations}, "body": {Kind: "llm", Model: req.Model}}
	return req
}
func TestLoopRootIterationsHistoryAndUsage(t *testing.T) {
	for _, count := range []int64{1, 2, 3, 32} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			rounds := []memoryHTTPRound{}
			for i := int64(0); i < count; i++ {
				round := memoryHTTPRound{final: fmt.Sprintf("LOOP_OUTPUT_%d", i)}
				if i > 0 {
					round.before = requireSequenceContext(fmt.Sprintf("LOOP_OUTPUT_%d", i-1))
				}
				rounds = append(rounds, round)
			}
			fixture, server := newMemoryHTTPFixture(t, nil, rounds...)
			req := loopRequest(server.URL, count)
			result, err := testExecutor().Execute(context.Background(), req)
			if err != nil || result.FinalText != fmt.Sprintf("LOOP_OUTPUT_%d", count-1) || result.Usage.TotalTokens != int(count)*10 || len(result.Snapshot) == 0 {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			fixture.check(t, int(count))
		})
	}
}
func TestLoopNestedSequenceBody(t *testing.T) {
	rounds := []memoryHTTPRound{}
	for i := 0; i < 3; i++ {
		first := memoryHTTPRound{final: fmt.Sprintf("FIRST_%d", i)}
		if i > 0 {
			first.before = requireSequenceContext(fmt.Sprintf("LAST_%d", i-1))
		}
		rounds = append(rounds, first, memoryHTTPRound{final: fmt.Sprintf("LAST_%d", i), before: requireSequenceContext(fmt.Sprintf("FIRST_%d", i))})
	}
	f, s := newMemoryHTTPFixture(t, nil, rounds...)
	req := loopRequest(s.URL, 3)
	req.Nodes["body"] = NodeConfig{Kind: "sequence", Children: []string{"first", "last"}}
	req.Nodes["first"] = NodeConfig{Kind: "llm", Model: req.Model}
	req.Nodes["last"] = NodeConfig{Kind: "llm", Model: req.Model}
	result, err := testExecutor().Execute(context.Background(), req)
	if err != nil || result.FinalText != "LAST_2" || result.Usage.TotalTokens != 60 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	f.check(t, 6)
}
func TestLoopInvalidConfigurationBeforeProvider(t *testing.T) {
	f, s := newMemoryHTTPFixture(t, nil)
	for _, mode := range []string{"zero", "negative", "33", "empty", "missing", "cycle", "children", "llm-body"} {
		t.Run(mode, func(t *testing.T) {
			req := loopRequest(s.URL, 2)
			root := req.Nodes[req.NodeID]
			switch mode {
			case "zero":
				root.MaxIterations = 0
			case "negative":
				root.MaxIterations = -1
			case "33":
				root.MaxIterations = 33
			case "empty":
				root.Body = ""
			case "missing":
				root.Body = "missing"
			case "cycle":
				root.Body = req.NodeID
			case "children":
				root.Children = []string{"body"}
			case "llm-body":
				leaf := req.Nodes["body"]
				leaf.Body = "body"
				req.Nodes["body"] = leaf
			}
			req.Nodes[req.NodeID] = root
			result, err := testExecutor().Execute(context.Background(), req)
			if err == nil || !reflect.DeepEqual(result, Result{}) {
				t.Fatalf("result=%+v err=%v", result, err)
			}
		})
	}
	f.check(t, 0)
}
func TestLoopLaterFailureNeverKeepsEarlierFinal(t *testing.T) {
	for _, mode := range []string{"503", "empty", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				n := calls.Add(1)
				if n == 1 {
					writeMemoryHTTPResponse(w, 0, "", "", "EARLY_NOT_FINAL", false)
					return
				}
				switch mode {
				case "503":
					http.Error(w, "unavailable", 503)
				case "empty":
					writeMemoryHTTPResponse(w, 1, "", "", "", false)
				case "deadline":
					<-r.Context().Done()
				}
			}))
			defer server.Close()
			iterations := int64(3)
			if mode == "empty" {
				iterations = 2
			}
			req := loopRequest(server.URL, iterations)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			result, err := testExecutor().Execute(ctx, req)
			want := ErrRetryableModel
			if mode == "empty" {
				want = ErrFinal
			}
			if mode == "deadline" {
				want = context.DeadlineExceeded
			}
			if !errors.Is(err, want) || !reflect.DeepEqual(result, Result{}) || calls.Load() != 2 {
				t.Fatalf("result=%+v err=%v calls=%d", result, err, calls.Load())
			}
		})
	}
}
func TestLoopToolStateAndBudgetSpanIterations(t *testing.T) {
	for _, limit := range []int64{1, 2} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			rounds := []memoryHTTPRound{{tool: mcpSearchProvider, args: `{"q":"first"}`}, {final: "first iteration"}, {tool: mcpSearchProvider, args: `{"q":"second"}`}}
			if limit == 2 {
				rounds = append(rounds, memoryHTTPRound{final: "last iteration"})
			}
			f, s := newMemoryHTTPFixture(t, []string{mcpSearchProvider}, rounds...)
			remote := &executionMCPTool{}
			req := loopRequest(s.URL, 2)
			req.MaxToolCalls = limit
			leaf := req.Nodes["body"]
			leaf.Tools = []MCPToolConfig{{Resource: "search", Tool: remote}}
			req.Nodes["body"] = leaf
			result, err := testExecutor().Execute(context.Background(), req)
			if limit == 1 {
				if !errors.Is(err, ErrMemoryTool) || !reflect.DeepEqual(result, Result{}) || remote.calls.Load() != 1 {
					t.Fatalf("budget reset result=%+v err=%v calls=%d", result, err, remote.calls.Load())
				}
			} else {
				if err != nil || result.FinalText != "last iteration" || remote.calls.Load() != 2 || result.Usage.TotalTokens != 40 {
					t.Fatalf("result=%+v err=%v calls=%d", result, err, remote.calls.Load())
				}
				f.check(t, 4)
			}
		})
	}
}
