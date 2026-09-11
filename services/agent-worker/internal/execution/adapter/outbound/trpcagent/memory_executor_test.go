package trpcagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/memory"
	memorytool "trpc.group/trpc-go/trpc-agent-go/memory/tool"
)

// This fixture speaks the actual OpenAI SSE protocol consumed by the pinned SDK.
// No fake Runner, agent, tool, Memory service, or execution Plan is substituted.
type memoryHTTPRound struct {
	tool, args, final string
	before            func(map[string]any) error
	arguments         func() string
	fragmented        bool
}
type memoryHTTPFixture struct {
	mu       sync.Mutex
	calls    int
	problems []string
	rounds   []memoryHTTPRound
	tools    []string
}

func newMemoryHTTPFixture(t *testing.T, tools []string, rounds ...memoryHTTPRound) (*memoryHTTPFixture, *httptest.Server) {
	t.Helper()
	f := &memoryHTTPFixture{rounds: rounds, tools: append([]string(nil), tools...)}
	server := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(server.Close)
	return f, server
}
func (f *memoryHTTPFixture) serve(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	f.mu.Lock()
	defer f.mu.Unlock()
	fail := func(format string, args ...any) {
		f.problems = append(f.problems, fmt.Sprintf(format, args...))
		http.Error(w, "fixture request rejected", http.StatusBadRequest)
	}
	n := f.calls
	f.calls++
	if n >= len(f.rounds) {
		fail("extra model request %d", n)
		return
	}
	if r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer fixture-token" {
		fail("wrong model endpoint or credential")
		return
	}
	var request map[string]any
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		fail("decode: %v", err)
		return
	}
	if request["model"] != "fixture-model" || request["stream"] != true || request["max_completion_tokens"] != float64(20000) {
		fail("published generation changed")
		return
	}
	var got []string
	declarations, _ := request["tools"].([]any)
	for _, value := range declarations {
		declaration, ok := value.(map[string]any)
		if !ok {
			fail("invalid declaration")
			return
		}
		function, ok := declaration["function"].(map[string]any)
		if !ok || declaration["type"] != "function" {
			fail("invalid function declaration")
			return
		}
		name, ok := function["name"].(string)
		if !ok {
			fail("missing tool name")
			return
		}
		got = append(got, name)
	}
	want := append([]string(nil), f.tools...)
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		fail("SDK tool selection got=%v want=%v", got, want)
		return
	}
	round := f.rounds[n]
	if round.before != nil {
		if err := round.before(request); err != nil {
			fail("round %d: %v", n, err)
			return
		}
	}
	args := round.args
	if round.arguments != nil {
		args = round.arguments()
	}
	writeMemoryHTTPResponse(w, n, round.tool, args, round.final, round.fragmented)
}
func (f *memoryHTTPFixture) check(t *testing.T, wantCalls int) {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.problems) > 0 || f.calls != wantCalls {
		t.Fatalf("model calls=%d want=%d fixture errors=%v", f.calls, wantCalls, f.problems)
	}
}
func writeMemoryHTTPResponse(w http.ResponseWriter, n int, tool, args, final string, fragmented bool) {
	w.Header().Set("Content-Type", "text/event-stream")
	delta := map[string]any{"role": "assistant", "content": final}
	finish := "stop"
	if tool != "" {
		delete(delta, "content")
		delta["tool_calls"] = []any{map[string]any{"index": 0, "id": fmt.Sprintf("memory-call-%d", n), "type": "function", "function": map[string]any{"name": tool, "arguments": args}}}
		finish = "tool_calls"
	}
	chunk := func(d map[string]any, reason any, usage any) {
		body := map[string]any{"id": fmt.Sprintf("memory-response-%d", n), "object": "chat.completion.chunk", "created": 1, "model": "fixture-model", "choices": []any{map[string]any{"index": 0, "delta": d, "finish_reason": reason}}}
		if usage != nil {
			body["usage"] = usage
		}
		wire, _ := json.Marshal(body)
		fmt.Fprintf(w, "data: %s\n\n", wire)
	}
	if fragmented && tool != "" {
		// The SDK must assemble a name and argument JSON across deltas before
		// the executor checks the complete published tool selection.
		delta["tool_calls"] = []any{map[string]any{"index": 0, "id": fmt.Sprintf("memory-call-%d", n), "type": "function", "function": map[string]any{"name": tool[:3], "arguments": args[:len(args)/2]}}}
		chunk(delta, nil, nil)
		chunk(map[string]any{"tool_calls": []any{map[string]any{"index": 0, "function": map[string]any{"name": tool[3:], "arguments": args[len(args)/2:]}}}}, nil, nil)
	} else {
		chunk(delta, nil, nil)
	}
	chunk(map[string]any{}, finish, map[string]int{"prompt_tokens": 7, "completion_tokens": 3, "total_tokens": 10})
	fmt.Fprint(w, "data: [DONE]\n\n")
}
func memoryHTTPRequest(endpoint string, tools []string) Request {
	req := testRequest(endpoint)
	req.Memory = &MemoryConfig{BoundKey: memory.UserKey{AppName: req.TenantID, UserID: "trusted-subject-agent"}, BaseRevision: 7, Tools: append([]string(nil), tools...)}
	req.MaxToolCalls = 20
	return req
}
func executeMemoryHTTP(t *testing.T, req Request) (Result, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return testExecutor().Execute(ctx, req)
}
func requireNoMemoryResult(t *testing.T, result Result, err error) {
	t.Helper()
	if err == nil || !reflect.DeepEqual(result, Result{}) {
		t.Fatalf("failed Memory run leaked candidate/final: memory=%t snapshot_bytes=%d final_bytes=%d usage=%+v err=%v", result.Memory != nil, len(result.Snapshot), len(result.FinalText), result.Usage, err)
	}
}
func decodeMemoryHTTPTool(request map[string]any, call int, dest any) error {
	messages, _ := request["messages"].([]any)
	id := fmt.Sprintf("memory-call-%d", call)
	count := 0
	for _, value := range messages {
		message, ok := value.(map[string]any)
		if !ok {
			continue
		}
		if message["role"] != "tool" || message["tool_call_id"] != id {
			continue
		}
		count++
		content, ok := message["content"].(string)
		if !ok {
			return fmt.Errorf("tool %s content is not text", id)
		}
		if err := json.Unmarshal([]byte(content), dest); err != nil {
			return fmt.Errorf("tool %s decode: %w", id, err)
		}
	}
	if count != 1 {
		return fmt.Errorf("tool %s response count=%d", id, count)
	}
	return nil
}

func TestExecutorMemoryHTTPRejectsUnselectedAndUnknownCalls(t *testing.T) {
	for _, tc := range []struct{ name, tool, args string }{
		{"unselected_sdk_tool", memory.ClearToolName, `{}`},
		{"unknown_tool", "not_a_memory_tool", `{}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tools := []string{memory.AddToolName, memory.UpdateToolName}
			fixture, server := newMemoryHTTPFixture(t, tools,
				memoryHTTPRound{tool: memory.AddToolName, args: `{"memory":"unaccepted private write"}`},
				memoryHTTPRound{tool: tc.tool, args: tc.args, before: func(request map[string]any) error {
					var response memorytool.AddMemoryResponse
					if err := decodeMemoryHTTPTool(request, 0, &response); err != nil {
						return err
					}
					if response.Memory != "unaccepted private write" {
						return fmt.Errorf("prior tool write not observed")
					}
					return nil
				}},
				memoryHTTPRound{final: "Model claims success despite tool failure"},
			)
			result, err := executeMemoryHTTP(t, memoryHTTPRequest(server.URL, tools))
			requireNoMemoryResult(t, result, err)
			// SDK may terminate immediately or return the tool error to the model. Both
			// paths must reject the whole Attempt rather than export the prior write.
			fixture.mu.Lock()
			calls, problems := fixture.calls, append([]string(nil), fixture.problems...)
			fixture.mu.Unlock()
			if calls < 2 || calls > 3 || len(problems) > 0 {
				t.Fatalf("calls=%d problems=%v", calls, problems)
			}
		})
	}
}

func memoryHTTPLoadCheck(call int, want []string, captureID *string) func(map[string]any) error {
	return func(request map[string]any) error {
		var response memorytool.LoadMemoryResponse
		if err := decodeMemoryHTTPTool(request, call, &response); err != nil {
			return err
		}
		var got []string
		ids := map[string]bool{}
		for _, entry := range response.Results {
			if entry.ID == "" || ids[entry.ID] {
				return fmt.Errorf("missing or duplicate Memory id")
			}
			ids[entry.ID] = true
			got = append(got, entry.Memory)
		}
		sort.Strings(got)
		sorted := append([]string(nil), want...)
		sort.Strings(sorted)
		if response.Count != len(want) || !reflect.DeepEqual(got, sorted) {
			return fmt.Errorf("load count=%d content=%v want=%v", response.Count, got, want)
		}
		if captureID != nil {
			if len(response.Results) != 1 {
				return fmt.Errorf("expected exactly one id")
			}
			*captureID = response.Results[0].ID
		}
		return nil
	}
}
func memoryHTTPAddCheck(call int, want string) func(map[string]any) error {
	return func(request map[string]any) error {
		var response memorytool.AddMemoryResponse
		if err := decodeMemoryHTTPTool(request, call, &response); err != nil {
			return err
		}
		if response.Memory != want || response.Message == "" {
			return fmt.Errorf("Memory add response=%+v", response)
		}
		return nil
	}
}
func TestExecutorMemoryHTTPSixToolsAndNextCandidate(t *testing.T) {
	tools := []string{memory.AddToolName, memory.LoadToolName, memory.SearchToolName, memory.UpdateToolName, memory.DeleteToolName, memory.ClearToolName}
	var greenID, blackID string
	fixture, server := newMemoryHTTPFixture(t, tools,
		memoryHTTPRound{tool: memory.AddToolName, args: `{"memory":"likes green tea","topics":["tea"],"user_id":"spoofed-subject","app_name":"spoofed-tenant"}`},
		memoryHTTPRound{tool: memory.LoadToolName, args: `{"limit":10}`, before: memoryHTTPAddCheck(0, "likes green tea")},
		memoryHTTPRound{tool: memory.SearchToolName, args: `{"query":"green tea"}`, before: memoryHTTPLoadCheck(1, []string{"likes green tea"}, &greenID)},
		memoryHTTPRound{tool: memory.UpdateToolName, arguments: func() string {
			return fmt.Sprintf(`{"memory_id":%q,"memory":"likes black tea","topics":["tea"]}`, greenID)
		}, before: func(request map[string]any) error {
			var response memorytool.SearchMemoryResponse
			if err := decodeMemoryHTTPTool(request, 2, &response); err != nil {
				return err
			}
			if response.Query != "green tea" || response.Count != 1 || len(response.Results) != 1 || response.Results[0].ID != greenID || response.Results[0].Memory != "likes green tea" {
				return fmt.Errorf("search did not read own add")
			}
			return nil
		}},
		memoryHTTPRound{tool: memory.LoadToolName, args: `{"limit":10}`, before: func(request map[string]any) error {
			var response memorytool.UpdateMemoryResponse
			if err := decodeMemoryHTTPTool(request, 3, &response); err != nil {
				return err
			}
			if response.Memory != "likes black tea" || response.MemoryID == "" || response.Message == "" {
				return fmt.Errorf("update not applied")
			}
			blackID = response.MemoryID
			return nil
		}},
		memoryHTTPRound{tool: memory.DeleteToolName, arguments: func() string { return fmt.Sprintf(`{"memory_id":%q}`, blackID) }, before: func(request map[string]any) error {
			var loadedID string
			if err := memoryHTTPLoadCheck(4, []string{"likes black tea"}, &loadedID)(request); err != nil {
				return err
			}
			if loadedID != blackID {
				return fmt.Errorf("updated id is not loadable")
			}
			return nil
		}},
		memoryHTTPRound{tool: memory.LoadToolName, args: `{"limit":10}`, before: func(request map[string]any) error {
			var response memorytool.DeleteMemoryResponse
			if err := decodeMemoryHTTPTool(request, 5, &response); err != nil {
				return err
			}
			if response.MemoryID != blackID || response.Message == "" {
				return fmt.Errorf("wrong delete response")
			}
			return nil
		}},
		memoryHTTPRound{tool: memory.AddToolName, args: `{"memory":"discard by clear"}`, before: memoryHTTPLoadCheck(6, nil, nil)},
		memoryHTTPRound{tool: memory.ClearToolName, args: `{}`, before: memoryHTTPAddCheck(7, "discard by clear")},
		memoryHTTPRound{tool: memory.LoadToolName, args: `{"limit":10}`, before: func(request map[string]any) error {
			var response memorytool.ClearMemoryResponse
			if err := decodeMemoryHTTPTool(request, 8, &response); err != nil {
				return err
			}
			if response.Message == "" {
				return fmt.Errorf("missing clear result")
			}
			return nil
		}},
		memoryHTTPRound{tool: memory.AddToolName, args: `{"memory":"keep for next execution","topics":["retained"]}`, before: memoryHTTPLoadCheck(9, nil, nil)},
		memoryHTTPRound{tool: memory.LoadToolName, args: `{"limit":10}`, before: memoryHTTPAddCheck(10, "keep for next execution")},
		memoryHTTPRound{final: "All six Memory tools completed", before: memoryHTTPLoadCheck(11, []string{"keep for next execution"}, nil)},
	)
	req := memoryHTTPRequest(server.URL, tools)
	req.MaxToolCalls = 12 // Exactly all twelve actual invocations, not six declarations.
	first, err := executeMemoryHTTP(t, req)
	if err != nil {
		t.Fatal(err)
	}
	fixture.check(t, 13)
	if first.FinalText != "All six Memory tools completed" || len(first.Snapshot) == 0 || first.Memory == nil {
		t.Fatalf("missing complete execution result")
	}
	if first.Usage != (Usage{InputTokens: 91, OutputTokens: 39, TotalTokens: 130}) {
		t.Fatalf("multi-round usage not accumulated: %+v", first.Usage)
	}
	candidate := first.Memory
	if candidate.Scope != req.Memory.BoundKey || candidate.BaseRevision != 7 || len(candidate.Entries) != 1 {
		t.Fatalf("wrong fixed-scope candidate: %+v", candidate)
	}
	entry := candidate.Entries[0]
	if entry.AppName != candidate.Scope.AppName || entry.UserID != candidate.Scope.UserID || entry.Memory.Memory != "keep for next execution" || !reflect.DeepEqual(entry.Memory.Topics, []string{"retained"}) {
		t.Fatalf("candidate scope/content changed: %+v", entry)
	}
	original := cloneMemoryEntries(candidate.Entries)

	// Explicitly supply the complete accepted candidate to a NEW SDK Session.
	// This tests the executor load seam, not a PG accept transaction or durability.
	nextFixture, nextServer := newMemoryHTTPFixture(t, []string{memory.LoadToolName},
		memoryHTTPRound{tool: memory.LoadToolName, args: `{"limit":10,"user_id":"spoofed-subject"}`},
		memoryHTTPRound{final: "Next execution recalled accepted Memory", before: memoryHTTPLoadCheck(0, []string{"keep for next execution"}, nil)},
	)
	nextReq := memoryHTTPRequest(nextServer.URL, []string{memory.LoadToolName})
	nextReq.SessionID = "another-session"
	nextReq.RunID = "next-run"
	nextReq.AttemptID = "next-attempt"
	nextReq.Memory.Entries = candidate.Entries
	nextReq.Memory.BaseRevision = 8
	nextReq.MaxToolCalls = 1
	next, err := executeMemoryHTTP(t, nextReq)
	if err != nil {
		t.Fatal(err)
	}
	nextFixture.check(t, 2)
	if next.Memory == nil || next.Memory.BaseRevision != 8 || next.Memory.Scope != candidate.Scope || !reflect.DeepEqual(next.Memory.Entries, original) || !reflect.DeepEqual(candidate.Entries, original) {
		t.Fatal("loaded candidate changed scope, metadata or caller-owned input")
	}
	if next.FinalText != "Next execution recalled accepted Memory" || len(next.Snapshot) == 0 || next.Usage != (Usage{InputTokens: 14, OutputTokens: 6, TotalTokens: 20}) {
		t.Fatal("next execution incomplete")
	}

	// Distinct bound subjects or tenants share no implicit global SDK Memory.
	for _, scope := range []memory.UserKey{{AppName: req.TenantID, UserID: "other-subject-agent"}, {AppName: "other-tenant", UserID: req.Memory.BoundKey.UserID}} {
		t.Run(scope.AppName+"/"+scope.UserID, func(t *testing.T) {
			isolatedFixture, isolatedServer := newMemoryHTTPFixture(t, []string{memory.LoadToolName}, memoryHTTPRound{tool: memory.LoadToolName, args: `{"limit":10}`}, memoryHTTPRound{final: "No foreign Memory", before: memoryHTTPLoadCheck(0, nil, nil)})
			isolated := memoryHTTPRequest(isolatedServer.URL, []string{memory.LoadToolName})
			isolated.TenantID = scope.AppName
			isolated.Memory.BoundKey = scope
			result, err := executeMemoryHTTP(t, isolated)
			if err != nil {
				t.Fatal(err)
			}
			isolatedFixture.check(t, 2)
			if result.Memory == nil || result.Memory.Scope != scope || len(result.Memory.Entries) != 0 {
				t.Fatal("Memory leaked across bound scopes")
			}
			// A fully valid candidate from another bound scope is rejected before
			// HTTP, not merely hidden by filtering after a model has run.
			rejectedFixture, rejectedServer := newMemoryHTTPFixture(t, nil)
			isolated.Model.Endpoint = rejectedServer.URL + "/v1"
			isolated.Memory.Entries = original
			rejected, err := executeMemoryHTTP(t, isolated)
			requireNoMemoryResult(t, rejected, err)
			rejectedFixture.check(t, 0)
		})
	}

	for _, limit := range []int{0, 1, -1} {
		t.Run(fmt.Sprintf("explicit_preload_%d_without_tools", limit), func(t *testing.T) {
			preloadFixture, preloadServer := newMemoryHTTPFixture(t, nil, memoryHTTPRound{final: "Preload answer", before: func(request map[string]any) error {
				messages, err := json.Marshal(request["messages"])
				if err != nil {
					return err
				}
				if strings.Contains(string(messages), "keep for next execution") != (limit != 0) {
					return fmt.Errorf("explicit preload limit=%d not honored", limit)
				}
				return nil
			}})
			preload := memoryHTTPRequest(preloadServer.URL, nil)
			preload.Memory.Entries = original
			preload.Memory.PreloadLimit = limit
			result, err := executeMemoryHTTP(t, preload)
			if err != nil {
				t.Fatal(err)
			}
			preloadFixture.check(t, 1)
			if result.Memory == nil || !reflect.DeepEqual(result.Memory.Entries, original) || result.FinalText != "Preload answer" {
				t.Fatal("preload mutated the accepted candidate")
			}
		})
	}
	t.Run("clear_exports_empty_candidate_not_absent_candidate", func(t *testing.T) {
		clearFixture, clearServer := newMemoryHTTPFixture(t, []string{memory.ClearToolName},
			memoryHTTPRound{tool: memory.ClearToolName, args: `{}`},
			memoryHTTPRound{final: "Memory cleared"},
		)
		clearReq := memoryHTTPRequest(clearServer.URL, []string{memory.ClearToolName})
		clearReq.Memory.Entries = original
		clearReq.MaxToolCalls = 1
		result, err := executeMemoryHTTP(t, clearReq)
		if err != nil {
			t.Fatal(err)
		}
		clearFixture.check(t, 2)
		if result.Memory == nil || result.Memory.Scope != candidate.Scope || result.Memory.BaseRevision != 7 || len(result.Memory.Entries) != 0 || result.FinalText != "Memory cleared" || len(result.Snapshot) == 0 || !reflect.DeepEqual(candidate.Entries, original) {
			t.Fatal("clear did not export exact empty scope snapshot or mutated input")
		}
	})
}

func TestExecutorMemoryHTTPPublishedToolBudget(t *testing.T) {
	for _, budget := range []int64{1, 2} {
		t.Run(fmt.Sprint(budget), func(t *testing.T) {
			tools := []string{memory.AddToolName}
			fixture, server := newMemoryHTTPFixture(t, tools,
				memoryHTTPRound{tool: memory.AddToolName, args: `{"memory":"first"}`},
				memoryHTTPRound{tool: memory.AddToolName, args: `{"memory":"second"}`, before: memoryHTTPAddCheck(0, "first")},
				memoryHTTPRound{final: "Budgeted answer"},
			)
			req := memoryHTTPRequest(server.URL, tools)
			req.MaxToolCalls = budget
			result, err := executeMemoryHTTP(t, req)
			if budget == 1 {
				requireNoMemoryResult(t, result, err)
				fixture.mu.Lock()
				calls, problems := fixture.calls, append([]string(nil), fixture.problems...)
				fixture.mu.Unlock()
				if calls < 2 || calls > 3 || len(problems) > 0 {
					t.Fatalf("calls=%d problems=%v", calls, problems)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				fixture.check(t, 3)
				if result.Memory == nil || len(result.Memory.Entries) != 2 || result.FinalText != "Budgeted answer" || result.Usage != (Usage{InputTokens: 21, OutputTokens: 9, TotalTokens: 30}) {
					t.Fatal("exact published tool budget rejected or result incomplete")
				}
			}
		})
	}
}

func TestExecutorMemoryHTTPRejectsBadScopeAndSelectionBeforeModel(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*Request)
	}{
		{"wrong_tenant", func(r *Request) { r.Memory.BoundKey.AppName = "foreign-tenant" }},
		{"empty_subject", func(r *Request) { r.Memory.BoundKey.UserID = "" }},
		{"zero_budget", func(r *Request) { r.MaxToolCalls = 0 }},
		{"negative_budget", func(r *Request) { r.MaxToolCalls = -1 }},
		{"unknown_selection", func(r *Request) { r.Memory.Tools = []string{"unknown"} }},
		{"duplicate_selection", func(r *Request) { r.Memory.Tools = []string{memory.AddToolName, memory.AddToolName} }},
		{"invalid_preload_limit", func(r *Request) { r.Memory.PreloadLimit = -2 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture, server := newMemoryHTTPFixture(t, nil)
			req := memoryHTTPRequest(server.URL, []string{memory.AddToolName})
			tc.change(&req)
			result, err := executeMemoryHTTP(t, req)
			requireNoMemoryResult(t, result, err)
			fixture.check(t, 0)
		})
	}
}

func TestExecutorMemoryHTTPAssemblesFragmentedToolCall(t *testing.T) {
	tools := []string{memory.AddToolName}
	fixture, server := newMemoryHTTPFixture(t, tools,
		memoryHTTPRound{tool: memory.AddToolName, args: `{"memory":"split streaming name and arguments"}`, fragmented: true},
		memoryHTTPRound{final: "Complete tool call accepted", before: memoryHTTPAddCheck(0, "split streaming name and arguments")},
	)
	req := memoryHTTPRequest(server.URL, tools)
	req.MaxToolCalls = 1
	result, err := executeMemoryHTTP(t, req)
	if err != nil {
		t.Fatal(err)
	}
	fixture.check(t, 2)
	if result.Memory == nil || len(result.Memory.Entries) != 1 || result.Memory.Entries[0].Memory.Memory != "split streaming name and arguments" || result.FinalText != "Complete tool call accepted" {
		t.Fatal("valid fragmented tool call was lost")
	}
}

// These assertions observe the pinned SDK's response at the HTTP boundary.
// They do not install an error-text classifier or alter tool error policy.
func memoryHTTPErrorCheck(call int, name, diagnostic string, sanitized bool) func(map[string]any) error {
	return func(request map[string]any) error {
		id := fmt.Sprintf("memory-call-%d", call)
		messages, _ := request["messages"].([]any)
		count := 0
		for _, value := range messages {
			message, ok := value.(map[string]any)
			if !ok {
				continue
			}
			content, _ := message["content"].(string)
			if !sanitized && message["role"] == "tool" && message["tool_call_id"] == id {
				if !strings.Contains(content, diagnostic) {
					return fmt.Errorf("SDK diagnostic did not reach model for %s", id)
				}
				count++
			}
			// Invalid argument JSON is normalized by the SDK's existing sanitizer to
			// an explicit user-role tool-result record before the next HTTP request.
			if sanitized && message["role"] == "user" && strings.Contains(content, "tool_call_id: "+id+"\ntool_name: "+name+"\n") && strings.Contains(content, diagnostic) {
				count++
			}
		}
		if count != 1 {
			return fmt.Errorf("SDK error response count=%d for %s", count, id)
		}
		return nil
	}
}

func TestExecutorMemoryHTTPBusinessErrorAllowsExplanationWithoutInventedEntry(t *testing.T) {
	for _, tc := range []struct {
		name, tool, args, diagnostic string
		sanitized                    bool
	}{
		{"malformed_arguments", memory.AddToolName, `{"memory":`, "unexpected end of JSON input", true},
		{"missing_required_argument", memory.UpdateToolName, `{"memory":"not added"}`, "memory ID is required", false},
		// The empty SDK Memory view has no user bucket yet. A missing entry in
		// an existing bucket is exercised by the correction test below.
		{"unknown_memory_id", memory.UpdateToolName, `{"memory_id":"does-not-exist","memory":"not added"}`, "user trusted-subject-agent not found", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tools := []string{tc.tool}
			explanation := "The requested change was not applied; please provide the missing information."
			fixture, server := newMemoryHTTPFixture(t, tools,
				memoryHTTPRound{tool: tc.tool, args: tc.args},
				memoryHTTPRound{final: explanation, before: memoryHTTPErrorCheck(0, tc.tool, tc.diagnostic, tc.sanitized)},
			)
			req := memoryHTTPRequest(server.URL, tools)
			req.MaxToolCalls = 1
			result, err := executeMemoryHTTP(t, req)
			if err != nil {
				t.Fatalf("ordinary SDK tool error became an Attempt failure: %v", err)
			}
			fixture.check(t, 2)
			if result.FinalText != explanation || len(result.Snapshot) == 0 || result.Memory == nil || len(result.Memory.Entries) != 0 || result.Memory.Scope != req.Memory.BoundKey || result.Memory.BaseRevision != 7 || result.Usage != (Usage{InputTokens: 14, OutputTokens: 6, TotalTokens: 20}) {
				t.Fatal("explanatory Final missing or failed tool fabricated a Memory entry")
			}
		})
	}
}

func TestExecutorMemoryHTTPCorrectsUnknownIDThroughSDKLoad(t *testing.T) {
	tools := []string{memory.AddToolName, memory.UpdateToolName, memory.LoadToolName}
	var actualID string
	fixture, server := newMemoryHTTPFixture(t, tools,
		memoryHTTPRound{tool: memory.AddToolName, args: `{"memory":"likes green tea"}`},
		memoryHTTPRound{tool: memory.UpdateToolName, args: `{"memory_id":"does-not-exist","memory":"must not create this"}`, before: memoryHTTPAddCheck(0, "likes green tea")},
		memoryHTTPRound{tool: memory.LoadToolName, args: `{"limit":10}`, before: memoryHTTPErrorCheck(1, memory.UpdateToolName, "does-not-exist not found", false)},
		memoryHTTPRound{tool: memory.UpdateToolName, arguments: func() string { return fmt.Sprintf(`{"memory_id":%q,"memory":"likes black tea"}`, actualID) }, before: memoryHTTPLoadCheck(2, []string{"likes green tea"}, &actualID)},
		memoryHTTPRound{tool: memory.LoadToolName, args: `{"limit":10}`, before: func(request map[string]any) error {
			var response memorytool.UpdateMemoryResponse
			if err := decodeMemoryHTTPTool(request, 3, &response); err != nil {
				return err
			}
			if response.Memory != "likes black tea" || response.MemoryID == "" || response.Message == "" {
				return fmt.Errorf("corrected SDK update did not succeed")
			}
			return nil
		}},
		memoryHTTPRound{final: "Corrected the existing Memory", before: memoryHTTPLoadCheck(4, []string{"likes black tea"}, nil)},
	)
	req := memoryHTTPRequest(server.URL, tools)
	req.MaxToolCalls = 5 // Failed SDK calls still consume the published tool budget.
	result, err := executeMemoryHTTP(t, req)
	if err != nil {
		t.Fatalf("model correction loop was escalated to platform failure: %v", err)
	}
	fixture.check(t, 6)
	if actualID == "" || result.FinalText != "Corrected the existing Memory" || len(result.Snapshot) == 0 || result.Memory == nil || result.Memory.Scope != req.Memory.BoundKey || result.Memory.BaseRevision != 7 || len(result.Memory.Entries) != 1 || result.Memory.Entries[0].Memory.Memory != "likes black tea" || result.Usage != (Usage{InputTokens: 42, OutputTokens: 18, TotalTokens: 60}) {
		t.Fatal("SDK correction did not produce the exact complete candidate")
	}
}

func TestExecutorMemoryHTTPBusinessErrorStillConsumesToolBudget(t *testing.T) {
	tools := []string{memory.UpdateToolName, memory.LoadToolName}
	fixture, server := newMemoryHTTPFixture(t, tools,
		memoryHTTPRound{tool: memory.UpdateToolName, args: `{"memory":"missing id"}`},
		memoryHTTPRound{tool: memory.LoadToolName, args: `{"limit":10}`, before: memoryHTTPErrorCheck(0, memory.UpdateToolName, "memory ID is required", false)},
		memoryHTTPRound{final: "No budget remains"},
	)
	req := memoryHTTPRequest(server.URL, tools)
	req.MaxToolCalls = 1
	result, err := executeMemoryHTTP(t, req)
	requireNoMemoryResult(t, result, err)
	fixture.mu.Lock()
	calls, problems := fixture.calls, append([]string(nil), fixture.problems...)
	fixture.mu.Unlock()
	if calls < 2 || calls > 3 || len(problems) > 0 {
		t.Fatalf("calls=%d problems=%v", calls, problems)
	}
}

func TestExecutorMemoryHTTPCancellationDiscardsPriorPrivateWrite(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			http.Error(w, "decode", 400)
			return
		}
		switch calls.Add(1) {
		case 1:
			writeMemoryHTTPResponse(w, 0, memory.AddToolName, `{"memory":"private write before cancellation"}`, "", false)
		case 2:
			if err := memoryHTTPAddCheck(0, "private write before cancellation")(request); err != nil {
				t.Error(err)
				http.Error(w, "missing real tool result", 400)
				return
			}
			close(started)
			select {
			case <-r.Context().Done():
				close(canceled)
			case <-release:
			}
		default:
			t.Error("unexpected model retry after cancellation")
			http.Error(w, "extra request", 400)
		}
	}))
	// Release the fixture even on a failing assertion; do not leave Server.Close
	// waiting forever if cancellation stops propagating in the implementation.
	defer server.Close()
	defer close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type outcome struct {
		result Result
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := testExecutor().Execute(ctx, memoryHTTPRequest(server.URL, []string{memory.AddToolName}))
		done <- outcome{result, err}
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("second actual model request did not start")
	}
	cancel()
	select {
	case got := <-done:
		requireNoMemoryResult(t, got.result, got.err)
		if !errors.Is(got.err, context.Canceled) {
			t.Fatalf("cancellation was not propagated: %v", got.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Memory execution did not drain after cancellation")
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("actual model HTTP context remained live")
	}
	if calls.Load() != 2 {
		t.Fatalf("model calls=%d", calls.Load())
	}
}
