package agent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storagebundle"
	servicetool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/stretchr/testify/require"
	sessioninmemory "trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

// The whole chain, closed and offline: a published revision names two tools, a
// client posts to the real OpenAI adapter, the model answers with tool calls,
// the framework runs the tools, the model sees their results and answers, and
// the answer comes back over SSE.
//
// The load-bearing assertion is on the model's *second* request. A final string
// proves nothing on its own — a model that ignored the tools entirely could
// produce it. What proves the round trip is that the second request carries the
// values the tools computed, in messages tied to the call IDs from the first.
func TestToolCallRoundTripReachesTheModelAndTheClient(t *testing.T) {
	upstream := newToolCallUpstream(t, "two plus three is 5, and you said pong")
	sink := &recordingAuditSink{}
	runtime := toolCallRuntime(t, upstream, sink)

	response := serveChatCompletion(t, runtime, `{
		"model":"test-model","stream":true,
		"messages":[{"role":"user","content":"add 2 and 3, and echo pong"}]
	}`)

	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), "data:")
	require.Contains(t, response.Body.String(), "two plus three is 5")

	// Two model calls, and only two: one that asked for tools and one that saw
	// their results. A third would mean the framework re-asked.
	require.Equal(t, 2, upstream.callCount())

	// Round one offers the tools the revision named, and carries no tool
	// results yet.
	first := upstream.call(t, 0)
	require.Equal(t,
		[]string{servicetool.RefAdd, servicetool.RefEcho}, toolFunctionNames(t, first))
	require.Empty(t, toolMessages(t, first))

	// Round two is the proof. The assistant turn the framework replayed still
	// carries both tool calls...
	second := upstream.call(t, 1)
	require.Equal(t,
		map[string]string{"call-add-1": servicetool.RefAdd, "call-echo-1": servicetool.RefEcho},
		assistantToolCalls(t, second))

	// ...and each is answered by a tool message holding what the tool actually
	// computed. 5 was produced by builtin_add here, not by the fake model.
	results := toolMessages(t, second)
	require.Len(t, results, 2)
	require.Equal(t, float64(5), decodeToolResult(t, results["call-add-1"])["sum"])
	require.Equal(t, "pong", decodeToolResult(t, results["call-echo-1"])["text"])

	// The trail sees both calls from both sides, under the trusted scope of the
	// authenticated request rather than anything in the body — including the
	// platform request id, which arrives here the same way the rest of the scope
	// does: read off the run context, through a real tool call, not passed in.
	events := sink.recorded()
	require.Len(t, events, 4)
	for _, event := range events {
		require.True(t, event.ScopeValid, "%+v", event)
		require.Equal(t, servicetool.AuditScope{
			RequestID:   "request-1",
			TenantID:    "tenant-a",
			AppID:       "assistant",
			PrincipalID: "principal-1",
			SessionID:   "shared-session",
			RevisionID:  "revision-1",
		}, event.Scope)
	}
	require.Equal(t,
		map[string]int{
			servicetool.RefAdd + "/before":  1,
			servicetool.RefAdd + "/after":   1,
			servicetool.RefEcho + "/before": 1,
			servicetool.RefEcho + "/after":  1,
		},
		auditTally(events))
	for _, event := range events {
		if event.Phase != servicetool.AuditPhaseAfter {
			continue
		}
		require.True(t, event.Succeeded, "%+v", event)
		require.True(t, event.DurationValid, "%+v", event)
	}
}

// A model that never stops calling tools must not hold the run — and the
// session lease under it — open indefinitely. The bound is on tool iterations,
// so a model given an unbounded appetite gets exactly maxToolIterations
// executed rounds and one refused one.
func TestToolIterationsAreBounded(t *testing.T) {
	upstream := newToolCallUpstream(t, "")
	upstream.alwaysCallTools = true
	sink := &recordingAuditSink{}
	runtime := toolCallRuntime(t, upstream, sink)

	response := serveChatCompletion(t, runtime, `{
		"model":"test-model","stream":true,
		"messages":[{"role":"user","content":"loop forever"}]
	}`)

	// The run ends rather than hanging. The stream terminates on an error
	// finish reason; the adapter does not forward the framework's explanation,
	// so the client sees that the turn failed but not why.
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Contains(t, response.Body.String(), `"finish_reason":"error"`)
	require.Contains(t, response.Body.String(), "data: [DONE]")

	// maxToolIterations rounds run, the next is refused before any tool is
	// executed, which is one more model call and no more tool calls.
	require.Equal(t, maxToolIterations+1, upstream.callCount())
	require.Len(t, sink.recorded(), maxToolIterations*2*2)
}

func toolCallRuntime(
	t *testing.T,
	upstream *toolCallUpstream,
	sink servicetool.AuditSink,
) *Runtime {
	t.Helper()
	sessionService := sessioninmemory.NewSessionService()
	t.Cleanup(func() { require.NoError(t, sessionService.Close()) })
	revision := toolRevision(
		upstream.server.URL,
		[]string{servicetool.RefAdd, servicetool.RefEcho},
		[]string{servicetool.PolicySafeTools},
	)
	runtime, err := newRuntime(
		context.Background(),
		revision,
		storagebundle.Fixed(storagebundle.Bundle{Session: sessionService}),
		sink,
		entitling(t, revision),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, runtime.Close()) })
	return runtime
}

// toolCallUpstream is a local OpenAI-compatible endpoint that drives a tool
// round trip: its first answer asks for two tool calls, its second is plain
// text. It records every request body so the test can assert what the framework
// sent back.
type toolCallUpstream struct {
	server *httptest.Server
	// alwaysCallTools keeps answering with tool calls, so the iteration bound
	// can be observed.
	alwaysCallTools bool

	mu    sync.Mutex
	calls []map[string]any
}

func newToolCallUpstream(t *testing.T, finalReply string) *toolCallUpstream {
	t.Helper()
	upstream := &toolCallUpstream{}
	upstream.server = httptest.NewServer(http.HandlerFunc(
		func(writer http.ResponseWriter, request *http.Request) {
			var body map[string]any
			require.NoError(t, json.NewDecoder(request.Body).Decode(&body))
			upstream.mu.Lock()
			upstream.calls = append(upstream.calls, body)
			round := len(upstream.calls)
			alwaysCallTools := upstream.alwaysCallTools
			upstream.mu.Unlock()

			writer.Header().Set("Content-Type", "text/event-stream")
			writer.WriteHeader(http.StatusOK)
			writeChunk := func(payload string) {
				_, err := writer.Write([]byte("data: " + payload + "\n\n"))
				require.NoError(t, err)
			}
			if round == 1 || alwaysCallTools {
				// Distinct call IDs per round, so a replayed transcript cannot
				// pass for a fresh one.
				writeChunk(toolCallChunk(round))
				writeChunk(finishChunk("tool_calls"))
			} else {
				writeChunk(`{"id":"chunk-1","object":"chat.completion.chunk",` +
					`"created":1,"model":"test-model","choices":[{"index":0,` +
					`"delta":{"role":"assistant","content":` + quote(finalReply) + `}}]}`)
				writeChunk(finishChunk("stop"))
			}
			writeChunk("[DONE]")
		}))
	t.Cleanup(upstream.server.Close)
	return upstream
}

// toolCallChunk asks for both tools at once, which is also the case the audit
// has to survive: two calls in flight in one turn, correlated by call ID.
func toolCallChunk(round int) string {
	suffix := ""
	if round > 1 {
		suffix = "-" + string(rune('0'+round))
	}
	return `{"id":"chunk-1","object":"chat.completion.chunk","created":1,` +
		`"model":"test-model","choices":[{"index":0,"delta":{"role":"assistant",` +
		`"tool_calls":[` +
		`{"index":0,"id":"call-add-1` + suffix + `","type":"function","function":` +
		`{"name":"` + servicetool.RefAdd + `","arguments":"{\"a\":2,\"b\":3}"}},` +
		`{"index":1,"id":"call-echo-1` + suffix + `","type":"function","function":` +
		`{"name":"` + servicetool.RefEcho + `","arguments":"{\"text\":\"pong\"}"}}` +
		`]}}]}`
}

func finishChunk(reason string) string {
	return `{"id":"chunk-1","object":"chat.completion.chunk","created":1,` +
		`"model":"test-model","choices":[{"index":0,"delta":{},` +
		`"finish_reason":` + quote(reason) + `}]}`
}

func (u *toolCallUpstream) callCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.calls)
}

func (u *toolCallUpstream) call(t *testing.T, index int) map[string]any {
	t.Helper()
	u.mu.Lock()
	defer u.mu.Unlock()
	require.Greater(t, len(u.calls), index, "upstream received %d calls", len(u.calls))
	return u.calls[index]
}

// assistantToolCalls maps call ID to function name for every tool call the
// framework replayed in the assistant turn.
func assistantToolCalls(t *testing.T, body map[string]any) map[string]string {
	t.Helper()
	replayed := map[string]string{}
	for _, entry := range body["messages"].([]any) {
		message := entry.(map[string]any)
		calls, ok := message["tool_calls"].([]any)
		if !ok {
			continue
		}
		for _, call := range calls {
			toolCall := call.(map[string]any)
			function := toolCall["function"].(map[string]any)
			replayed[toolCall["id"].(string)] = function["name"].(string)
		}
	}
	return replayed
}

// toolMessages maps call ID to the tool result content the framework sent back
// to the model.
func toolMessages(t *testing.T, body map[string]any) map[string]string {
	t.Helper()
	results := map[string]string{}
	for _, entry := range body["messages"].([]any) {
		message := entry.(map[string]any)
		if message["role"] != "tool" {
			continue
		}
		callID, ok := message["tool_call_id"].(string)
		require.True(t, ok, "tool message has no tool_call_id: %v", message)
		content, ok := message["content"].(string)
		require.True(t, ok, "tool message content is not a string: %v", message)
		results[callID] = content
	}
	return results
}

func decodeToolResult(t *testing.T, content string) map[string]any {
	t.Helper()
	var decoded map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(content)), &decoded),
		"tool result is not JSON: %q", content)
	return decoded
}

func auditTally(events []servicetool.AuditEvent) map[string]int {
	tally := map[string]int{}
	for _, event := range events {
		tally[event.ToolName+"/"+string(event.Phase)]++
	}
	return tally
}

type recordingAuditSink struct {
	mu     sync.Mutex
	events []servicetool.AuditEvent
}

func (s *recordingAuditSink) Record(_ context.Context, event servicetool.AuditEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

func (s *recordingAuditSink) recorded() []servicetool.AuditEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]servicetool.AuditEvent(nil), s.events...)
}
