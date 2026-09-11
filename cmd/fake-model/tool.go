// Tool-mode support for fake-model: a scripted function-calling round and
// the HTTP endpoint the tool binding points at.
//
// Two halves, both needed for the P3 end-to-end drill:
//
//   - /__mode {"mode":"tools","tool":"echo_upstream","tool_args":"{...}"}
//     makes the next completion a streaming tool call. Once the request
//     carries a role=tool message (the platform's real second round), the
//     stream answers with plain text instead — the exact two-request shape a
//     function-calling loop produces.
//   - /__tool/echo is the stub upstream a tool_binding can point at. Its
//     behaviour is switchable (/__tool/mode {"mode":"hang"}) so a drill can
//     turn a healthy tool into one whose result is unknowable, and
//     /__tool/sent reports what actually arrived — the only witness of a
//     tool call that is outside the platform's own database.
package main

import (
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"
)

const (
	toolModePath    = "/__tool/mode"
	toolEchoPath    = "/__tool/echo"
	toolSentPath    = "/__tool/sent"
	toolResetPath   = "/__tool/reset"
	defaultToolName = "echo_upstream"
)

// toolState is the stub upstream's state. hang makes echo block until the
// caller gives up, which is what turns a write-tool call into an Unknown.
type toolState struct {
	mu     sync.Mutex
	mode   string // ok | hang
	hits   int
	bodies []string
}

func (t *toolState) record(body string) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.hits++
	t.bodies = append(t.bodies, body)
	return t.mode
}

func (t *toolState) setMode(mode string) {
	t.mu.Lock()
	t.mode = mode
	t.mu.Unlock()
}

func (t *toolState) snapshot() map[string]any {
	t.mu.Lock()
	defer t.mu.Unlock()
	calls := make([]map[string]string, 0, len(t.bodies))
	for _, b := range t.bodies {
		calls = append(calls, map[string]string{"body": b})
	}
	return map[string]any{"mode": t.mode, "hits": t.hits, "calls": calls}
}

func (t *toolState) reset() {
	t.mu.Lock()
	t.hits = 0
	t.bodies = nil
	t.mu.Unlock()
}

// handleTool routes the stub's own control surface. It is registered under
// /__tool/ so the completion catch-all never sees it (the same routing trap
// the KF stub hit: without an explicit route, /__tool/sent was answered as a
// chat completion).
func (s *server) handleTool(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case toolModePath:
		var p struct {
			Mode string `json:"mode"`
		}
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&p); err != nil {
			writeErr(w, http.StatusBadRequest, "decode body: "+err.Error())
			return
		}
		if p.Mode != "ok" && p.Mode != "hang" {
			writeErr(w, http.StatusBadRequest, `tool mode must be "ok" or "hang"`)
			return
		}
		s.tool.setMode(p.Mode)
		s.log.Printf("tool target mode=%s", p.Mode)
		writeJSON(w, http.StatusOK, s.tool.snapshot())
	case toolEchoPath:
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST the tool arguments here")
			return
		}
		body, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		mode := s.tool.record(string(body))
		s.log.Printf("tool call #%d mode=%s body=%s", s.tool.hits, mode, string(body))
		if mode == "hang" {
			// The point of the mode is to outlive the caller: the platform's
			// tool timeout must classify the result as unknown, and this
			// handler returning the moment the caller goes away proves the
			// cancellation reached it.
			<-r.Context().Done()
			s.log.Printf("tool call: caller gave up (%v)", r.Context().Err())
			return
		}
		var args map[string]any
		_ = json.Unmarshal(body, &args)
		writeJSON(w, http.StatusOK, map[string]any{
			"echo": args["text"],
			"seen": string(body),
		})
	case toolSentPath:
		writeJSON(w, http.StatusOK, s.tool.snapshot())
	case toolResetPath:
		s.tool.reset()
		writeJSON(w, http.StatusOK, s.tool.snapshot())
	default:
		writeErr(w, http.StatusNotFound, "unknown tool path: "+r.URL.Path)
	}
}

// writeToolCallStream emits the streaming shape of one function call: a
// delta carrying the call, then a finish_reason=tool_calls chunk. It mirrors
// what OpenAI emits and what the platform's own client parses, verified by
// the tools_ledger integration test rather than assumed.
func writeToolCallStream(w http.ResponseWriter, flusher http.Flusher, toolName, args string) {
	call, _ := json.Marshal(map[string]any{
		"id":      completionID,
		"object":  "chat.completion.chunk",
		"created": time.Now().Unix(),
		"model":   modelName,
		"choices": []map[string]any{{
			"index": 0,
			"delta": map[string]any{
				"tool_calls": []map[string]any{{
					"index": 0, "id": "call_fake_1", "type": "function",
					"function": map[string]string{"name": toolName, "arguments": args},
				}},
			},
			"finish_reason": nil,
		}},
	})
	writeSSE(w, flusher, event(string(call)))
	done, _ := json.Marshal(map[string]any{
		"id":      completionID,
		"object":  "chat.completion.chunk",
		"created": time.Now().Unix(),
		"model":   modelName,
		"choices": []map[string]any{{"index": 0, "delta": map[string]any{}, "finish_reason": "tool_calls"}},
	})
	writeSSE(w, flusher, event(string(done)))
	writeSSE(w, flusher, "data: [DONE]\n\n")
}
