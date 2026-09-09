// Command mock-model is a deterministic OpenAI-compatible endpoint used by
// the Phase 7 Compose and fault tests. It deliberately has no external
// dependencies and binds to the container interface only.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type server struct {
	mu       sync.Mutex
	delay    time.Duration
	status   int
	requests int
	toolCall bool
	blocked  bool
	active   int
}

func main() {
	addr := flag.String("addr", ":8090", "listen address")
	flag.Parse()
	s := &server{status: http.StatusOK}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.health)
	mux.HandleFunc("/v1/chat/completions", s.completion)
	mux.HandleFunc("/control/delay", s.setDelay)
	mux.HandleFunc("/control/status", s.setStatus)
	mux.HandleFunc("/control/tool-call", s.setToolCall)
	mux.HandleFunc("/control/block", s.setBlock)
	mux.HandleFunc("/control/reset", s.reset)
	mux.HandleFunc("/control/stats", s.stats)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func (s *server) health(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }

func (s *server) completion(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.requests++
	s.active++
	delay, status, toolCall := s.delay, s.status, s.toolCall
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.active--; s.mu.Unlock() }()
	for {
		s.mu.Lock()
		blocked := s.blocked
		s.mu.Unlock()
		if !blocked {
			break
		}
		select {
		case <-time.After(50 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
	}
	var request struct {
		Messages []struct {
			Role    string `json:"role"`
			Content any    `json:"content"`
		} `json:"messages"`
	}
	_ = json.NewDecoder(r.Body).Decode(&request)
	lastRole, lastContent := "", ""
	if len(request.Messages) > 0 {
		last := request.Messages[len(request.Messages)-1]
		lastRole = last.Role
		lastContent, _ = last.Content.(string)
	}
	if delay > 0 {
		t := time.NewTimer(delay)
		select {
		case <-t.C:
		case <-r.Context().Done():
			t.Stop()
			return
		}
	}
	if status != http.StatusOK {
		http.Error(w, `{"error":"mock model failure"}`, status)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	content := "mock response"
	if lastRole == "tool" && strings.Contains(lastContent, "confirmation required: confirm ") {
		content = lastContent
	}
	if lastRole == "tool" && !strings.Contains(lastContent, "confirmation required: confirm ") {
		content = "mock tool completed"
	}
	response := map[string]any{
		"id": "mock-completion", "object": "chat.completion", "created": time.Now().Unix(), "model": "mock-model",
		"choices": []any{map[string]any{"index": 0, "finish_reason": "stop", "message": map[string]any{"role": "assistant", "content": content}}},
		"usage":   map[string]any{"prompt_tokens": 1, "completion_tokens": 2, "total_tokens": 3},
	}
	if toolCall && lastRole != "tool" {
		response["choices"] = []any{map[string]any{"index": 0, "finish_reason": "tool_calls", "message": map[string]any{
			"role": "assistant", "content": nil, "tool_calls": []any{map[string]any{"id": "mock-tool-call", "type": "function", "function": map[string]any{"name": "phase6.dangerous_echo", "arguments": `{"value":"phase7"}`}}},
		}}}
	}
	_ = json.NewEncoder(w).Encode(response)
}

func (s *server) setDelay(w http.ResponseWriter, r *http.Request) {
	value, _ := strconv.Atoi(r.URL.Query().Get("ms"))
	s.mu.Lock()
	s.delay = time.Duration(value) * time.Millisecond
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}
func (s *server) setStatus(w http.ResponseWriter, r *http.Request) {
	value, _ := strconv.Atoi(r.URL.Query().Get("code"))
	if value == 0 {
		value = 200
	}
	s.mu.Lock()
	s.status = value
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}
func (s *server) setToolCall(w http.ResponseWriter, r *http.Request) {
	value := strings.EqualFold(r.URL.Query().Get("enabled"), "true")
	s.mu.Lock()
	s.toolCall = value
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}
func (s *server) setBlock(w http.ResponseWriter, r *http.Request) {
	value := strings.EqualFold(r.URL.Query().Get("enabled"), "true")
	s.mu.Lock()
	s.blocked = value
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}
func (s *server) reset(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	s.delay, s.status, s.requests, s.toolCall, s.blocked = 0, 200, 0, false, false
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}
func (s *server) stats(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{"requests": s.requests, "active": s.active, "delay_ms": s.delay.Milliseconds(), "status": s.status, "tool_call": s.toolCall, "blocked": s.blocked})
}
