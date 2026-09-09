// Command mock-telegram implements the small Telegram Bot API surface used by
// the adapter and exposes an internal control API for deterministic tests.
package main

import (
	"encoding/base64"
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
	updates  []json.RawMessage
	sent     []map[string]any
	sendCode int
}

func main() {
	addr := flag.String("addr", ":8091", "listen address")
	flag.Parse()
	s := &server{sendCode: 200}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("/control/inject", s.inject)
	mux.HandleFunc("/control/repeat", s.repeat)
	mux.HandleFunc("/control/send-status", s.sendStatus)
	mux.HandleFunc("/control/reset", s.reset)
	mux.HandleFunc("/control/stats", s.stats)
	// Telegram's Bot API concatenates the token directly after /bot, so the
	// route is not a normal /bot/ subtree.
	mux.HandleFunc("/", s.api)
	if err := http.ListenAndServe(*addr, mux); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func (s *server) api(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/bot")
	path = strings.TrimPrefix(path, "")
	idx := strings.Index(path, "/")
	if idx < 0 {
		http.NotFound(w, r)
		return
	}
	method := path[idx+1:]
	switch method {
	case "getMe":
		writeJSON(w, map[string]any{"ok": true, "result": map[string]any{"id": 123, "is_bot": true, "first_name": "Phase7", "username": "phase7_bot"}})
	case "getUpdates":
		s.mu.Lock()
		if len(s.updates) == 0 {
			s.mu.Unlock()
			timer := time.NewTimer(50 * time.Millisecond)
			select {
			case <-r.Context().Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
			s.mu.Lock()
		}
		updates := append([]json.RawMessage(nil), s.updates...)
		s.updates = nil
		s.mu.Unlock()
		result := make([]json.RawMessage, 0, len(updates))
		result = append(result, updates...)
		writeJSON(w, map[string]any{"ok": true, "result": result})
	case "sendMessage":
		if r.Method != http.MethodPost {
			http.Error(w, "method", 405)
			return
		}
		var payload map[string]any
		_ = json.NewDecoder(r.Body).Decode(&payload)
		s.mu.Lock()
		code := s.sendCode
		if code == 200 {
			s.sent = append(s.sent, payload)
		}
		s.mu.Unlock()
		if code != 200 {
			http.Error(w, `{"ok":false,"error_code":500}`, code)
			return
		}
		writeJSON(w, map[string]any{"ok": true, "result": map[string]any{"message_id": len(s.sent), "chat": map[string]any{"id": payload["chat_id"]}, "text": payload["text"]}})
	default:
		http.NotFound(w, r)
	}
}

func (s *server) inject(w http.ResponseWriter, r *http.Request) {
	raw, err := controlBody(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	s.mu.Lock()
	s.updates = append(s.updates, raw)
	s.mu.Unlock()
	w.WriteHeader(204)
}
func (s *server) repeat(w http.ResponseWriter, r *http.Request) {
	raw, err := controlBody(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	s.mu.Lock()
	s.updates = append(s.updates, raw, raw)
	s.mu.Unlock()
	w.WriteHeader(204)
}

// controlBody also accepts a base64url query value so the Windows Compose
// harness can avoid native-argument quote rewriting when posting JSON.
func controlBody(r *http.Request) (json.RawMessage, error) {
	if encoded := r.URL.Query().Get("body_b64"); encoded != "" {
		body, err := base64.RawURLEncoding.DecodeString(encoded)
		if err != nil {
			return nil, err
		}
		var raw json.RawMessage
		if err := json.Unmarshal(body, &raw); err != nil {
			return nil, err
		}
		return raw, nil
	}
	var raw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		return nil, err
	}
	return raw, nil
}
func (s *server) sendStatus(w http.ResponseWriter, r *http.Request) {
	code, _ := strconv.Atoi(r.URL.Query().Get("code"))
	if code == 0 {
		code = 200
	}
	s.mu.Lock()
	s.sendCode = code
	s.mu.Unlock()
	w.WriteHeader(204)
}
func (s *server) reset(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	s.updates, s.sent, s.sendCode = nil, nil, 200
	s.mu.Unlock()
	w.WriteHeader(204)
}
func (s *server) stats(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	writeJSON(w, map[string]any{"queued_updates": len(s.updates), "sent": len(s.sent), "send_code": s.sendCode})
}
func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}
