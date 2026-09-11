// This is a disposable OpenAI-compatible model endpoint for the Compose
// production gate. It never logs or persists request content or credentials.
package main

import (
	"encoding/json"
	"io"
	"net/http"
	"time"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/v1/chat/completions", completions)
	if err := http.ListenAndServe(":8090", mux); err != nil {
		panic(err)
	}
}

func completions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	// Consume a bounded request body without retaining or logging its contents.
	_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 1<<20))
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id":      "chatcmpl-production-acceptance",
		"object":  "chat.completion",
		"created": time.Now().Unix(),
		"model":   "acceptance-model",
		"choices": []any{map[string]any{
			"index": 0,
			"message": map[string]string{
				"role":    "assistant",
				"content": "production acceptance model response",
			},
			"finish_reason": "stop",
		}},
		"usage": map[string]int{
			"prompt_tokens":     12,
			"completion_tokens": 6,
			"total_tokens":      18,
		},
	})
}
