// Package web serves the admin/chat pages of the platform.
package web

import (
	"embed"
	"encoding/json"
	"net/http"
)

//go:embed index.html
var static embed.FS

// NewServer assembles the public HTTP handler: the chat page at "/", the
// tenant list API, and the channel gateway under /callback and /webchat.
func NewServer(gateway http.Handler, tenants []string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		data, err := static.ReadFile("index.html")
		if err != nil {
			http.Error(w, "page unavailable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(data)
	})
	mux.HandleFunc("/api/tenants", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(tenants)
	})
	mux.Handle("/callback/", gateway)
	mux.Handle("/webchat/", gateway)
	return mux
}
