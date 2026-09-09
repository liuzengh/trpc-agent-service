// Package web serves admin and chat pages for the Agent platform.
package web

import (
	"bytes"
	_ "embed"
	"net/http"
	"time"
)

//go:embed index.html
var indexHTML []byte

// Handler serves the embedded WebUI.
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		http.ServeContent(w, r, "index.html", time.Time{}, bytes.NewReader(indexHTML))
	})
}
