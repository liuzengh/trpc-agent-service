// Package console serves the embedded, same-origin operations application.
// API authentication remains at the Admin API boundary; no credentials or
// tenant data are embedded in these public static assets.
package console

import (
	"embed"
	"net/http"
)

//go:embed assets/index.html assets/styles.css assets/app.js assets/favicon.svg
var assets embed.FS

// NewHandler returns an exact-path handler mounted at /console/ without
// StripPrefix. Unknown paths are rejected rather than falling back to HTML.
func NewHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; font-src 'self'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'; object-src 'none'")
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path == "/console" {
			http.Redirect(w, r, "/console/", http.StatusPermanentRedirect)
			return
		}
		var name, contentType string
		switch r.URL.Path {
		case "/console/":
			name, contentType = "index.html", "text/html; charset=utf-8"
		case "/console/styles.css":
			name, contentType = "styles.css", "text/css; charset=utf-8"
		case "/console/app.js":
			name, contentType = "app.js", "text/javascript; charset=utf-8"
		case "/console/favicon.svg":
			name, contentType = "favicon.svg", "image/svg+xml"
		default:
			http.NotFound(w, r)
			return
		}
		body, err := assets.ReadFile("assets/" + name)
		if err != nil {
			http.Error(w, "asset unavailable", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", contentType)
		if r.Method == http.MethodGet {
			_, _ = w.Write(body)
		}
	})
}
