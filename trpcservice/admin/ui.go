package admin

import (
	"embed"
	"net/http"
	"net/url"
)

// Only the static login shell is public. All data still requires Admin Bearer
// authorization; tokens stay in browser memory, not cookies or local storage.
//
//go:embed ui/index.html ui/app.js ui/style.css
var uiFiles embed.FS

func sameOrigin(r *http.Request) bool {
	if r.Header.Get("Sec-Fetch-Site") == "cross-site" {
		return false
	}
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host == r.Host && u.User == nil && u.Path == "" && u.RawQuery == "" && u.Fragment == ""
}

func (h *Handler) serveUI(w http.ResponseWriter, r *http.Request) bool {
	files := map[string]struct{ name, contentType string }{
		"/admin/ui/":          {"ui/index.html", "text/html; charset=utf-8"},
		"/admin/ui/app.js":    {"ui/app.js", "application/javascript; charset=utf-8"},
		"/admin/ui/style.css": {"ui/style.css", "text/css; charset=utf-8"},
	}
	if r.URL.Path == "/admin/ui" {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		} else {
			http.Redirect(w, r, "/admin/ui/", http.StatusTemporaryRedirect)
		}
		return true
	}
	file, ok := files[r.URL.Path]
	if !ok {
		return false
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return true
	}
	raw, err := uiFiles.ReadFile(file.name)
	if err != nil {
		http.Error(w, "asset unavailable", 500)
		return true
	}
	w.Header().Set("Content-Type", file.contentType)
	if r.Method != http.MethodHead {
		_, _ = w.Write(raw)
	}
	return true
}
