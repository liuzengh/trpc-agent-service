package admin

import (
	"embed"
	"io/fs"
	"net/http"
	"net/url"
	"strings"
)

// Only built static assets are public. All API data requires a scoped session
// (HttpOnly Cookie + CSRF) or the existing Admin Bearer credentials.
//
//go:embed ui
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
	if r.URL.Path == "/admin/ui" {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		} else {
			http.Redirect(w, r, "/admin/ui/", http.StatusTemporaryRedirect)
		}
		return true
	}
	if !strings.HasPrefix(r.URL.Path, "/admin/ui/") {
		return false
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "no-referrer")
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return true
	}
	path := strings.TrimPrefix(r.URL.Path, "/admin/ui/")
	name, contentType := "", ""
	switch {
	case path == "":
		name = "ui/dist/index.html"
		contentType = "text/html; charset=utf-8"
	case strings.HasPrefix(path, "assets/") && fs.ValidPath(path):
		name = "ui/dist/" + path
		switch {
		case strings.HasSuffix(path, ".js"):
			contentType = "application/javascript; charset=utf-8"
		case strings.HasSuffix(path, ".css"):
			contentType = "text/css; charset=utf-8"
		case strings.HasSuffix(path, ".svg"):
			contentType = "image/svg+xml"
		case strings.HasSuffix(path, ".woff2"):
			contentType = "font/woff2"
		}
	}
	if name == "" || contentType == "" {
		http.NotFound(w, r)
		return true
	}
	nonce, err := randomCredential()
	if err != nil {
		http.Error(w, "asset unavailable", 500)
		return true
	}
	w.Header().Set("Content-Security-Policy", "default-src 'none'; script-src 'self'; style-src 'self' 'nonce-"+nonce+"'; style-src-attr 'unsafe-inline'; img-src 'self' data:; font-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
	raw, err := uiFiles.ReadFile(name)
	if err != nil {
		if path == "" {
			http.Error(w, "Console assets are not built. Run ./scripts/build.sh before starting the service.", http.StatusServiceUnavailable)
		} else {
			http.NotFound(w, r)
		}
		return true
	}
	if path == "" {
		raw = []byte(strings.ReplaceAll(string(raw), "__CSP_NONCE__", nonce))
	}
	w.Header().Set("Content-Type", contentType)
	if r.Method != http.MethodHead {
		_, _ = w.Write(raw)
	}
	return true
}
