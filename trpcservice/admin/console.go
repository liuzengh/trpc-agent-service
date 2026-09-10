package admin

import (
	"embed"
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

const adminSessionCookie = "trpc_admin_session"

//go:embed ui/index.html
var consoleAssets embed.FS

// Console adds a browser-safe same-origin session to the Admin API. It does
// not mint credentials: operators paste an existing short-lived Admin token,
// which is verified before being put in an HttpOnly, Strict cookie.
type Console struct {
	API                        http.Handler
	Principals                 PrincipalResolver
	AllowInsecureSessionCookie bool
}

func (c Console) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/admin" || r.URL.Path == "/admin/":
		c.index(w, r)
	case r.URL.Path == "/admin/session" && r.Method == http.MethodPost:
		c.login(w, r)
	case r.URL.Path == "/admin/session" && r.Method == http.MethodDelete:
		http.SetCookie(w, &http.Cookie{Name: adminSessionCookie, Value: "", Path: "/", MaxAge: -1, HttpOnly: true, SameSite: http.SameSiteStrictMode})
		w.WriteHeader(http.StatusNoContent)
	case strings.HasPrefix(r.URL.Path, "/v1/"):
		c.api(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (c Console) index(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	data, err := consoleAssets.ReadFile("ui/index.html")
	if err != nil {
		http.Error(w, "admin UI unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; connect-src 'self'; base-uri 'none'; frame-ancestors 'none'")
	_, _ = w.Write(data)
}

func (c Console) login(w http.ResponseWriter, r *http.Request) {
	if c.Principals == nil {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	body := http.MaxBytesReader(w, r.Body, 12<<10)
	defer body.Close()
	var input struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(body).Decode(&input); err != nil || strings.TrimSpace(input.Token) == "" {
		http.Error(w, "invalid token", http.StatusBadRequest)
		return
	}
	probe := r.Clone(r.Context())
	probe.Header = r.Header.Clone()
	probe.Header.Set("Authorization", "Bearer "+strings.TrimSpace(input.Token))
	if _, err := c.Principals.Resolve(probe); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	// TLS commonly terminates at the ingress, so r.TLS is not a reliable
	// signal. Secure is the safe default; the role may explicitly opt into an
	// insecure cookie only for a local development environment.
	http.SetCookie(w, &http.Cookie{Name: adminSessionCookie, Value: strings.TrimSpace(input.Token), Path: "/", HttpOnly: true, Secure: !c.AllowInsecureSessionCookie, SameSite: http.SameSiteStrictMode, MaxAge: int((15 * time.Minute).Seconds())})
	w.WriteHeader(http.StatusNoContent)
}

func (c Console) api(w http.ResponseWriter, r *http.Request) {
	if c.API == nil || c.Principals == nil {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	if r.Header.Get("Authorization") == "" {
		cookie, err := r.Cookie(adminSessionCookie)
		if err != nil || cookie.Value == "" {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodGet && !sameOrigin(r) {
			http.Error(w, "Forbidden", http.StatusForbidden)
			return
		}
		r = r.Clone(r.Context())
		r.Header = r.Header.Clone()
		r.Header.Set("Authorization", "Bearer "+cookie.Value)
	}
	c.API.ServeHTTP(w, r)
}

func sameOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return false
	}
	return origin == "http://"+r.Host || origin == "https://"+r.Host
}
