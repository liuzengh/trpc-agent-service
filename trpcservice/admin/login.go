package admin

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/console"
)

const browserCookie = "trpc_admin_session"

var errLogin = errors.New("admin authentication required")

type loginBucket struct {
	Count int
	Until time.Time
}
type loginLimiter struct {
	mu      sync.Mutex
	buckets map[string]loginBucket
}

func (l *loginLimiter) allow(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = address
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if l.buckets == nil {
		l.buckets = map[string]loginBucket{}
	}
	for key, b := range l.buckets {
		if now.After(b.Until) {
			delete(l.buckets, key)
		}
	}
	if len(l.buckets) > 2000 {
		return false
	}
	b := l.buckets[host]
	if b.Until.IsZero() {
		b.Until = now.Add(5 * time.Minute)
	}
	b.Count++
	l.buckets[host] = b
	return b.Count <= 15
}
func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
func principalFingerprint(p Principal) string {
	ids := append([]string(nil), p.TenantIDs...)
	sort.Strings(ids)
	raw, _ := json.Marshal([]any{p.Name, p.Role, ids, p.Token})
	return digest(string(raw))
}
func randomCredential() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
func csrfFor(token string) string { return digest("csrf:" + token) }
func publicIdentity(p Principal, csrf string) map[string]any {
	return map[string]any{"name": p.Name, "role": p.Role, "tenant_ids": p.TenantIDs, "csrf_token": csrf}
}

func secureBrowserRequest(r *http.Request) bool {
	return r.TLS != nil || strings.HasPrefix(r.Header.Get("Origin"), "https://")
}
func localBrowserRequest(r *http.Request) bool {
	host := r.Host
	if value, _, err := net.SplitHostPort(host); err == nil {
		host = value
	}
	host = strings.Trim(host, "[]")
	return host == "localhost" || net.ParseIP(host) != nil && net.ParseIP(host).IsLoopback()
}

func (h *Handler) login(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		adminJSON(w, 405, map[string]string{"error": "method not allowed"})
		return
	}
	if !sameOrigin(r) || r.Header.Get("Origin") == "" {
		adminJSON(w, 403, map[string]string{"error": "同源浏览器登录请求必需"})
		return
	}
	if !secureBrowserRequest(r) && !localBrowserRequest(r) {
		adminJSON(w, 403, map[string]string{"error": "远程管理登录必须使用 HTTPS"})
		return
	}
	if !h.loginLimit.allow(r.RemoteAddr) {
		w.Header().Set("Retry-After", "300")
		adminJSON(w, 429, map[string]string{"error": "登录尝试过多，请稍后重试"})
		return
	}
	var input struct {
		Token string `json:"token"`
	}
	if !decodeAdmin(w, r, &input) {
		return
	}
	p, ok := authenticate(h.principals, "Bearer "+strings.TrimSpace(input.Token))
	if !ok {
		adminJSON(w, 401, map[string]string{"error": "管理凭据无效"})
		return
	}
	token, err := randomCredential()
	if err != nil {
		h.writeResult(w, 0, nil, err)
		return
	}
	data, _ := json.Marshal(map[string]string{"fingerprint": principalFingerprint(p)})
	record, err := h.service.consoleStore.Create(r.Context(), console.Record{Kind: "auth", ID: digest(token), OwnerID: p.Name, Status: "active", Data: data, ExpiresAt: time.Now().UTC().Add(8 * time.Hour)})
	if err != nil {
		h.writeResult(w, 0, nil, err)
		return
	}
	if old, err := r.Cookie(browserCookie); err == nil {
		_ = h.service.consoleStore.Delete(r.Context(), "auth", "", digest(old.Value))
	}
	_ = h.service.consoleStore.Cleanup(r.Context(), "auth")
	http.SetCookie(w, &http.Cookie{Name: browserCookie, Value: token, Path: "/admin/", HttpOnly: true, Secure: secureBrowserRequest(r), SameSite: http.SameSiteStrictMode, MaxAge: 8 * 60 * 60, Expires: record.ExpiresAt})
	adminJSON(w, 200, publicIdentity(p, csrfFor(token)))
}

func (h *Handler) authenticateRequest(r *http.Request) (Principal, string, error) {
	if authorization := r.Header.Get("Authorization"); authorization != "" {
		p, ok := authenticate(h.principals, authorization)
		if ok {
			return p, "", nil
		}
		return Principal{}, "", errLogin
	}
	cookie, err := r.Cookie(browserCookie)
	if err != nil || len(cookie.Value) != 64 {
		return Principal{}, "", errLogin
	}
	record, err := h.service.consoleStore.Get(r.Context(), "auth", "", digest(cookie.Value))
	if err != nil {
		if errors.Is(err, console.ErrNotFound) {
			return Principal{}, "", errLogin
		}
		return Principal{}, "", err
	}
	var data struct {
		Fingerprint string `json:"fingerprint"`
	}
	if json.Unmarshal(record.Data, &data) != nil {
		return Principal{}, "", errLogin
	}
	for _, p := range h.principals {
		if p.Name == record.OwnerID && subtle.ConstantTimeCompare([]byte(data.Fingerprint), []byte(principalFingerprint(p))) == 1 {
			return p, csrfFor(cookie.Value), nil
		}
	}
	return Principal{}, "", errLogin
}

func (h *Handler) logout(w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(browserCookie); err == nil {
		if err := h.service.consoleStore.Delete(r.Context(), "auth", "", digest(c.Value)); err != nil {
			h.writeResult(w, 0, nil, err)
			return
		}
	}
	http.SetCookie(w, &http.Cookie{Name: browserCookie, Value: "", Path: "/admin/", HttpOnly: true, Secure: secureBrowserRequest(r), SameSite: http.SameSiteStrictMode, MaxAge: -1})
	adminJSON(w, 200, map[string]string{"status": "signed_out"})
}
