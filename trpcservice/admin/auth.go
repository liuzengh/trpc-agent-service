package admin

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/argon2"
)

var (
	// ErrUnauthenticated reports a missing or invalid admin credential.
	ErrUnauthenticated = errors.New("admin authentication required")
	// ErrForbidden reports a principal outside the requested tenant scope.
	ErrForbidden = errors.New("admin principal is outside tenant scope")
)

// Principal is the proof-bearing identity for the control-plane API. A global
// principal represents the explicitly configured platform-admin wildcard.
type Principal struct {
	SubjectID    string
	TenantScopes map[string]struct{}
	Global       bool
}

// ScopeIDs returns a stable, defensive list for the /me response.
func (p Principal) ScopeIDs() []string {
	if p.Global {
		return []string{"*"}
	}
	values := make([]string, 0, len(p.TenantScopes))
	for scope := range p.TenantScopes {
		values = append(values, scope)
	}
	sort.Strings(values)
	return values
}

// Allows reports whether the principal may access the tenant operation.
func (p Principal) Allows(tenantID string, creating bool) bool {
	if creating {
		return p.Global
	}
	if tenantID == "" {
		return false
	}
	if p.Global {
		return true
	}
	_, ok := p.TenantScopes[tenantID]
	return ok
}

// Authenticator is deliberately separate from gateway.APIAuthenticator so a
// chat token can never be upgraded into an Admin principal by path routing.
type Authenticator interface {
	Authenticate(context.Context, *http.Request) (Principal, error)
}

// StaticAuthenticator is the production bootstrap's small explicit token
// boundary. A later identity provider can implement Authenticator without
// changing handlers or domain repositories.
type StaticAuthenticator struct {
	token     string
	principal Principal
}

// AdminSessionCookie is the browser-only session cookie name. It is not a
// bearer credential and is deliberately scoped to the same-origin admin API.
const AdminSessionCookie = "trpc_admin_session"

const defaultSessionTTL = 8 * time.Hour

type sessionPayload struct {
	ExpiresAtUnix     int64  `json:"e"`
	Nonce             string `json:"n"`
	CredentialVersion string `json:"v"`
}

// SessionAuthenticator provides the browser authentication boundary for the
// admin console. The optional static authenticator is retained as a private
// service-to-service compatibility path; browsers never receive its token.
type SessionAuthenticator struct {
	username          string
	passwordVerifier  [sha256.Size]byte
	signingKey        [sha256.Size]byte
	credentialVersion [sha256.Size]byte
	static            *StaticAuthenticator
	ttl               time.Duration

	now  func() time.Time
	rand func([]byte) error
}

// NewSessionAuthenticator creates an account/password authenticator that
// signs self-contained browser sessions. Every replica with the same configured
// credentials can verify a session without a shared process-local session map.
func NewSessionAuthenticator(username, password string, static *StaticAuthenticator) (*SessionAuthenticator, error) {
	username = strings.TrimSpace(username)
	if username == "" || password == "" || strings.ContainsAny(username, "\r\n") || strings.ContainsAny(password, "\r\n") {
		return nil, ErrUnauthenticated
	}
	if static == nil {
		return nil, ErrUnauthenticated
	}
	passwordVerifier := derivePasswordVerifier(password, static.token)
	return &SessionAuthenticator{
		username:          username,
		passwordVerifier:  passwordVerifier,
		signingKey:        deriveSessionSigningKey(static.token),
		credentialVersion: deriveCredentialVersion(static.token, passwordVerifier),
		static:            static,
		ttl:               defaultSessionTTL,
		now:               time.Now,
		rand:              secureRandom,
	}, nil
}

// Authenticate accepts a valid session cookie, or the configured static
// bearer token when no browser session is present.
func (a *SessionAuthenticator) Authenticate(ctx context.Context, request *http.Request) (Principal, error) {
	if a == nil || request == nil || ctx == nil {
		return Principal{}, ErrUnauthenticated
	}
	if err := ctx.Err(); err != nil {
		return Principal{}, err
	}
	if cookie, err := request.Cookie(AdminSessionCookie); err == nil && cookie.Value != "" {
		principal, ok := a.sessionPrincipal(cookie.Value)
		if !ok {
			return Principal{}, ErrUnauthenticated
		}
		if request.Method != http.MethodGet && request.Method != http.MethodHead && !sameOrigin(request) {
			return Principal{}, ErrForbidden
		}
		return principal, nil
	}
	if a.static != nil {
		return a.static.Authenticate(ctx, request)
	}
	return Principal{}, ErrUnauthenticated
}

// ServeHTTP exposes /admin/auth/login, /admin/auth/session and
// /admin/auth/logout. It is intentionally separate from /admin/v1 so the
// resource handler can keep its existing proof-bearing Authenticator API.
func (a *SessionAuthenticator) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request == nil {
		writeError(writer, uuid.NewString(), http.StatusBadRequest, "invalid_request")
		return
	}
	requestID := strings.TrimSpace(request.Header.Get("X-Request-ID"))
	if requestID == "" {
		requestID = uuid.NewString()
	}
	switch request.URL.Path {
	case "/admin/auth/login":
		if request.Method != http.MethodPost {
			writer.Header().Set("Allow", http.MethodPost)
			writeError(writer, requestID, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		a.login(writer, request, requestID)
	case "/admin/auth/session":
		if request.Method != http.MethodGet {
			writer.Header().Set("Allow", http.MethodGet)
			writeError(writer, requestID, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		a.session(writer, request, requestID)
	case "/admin/auth/logout":
		if request.Method != http.MethodPost {
			writer.Header().Set("Allow", http.MethodPost)
			writeError(writer, requestID, http.StatusMethodNotAllowed, "method_not_allowed")
			return
		}
		a.logout(writer, request, requestID)
	default:
		writeError(writer, requestID, http.StatusNotFound, "not_found")
	}
}

func (a *SessionAuthenticator) login(writer http.ResponseWriter, request *http.Request, requestID string) {
	if !sameOriginOrNoOrigin(request) {
		writeError(writer, requestID, http.StatusForbidden, "forbidden")
		return
	}
	var input struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := decodeAuthBody(request, &input); err != nil {
		writeError(writer, requestID, http.StatusBadRequest, "invalid_request")
		return
	}
	if !constantTimeEqual(input.Username, a.username) || !constantTimeDigestEqual(input.Password, a.passwordVerifier, a.static.token) {
		writeError(writer, requestID, http.StatusUnauthorized, "unauthorized")
		return
	}
	token, err := a.newSession()
	if err != nil {
		logRequestFailure(requestID, http.StatusInternalServerError, "internal_error", err)
		writeError(writer, requestID, http.StatusInternalServerError, "internal_error")
		return
	}
	http.SetCookie(writer, &http.Cookie{
		Name: AdminSessionCookie, Value: token, Path: "/", HttpOnly: true,
		Secure: requestIsHTTPS(request), SameSite: http.SameSiteLaxMode,
		MaxAge: int(a.ttl.Seconds()), Expires: a.now().Add(a.ttl),
	})
	writeJSON(writer, requestID, http.StatusOK, principalResponse(a.static.principal))
}

func constantTimeEqual(left, right string) bool {
	leftDigest := sha256.Sum256([]byte(left))
	rightDigest := sha256.Sum256([]byte(right))
	return subtle.ConstantTimeCompare(leftDigest[:], rightDigest[:]) == 1
}

func constantTimeDigestEqual(value string, expected [sha256.Size]byte, staticToken string) bool {
	digest := derivePasswordVerifier(value, staticToken)
	return subtle.ConstantTimeCompare(digest[:], expected[:]) == 1
}

func (a *SessionAuthenticator) session(writer http.ResponseWriter, request *http.Request, requestID string) {
	principal, err := a.Authenticate(request.Context(), request)
	if err != nil {
		writeError(writer, requestID, http.StatusUnauthorized, "unauthorized")
		return
	}
	writeJSON(writer, requestID, http.StatusOK, principalResponse(principal))
}

func (a *SessionAuthenticator) logout(writer http.ResponseWriter, request *http.Request, requestID string) {
	if !sameOrigin(request) {
		writeError(writer, requestID, http.StatusForbidden, "forbidden")
		return
	}
	http.SetCookie(writer, &http.Cookie{Name: AdminSessionCookie, Value: "", Path: "/", HttpOnly: true, Secure: requestIsHTTPS(request), SameSite: http.SameSiteLaxMode, MaxAge: -1, Expires: time.Unix(1, 0).UTC()})
	writeJSON(writer, requestID, http.StatusOK, map[string]any{"logged_out": true})
}

func (a *SessionAuthenticator) newSession() (string, error) {
	nonce := make([]byte, 32)
	random := a.rand
	if random == nil {
		random = secureRandom
	}
	if err := random(nonce); err != nil {
		return "", err
	}
	payload, err := json.Marshal(sessionPayload{
		ExpiresAtUnix:     a.now().Add(a.ttl).Unix(),
		Nonce:             base64.RawURLEncoding.EncodeToString(nonce),
		CredentialVersion: base64.RawURLEncoding.EncodeToString(a.credentialVersion[:]),
	})
	if err != nil {
		return "", err
	}
	encodedPayload := base64.RawURLEncoding.EncodeToString(payload)
	return encodedPayload + "." + a.signSessionPayload(encodedPayload), nil
}

func (a *SessionAuthenticator) sessionPrincipal(token string) (Principal, bool) {
	encodedPayload, signature, ok := strings.Cut(token, ".")
	if !ok || encodedPayload == "" || signature == "" || strings.Contains(signature, ".") ||
		!hmac.Equal([]byte(signature), []byte(a.signSessionPayload(encodedPayload))) {
		return Principal{}, false
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(encodedPayload)
	if err != nil {
		return Principal{}, false
	}
	var payload sessionPayload
	decoder := json.NewDecoder(strings.NewReader(string(payloadBytes)))
	if err := decoder.Decode(&payload); err != nil || decoder.Decode(&struct{}{}) != io.EOF || payload.ExpiresAtUnix <= 0 || payload.Nonce == "" || payload.CredentialVersion == "" {
		return Principal{}, false
	}
	nonce, err := base64.RawURLEncoding.DecodeString(payload.Nonce)
	if err != nil || len(nonce) != 32 {
		return Principal{}, false
	}
	credentialVersion, err := base64.RawURLEncoding.DecodeString(payload.CredentialVersion)
	if err != nil || subtle.ConstantTimeCompare(credentialVersion, a.credentialVersion[:]) != 1 {
		return Principal{}, false
	}
	if !a.now().Before(time.Unix(payload.ExpiresAtUnix, 0)) {
		return Principal{}, false
	}
	return a.static.principal, true
}

func (a *SessionAuthenticator) signSessionPayload(payload string) string {
	mac := hmac.New(sha256.New, a.signingKey[:])
	_, _ = mac.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func deriveSessionSigningKey(staticToken string) [sha256.Size]byte {
	return sha256.Sum256([]byte("trpc-admin-session-signing-key\x00" + staticToken))
}

func derivePasswordVerifier(password, staticToken string) [sha256.Size]byte {
	salt := sha256.Sum256([]byte("trpc-admin-password-salt\x00" + staticToken))
	derived := argon2.IDKey([]byte(password), salt[:], 3, 64*1024, 2, sha256.Size)
	var verifier [sha256.Size]byte
	copy(verifier[:], derived)
	return verifier
}

func deriveCredentialVersion(staticToken string, passwordVerifier [sha256.Size]byte) [sha256.Size]byte {
	mac := hmac.New(sha256.New, []byte(staticToken))
	_, _ = mac.Write([]byte("trpc-admin-credential-version\x00"))
	_, _ = mac.Write(passwordVerifier[:])
	var version [sha256.Size]byte
	copy(version[:], mac.Sum(nil))
	return version
}

func decodeAuthBody(request *http.Request, target any) error {
	if request == nil || request.Body == nil {
		return errors.New("request body is required")
	}
	decoder := json.NewDecoder(io.LimitReader(request.Body, 1<<20))
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return nil
}

func principalResponse(principal Principal) map[string]any {
	return map[string]any{
		"subject_id": principal.SubjectID, "global": principal.Global,
		"tenant_scopes": principal.ScopeIDs(), "can_create_tenant": principal.Global,
	}
}

func sameOriginOrNoOrigin(request *http.Request) bool {
	if request == nil {
		return false
	}
	if strings.TrimSpace(request.Header.Get("Origin")) == "" && strings.TrimSpace(request.Header.Get("Referer")) == "" {
		return true
	}
	return sameOrigin(request)
}

func sameOrigin(request *http.Request) bool {
	if request == nil {
		return false
	}
	value := strings.TrimSpace(request.Header.Get("Origin"))
	if value == "" {
		value = strings.TrimSpace(request.Header.Get("Referer"))
	}
	if value == "" {
		return false
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.Host == "" {
		return false
	}
	host := request.Host
	if host == "" {
		host = request.URL.Host
	}
	scheme := requestScheme(request)
	return strings.EqualFold(parsed.Host, host) && strings.EqualFold(parsed.Scheme, scheme)
}

func requestIsHTTPS(request *http.Request) bool { return requestScheme(request) == "https" }

func requestScheme(request *http.Request) string {
	if request != nil && request.TLS != nil {
		return "https"
	}
	if request != nil {
		if forwarded := strings.TrimSpace(strings.Split(request.Header.Get("X-Forwarded-Proto"), ",")[0]); strings.EqualFold(forwarded, "https") {
			return "https"
		}
	}
	return "http"
}

func secureRandom(buffer []byte) error {
	_, err := rand.Read(buffer)
	return err
}

// NewStaticAuthenticator creates a bearer-token authenticator with fixed scopes.
func NewStaticAuthenticator(token string, scopes []string) (*StaticAuthenticator, error) {
	token = strings.TrimSpace(token)
	if token == "" || strings.ContainsAny(token, "\r\n") {
		return nil, ErrUnauthenticated
	}
	principal := Principal{SubjectID: "admin", TenantScopes: map[string]struct{}{}}
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if scope == "*" {
			principal.Global = true
			continue
		}
		if scope != "" {
			principal.TenantScopes[scope] = struct{}{}
		}
	}
	if !principal.Global && len(principal.TenantScopes) == 0 {
		return nil, ErrForbidden
	}
	return &StaticAuthenticator{token: token, principal: principal}, nil
}

// Authenticate validates the request bearer token and returns its principal.
func (a *StaticAuthenticator) Authenticate(ctx context.Context, request *http.Request) (Principal, error) {
	if a == nil || request == nil || ctx == nil {
		return Principal{}, ErrUnauthenticated
	}
	if err := ctx.Err(); err != nil {
		return Principal{}, err
	}
	header := request.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") || strings.TrimSpace(strings.TrimPrefix(header, "Bearer ")) != a.token {
		return Principal{}, ErrUnauthenticated
	}
	return a.principal, nil
}
