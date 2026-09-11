package web

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/domain/member"
	"github.com/liuzengh/trpc-agent-service/trpcservice/infra/auth"
	"golang.org/x/crypto/bcrypt"
)

func TestLoginUsesMemberTenantWithoutTenantInput(t *testing.T) {
	hash, err := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	mgr := member.NewManager()
	if err := mgr.Create(context.Background(), &member.Member{
		TenantID: "tenant-a",
		UserID:   "alice",
		Role:     member.RoleMember,
		Password: string(hash),
	}); err != nil {
		t.Fatal(err)
	}

	authMW := NewAuthMiddleware(mgr, "test-secret")
	req := httptest.NewRequest(http.MethodPost, "/auth/login",
		bytes.NewBufferString(`{"user_id":"alice","password":"secret"}`))
	rec := httptest.NewRecorder()
	authMW.Login(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var got LoginResponse
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.Member == nil || got.Member.TenantID != "tenant-a" {
		t.Fatalf("member = %+v, want tenant-a", got.Member)
	}
}

func TestMeReturnsAuthenticatedMemberWithoutPassword(t *testing.T) {
	hash, err := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	mgr := member.NewManager()
	if err := mgr.Create(context.Background(), &member.Member{
		TenantID: "tenant-a",
		UserID:   "alice",
		Role:     member.RoleMember,
		Password: string(hash),
	}); err != nil {
		t.Fatal(err)
	}

	authMW := NewAuthMiddleware(mgr, "test-secret")
	token, err := auth.GenerateToken("test-secret", "tenant-a", "alice", member.RoleMember)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/auth/me", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	authMW.Wrap([]string{"/auth/login"}, http.HandlerFunc(authMW.Me)).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	var got member.Member
	if err := json.NewDecoder(rec.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.UserID != "alice" || got.TenantID != "tenant-a" || got.Role != member.RoleMember {
		t.Fatalf("member = %+v, want authenticated member", got)
	}
	if got.Password != "" {
		t.Fatal("me response must not expose password hash")
	}
}

// A browser CORS preflight (OPTIONS) never carries the Authorization header, so
// the auth middleware must let it through. Otherwise every cross-origin request
// with a Bearer token fails its preflight and the browser blocks the real call
// (the API appears unreachable even though the token is perfectly valid).
func TestAuthMiddlewareLetsPreflightThrough(t *testing.T) {
	authMW := NewAuthMiddleware(member.NewManager(), "test-secret")
	reached := false
	handler := authMW.Wrap([]string{"/auth/login"}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodOptions, "/tenants", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	req.Header.Set("Access-Control-Request-Method", "GET")
	req.Header.Set("Access-Control-Request-Headers", "authorization")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if !reached {
		t.Fatalf("preflight must reach the next handler, got status = %d", rec.Code)
	}
}

// A skip path ending in "/" is a prefix. The IM callback ingress needs this:
// its route carries the binding id as the last segment, so the exact path set is
// not knowable up front — and an unmatched callback would be answered with 401,
// which the platform reads as "this endpoint is not ours".
func TestAuthMiddlewareSkipsPathPrefix(t *testing.T) {
	authMW := NewAuthMiddleware(member.NewManager(), "test-secret")
	reached := ""
	handler := authMW.Wrap([]string{"/healthz", "/webhooks/im/"}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))

	for _, path := range []string{"/webhooks/im/b-1", "/webhooks/im/tenant-2/bot-3"} {
		reached = ""
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
		if reached != path {
			t.Errorf("%s did not reach the handler (status %d)", path, rec.Code)
		}
	}

	// The prefix must not leak: a sibling route still needs a token.
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/webhooks/other", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("/webhooks/other status = %d, want 401", rec.Code)
	}
}

// CORS must wrap the auth middleware so rejection responses (401) still carry
// the CORS headers the browser needs to read the status instead of reporting an
// opaque network error.
func TestCORSWrapsAuthRejection(t *testing.T) {
	authMW := NewAuthMiddleware(member.NewManager(), "test-secret")
	handler := CORS(authMW.Wrap([]string{"/auth/login"}, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})))

	req := httptest.NewRequest(http.MethodGet, "/tenants", nil)
	req.Header.Set("Origin", "http://localhost:5173")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got == "" {
		t.Fatal("401 response must carry Access-Control-Allow-Origin")
	}
}

func TestMemberAPIRejects普通Member(t *testing.T) {
	mgr := member.NewManager()
	api := NewMemberAPI(mgr)
	mux := http.NewServeMux()
	api.Register(mux)

	claims := &auth.Claims{TenantID: "tenant-a", UserID: "alice", Role: member.RoleMember}
	req := httptest.NewRequest(http.MethodGet, "/members", nil)
	req = req.WithContext(context.WithValue(req.Context(), AuthUserKey, claims))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, body = %s, want 403", rec.Code, rec.Body.String())
	}
}

func TestRBACMiddlewareHidesHighPrivilegeRoutesFromMember(t *testing.T) {
	handler := RequireRoutePermission(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	claims := &auth.Claims{TenantID: "tenant-a", UserID: "alice", Role: member.RoleMember}
	req := httptest.NewRequest(http.MethodDelete, "/tenants/tenant-a", nil)
	req = req.WithContext(context.WithValue(req.Context(), AuthUserKey, claims))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rec.Code)
	}
}
