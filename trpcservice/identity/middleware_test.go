package identity

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type failingSessionPrincipalResolver struct{ err error }

func (r failingSessionPrincipalResolver) ResolveSessionUser(context.Context, string) (SessionUser, error) {
	return SessionUser{}, r.err
}

// authzHandler represents a session- and CSRF-protected API endpoint.
func authzHandler(store SessionStore, csrfEnabled bool) http.Handler {
	return authzHandlerWithAudit(store, nil, csrfEnabled)
}

func authzHandlerWithAudit(store SessionStore, audit AuditRecorder, csrfEnabled bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/protected", func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	var next http.Handler = mux
	next = SessionMiddleware(store, nil, audit, next)
	if csrfEnabled {
		next = CSRFMiddleware(next)
	}
	return next
}

func newSessionFor(t *testing.T, store SessionStore) string {
	t.Helper()
	id, err := store.Create(context.Background(), SessionUser{PlatformUserID: "platform-u1", IsSystemAdmin: true}, time.Hour)
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	return id
}

func TestSessionMiddlewareRejectsMissingSession(t *testing.T) {
	store := NewMemorySessionStore()
	handler := authzHandler(store, false)

	request := httptest.NewRequest(http.MethodGet, "/protected", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", recorder.Code)
	}
}

func TestSessionMiddlewareAcceptsValidSession(t *testing.T) {
	store := NewMemorySessionStore()
	handler := authzHandler(store, false)
	sessionID := newSessionFor(t, store)

	request := httptest.NewRequest(http.MethodGet, "/protected", nil)
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: sessionID})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
}

func TestSessionMiddlewareDoesNotInvalidateSessionOnResolverInfrastructureFailure(t *testing.T) {
	store := NewMemorySessionStore()
	sessionID := newSessionFor(t, store)
	mux := http.NewServeMux()
	mux.HandleFunc("/protected", func(writer http.ResponseWriter, _ *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})
	handler := SessionMiddleware(store, failingSessionPrincipalResolver{err: errors.New("database schema mismatch")}, nil, mux)

	request := httptest.NewRequest(http.MethodGet, "/protected", nil)
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: sessionID})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("resolver infrastructure failure status = %d, want 500", recorder.Code)
	}
	if _, err := store.Get(context.Background(), sessionID); err != nil {
		t.Fatalf("resolver infrastructure failure invalidated otherwise valid session: %v", err)
	}
}

func TestSessionMiddlewareInvalidatesSessionWhenPrincipalIsGone(t *testing.T) {
	for _, principalErr := range []error{ErrPlatformUserNotFound, ErrPlatformUserSuspended} {
		store := NewMemorySessionStore()
		sessionID := newSessionFor(t, store)
		mux := http.NewServeMux()
		mux.HandleFunc("/protected", func(writer http.ResponseWriter, _ *http.Request) {
			writer.WriteHeader(http.StatusOK)
		})
		handler := SessionMiddleware(store, failingSessionPrincipalResolver{err: principalErr}, nil, mux)

		request := httptest.NewRequest(http.MethodGet, "/protected", nil)
		request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: sessionID})
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)

		if recorder.Code != http.StatusUnauthorized {
			t.Fatalf("principal error %v status = %d, want 401", principalErr, recorder.Code)
		}
		if _, err := store.Get(context.Background(), sessionID); err == nil {
			t.Fatalf("principal error %v left invalid session usable", principalErr)
		}
	}
}

func TestSessionMiddlewareRejectsExpiredSession(t *testing.T) {
	store := NewMemorySessionStore()
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	handler := authzHandler(store, false)
	sessionID := newSessionFor(t, store)

	now = now.Add(2 * time.Hour) // past TTL
	request := httptest.NewRequest(http.MethodGet, "/protected", nil)
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: sessionID})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expired session status = %d, want 401", recorder.Code)
	}
}

func TestSessionMiddlewareRecordsExpireAudit(t *testing.T) {
	store := NewMemorySessionStore()
	audits := NewMemoryIdentityStore()
	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	handler := authzHandlerWithAudit(store, audits, false)
	sessionID := newSessionFor(t, store)

	now = now.Add(2 * time.Hour) // past TTL → session_expire audit
	request := httptest.NewRequest(http.MethodGet, "/protected", nil)
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: sessionID})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expired session status = %d, want 401", recorder.Code)
	}
	found := false
	for _, event := range audits.Audits() {
		if event.Action == actionSessionExpire {
			found = true
		}
	}
	if !found {
		t.Fatal("session_expire audit was not recorded")
	}
}

func TestSessionMiddlewareSkipsMissingCookieAudit(t *testing.T) {
	store := NewMemorySessionStore()
	audits := NewMemoryIdentityStore()
	handler := authzHandlerWithAudit(store, audits, false)

	request := httptest.NewRequest(http.MethodGet, "/protected", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", recorder.Code)
	}
	// A first visit without credentials is not a session-expiration event.
	if events := audits.Audits(); len(events) != 0 {
		t.Fatalf("audit events = %d, want 0 for missing cookie", len(events))
	}
}

func TestCSRFMiddlewareRejectsMissingAndWrongHeader(t *testing.T) {
	store := NewMemorySessionStore()
	handler := authzHandler(store, true)
	sessionID := newSessionFor(t, store)

	// Missing X-CSRF-Token.
	request := httptest.NewRequest(http.MethodPost, "/protected", nil)
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: sessionID})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("missing CSRF status = %d, want 403", recorder.Code)
	}

	// Mismatch between cookie and header.
	request = httptest.NewRequest(http.MethodPost, "/protected", nil)
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: sessionID})
	request.AddCookie(&http.Cookie{Name: csrfCookieName, Value: "cookie-token"})
	request.Header.Set("X-CSRF-Token", "header-token")
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("mismatched CSRF status = %d, want 403", recorder.Code)
	}
}

func TestCSRFMiddlewareAcceptsMatchingDoubleSubmit(t *testing.T) {
	store := NewMemorySessionStore()
	handler := authzHandler(store, true)
	sessionID := newSessionFor(t, store)

	// Simulate Double-Submit: cookie and header carry the same rotating token.
	token := "csrf-token-123"
	request := httptest.NewRequest(http.MethodPost, "/protected", nil)
	request.AddCookie(&http.Cookie{Name: sessionCookieName, Value: sessionID})
	request.AddCookie(&http.Cookie{Name: csrfCookieName, Value: token})
	request.Header.Set("X-CSRF-Token", token)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("matching CSRF status = %d, want 200", recorder.Code)
	}
}

func TestSessionCookieAttributes(t *testing.T) {
	attributes := SessionCookieAttributes(false)
	if !strings.Contains(attributes, "HttpOnly") || !strings.Contains(attributes, "SameSite=Lax") {
		t.Fatalf("cookie attributes = %q, want HttpOnly+SameSite=Lax", attributes)
	}
	if strings.Contains(attributes, "Secure") {
		t.Fatal("HTTP mode cookie must not include Secure")
	}
	secure := SessionCookieAttributes(true)
	if !strings.Contains(secure, "Secure") {
		t.Fatal("HTTPS mode cookie must include Secure")
	}
}

func TestSessionMiddlewareRejectsBrowserSessionAsBearerToken(t *testing.T) {
	store := NewMemorySessionStore()
	handler := authzHandler(store, false)
	sessionID := newSessionFor(t, store)

	request := httptest.NewRequest(http.MethodGet, "/protected", nil)
	request.Header.Set("Authorization", "Bearer "+sessionID)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 because browser session IDs are cookie-only", recorder.Code)
	}
}

func TestCSRFMiddlewareDoesNotExemptBearerHeader(t *testing.T) {
	store := NewMemorySessionStore()
	handler := authzHandler(store, true)
	sessionID := newSessionFor(t, store)

	// A Bearer header cannot turn a browser session into an API credential or
	// bypass CSRF protection.
	request := httptest.NewRequest(http.MethodPost, "/protected", nil)
	request.Header.Set("Authorization", "Bearer "+sessionID)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 without the CSRF double-submit proof", recorder.Code)
	}
}
