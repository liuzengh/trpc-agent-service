package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSessionAuthenticatorLoginSessionLogoutAndExpiry(t *testing.T) {
	static, err := NewStaticAuthenticator("service-token", []string{"tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	auth, err := NewSessionAuthenticator("operator", "secret", static)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	auth.now = func() time.Time { return now }
	login := httptest.NewRequest(http.MethodPost, "http://admin.test/admin/auth/login", bytes.NewBufferString(`{"username":"operator","password":"secret"}`))
	login.Header.Set("Content-Type", "application/json")
	login.Header.Set("Origin", "http://admin.test")
	recorder := httptest.NewRecorder()
	auth.ServeHTTP(recorder, login)
	if recorder.Code != http.StatusOK {
		t.Fatalf("login status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	cookie := recorder.Result().Cookies()[0]
	if cookie.Name != AdminSessionCookie || !cookie.HttpOnly || cookie.Value == "" {
		t.Fatalf("session cookie=%+v", cookie)
	}
	if cookie.Secure {
		t.Fatalf("plain HTTP login unexpectedly created a secure cookie=%+v", cookie)
	}
	var envelope struct {
		Data struct {
			SubjectID    string   `json:"subject_id"`
			TenantScopes []string `json:"tenant_scopes"`
		} `json:"data"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Data.SubjectID != "admin" || len(envelope.Data.TenantScopes) != 1 || envelope.Data.TenantScopes[0] != "tenant-a" {
		t.Fatalf("principal envelope=%+v", envelope.Data)
	}

	session := httptest.NewRequest(http.MethodGet, "http://admin.test/admin/auth/session", nil)
	session.AddCookie(cookie)
	recorder = httptest.NewRecorder()
	auth.ServeHTTP(recorder, session)
	if recorder.Code != http.StatusOK {
		t.Fatalf("session status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	logout := httptest.NewRequest(http.MethodPost, "http://admin.test/admin/auth/logout", nil)
	logout.AddCookie(cookie)
	logout.Header.Set("Origin", "http://admin.test")
	recorder = httptest.NewRecorder()
	auth.ServeHTTP(recorder, logout)
	if recorder.Code != http.StatusOK || recorder.Result().Cookies()[0].MaxAge != -1 {
		t.Fatalf("logout status=%d cookies=%v", recorder.Code, recorder.Result().Cookies())
	}
	unauthorized := httptest.NewRequest(http.MethodGet, "http://admin.test/admin/auth/session", nil)
	recorder = httptest.NewRecorder()
	auth.ServeHTTP(recorder, unauthorized)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("session without cleared cookie status=%d", recorder.Code)
	}

	login = httptest.NewRequest(http.MethodPost, "http://admin.test/admin/auth/login", bytes.NewBufferString(`{"username":"operator","password":"secret"}`))
	login.Header.Set("Content-Type", "application/json")
	recorder = httptest.NewRecorder()
	auth.ServeHTTP(recorder, login)
	cookie = recorder.Result().Cookies()[0]
	now = now.Add(defaultSessionTTL + time.Second)
	expired := httptest.NewRequest(http.MethodGet, "http://admin.test/admin/auth/session", nil)
	expired.AddCookie(cookie)
	recorder = httptest.NewRecorder()
	auth.ServeHTTP(recorder, expired)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("expired session status=%d", recorder.Code)
	}
}

func TestSessionAuthenticatorCookieWorksAcrossReplicasAndRejectsTampering(t *testing.T) {
	firstStatic, _ := NewStaticAuthenticator("service-token", []string{"tenant-a"})
	secondStatic, _ := NewStaticAuthenticator("service-token", []string{"tenant-a"})
	first, err := NewSessionAuthenticator("operator", "secret", firstStatic)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewSessionAuthenticator("operator", "secret", secondStatic)
	if err != nil {
		t.Fatal(err)
	}
	login := httptest.NewRequest(http.MethodPost, "http://admin.test/admin/auth/login", bytes.NewBufferString(`{"username":"operator","password":"secret"}`))
	login.Header.Set("Origin", "http://admin.test")
	recorder := httptest.NewRecorder()
	first.ServeHTTP(recorder, login)
	if recorder.Code != http.StatusOK {
		t.Fatalf("login status=%d", recorder.Code)
	}
	cookie := recorder.Result().Cookies()[0]
	session := httptest.NewRequest(http.MethodGet, "http://admin.test/admin/auth/session", nil)
	session.AddCookie(cookie)
	recorder = httptest.NewRecorder()
	second.ServeHTTP(recorder, session)
	if recorder.Code != http.StatusOK {
		t.Fatalf("cross-replica session status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	differentStatic, _ := NewStaticAuthenticator("different-service-token", []string{"tenant-a"})
	differentReplica, err := NewSessionAuthenticator("operator", "secret", differentStatic)
	if err != nil {
		t.Fatal(err)
	}
	session = httptest.NewRequest(http.MethodGet, "http://admin.test/admin/auth/session", nil)
	session.AddCookie(cookie)
	recorder = httptest.NewRecorder()
	differentReplica.ServeHTTP(recorder, session)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("different secret session status=%d", recorder.Code)
	}
	differentPasswordReplica, err := NewSessionAuthenticator("operator", "changed-secret", secondStatic)
	if err != nil {
		t.Fatal(err)
	}
	session = httptest.NewRequest(http.MethodGet, "http://admin.test/admin/auth/session", nil)
	session.AddCookie(cookie)
	recorder = httptest.NewRecorder()
	differentPasswordReplica.ServeHTTP(recorder, session)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("changed password session status=%d", recorder.Code)
	}

	tampered := *cookie
	tampered.Value += "x"
	session = httptest.NewRequest(http.MethodGet, "http://admin.test/admin/auth/session", nil)
	session.AddCookie(&tampered)
	recorder = httptest.NewRecorder()
	second.ServeHTTP(recorder, session)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("tampered session status=%d", recorder.Code)
	}
}

func TestSessionAuthenticatorHonorsTLSProxyScheme(t *testing.T) {
	static, _ := NewStaticAuthenticator("service-token", []string{"tenant-a"})
	auth, err := NewSessionAuthenticator("operator", "secret", static)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, "http://admin.test/admin/auth/login", bytes.NewBufferString(`{"username":"operator","password":"secret"}`))
	request.Header.Set("X-Forwarded-Proto", "https")
	request.Header.Set("Origin", "https://admin.test")
	recorder := httptest.NewRecorder()
	auth.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || !recorder.Result().Cookies()[0].Secure {
		t.Fatalf("TLS proxy login status=%d cookies=%v", recorder.Code, recorder.Result().Cookies())
	}
}

func TestSessionAuthenticatorRejectsInvalidCredentialsAndCrossOriginWrites(t *testing.T) {
	static, _ := NewStaticAuthenticator("service-token", []string{"tenant-a"})
	auth, err := NewSessionAuthenticator("operator", "secret", static)
	if err != nil {
		t.Fatal(err)
	}
	bad := httptest.NewRequest(http.MethodPost, "http://admin.test/admin/auth/login", bytes.NewBufferString(`{"username":"operator","password":"wrong"}`))
	bad.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	auth.ServeHTTP(recorder, bad)
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("invalid credentials status=%d", recorder.Code)
	}
	login := httptest.NewRequest(http.MethodPost, "http://admin.test/admin/auth/login", bytes.NewBufferString(`{"username":"operator","password":"secret"}`))
	login.Header.Set("Content-Type", "application/json")
	login.Header.Set("Origin", "http://admin.test")
	recorder = httptest.NewRecorder()
	auth.ServeHTTP(recorder, login)
	cookie := recorder.Result().Cookies()[0]
	for _, method := range []string{http.MethodPost} {
		request := httptest.NewRequest(method, "http://admin.test/admin/auth/logout", nil)
		request.AddCookie(cookie)
		request.Header.Set("Origin", "http://evil.test")
		recorder = httptest.NewRecorder()
		auth.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusForbidden {
			t.Fatalf("cross-origin status=%d", recorder.Code)
		}
	}
	staticRequest := httptest.NewRequest(http.MethodGet, "http://admin.test/admin/auth/session", nil)
	staticRequest.Header.Set("Authorization", "Bearer service-token")
	recorder = httptest.NewRecorder()
	auth.ServeHTTP(recorder, staticRequest)
	if recorder.Code != http.StatusOK {
		t.Fatalf("static compatibility status=%d", recorder.Code)
	}
	var nilAuth *SessionAuthenticator
	if _, err := nilAuth.Authenticate(context.Background(), nil); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("nil auth error=%v", err)
	}
}

func TestSessionAuthenticatorNilRequestDoesNotPanic(t *testing.T) {
	auth, err := NewSessionAuthenticator("operator", "secret", func() *StaticAuthenticator {
		static, _ := NewStaticAuthenticator("service-token", []string{"tenant-a"})
		return static
	}())
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	auth.ServeHTTP(recorder, nil)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("nil request status = %d", recorder.Code)
	}
}

func TestSessionAuthenticatorRejectsUnsupportedAuthRoutes(t *testing.T) {
	static, err := NewStaticAuthenticator("service-token", []string{"tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	auth, err := NewSessionAuthenticator("operator", "secret", static)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		method string
		path   string
		allow  string
	}{
		{name: "login", method: http.MethodGet, path: "/admin/auth/login", allow: http.MethodPost},
		{name: "session", method: http.MethodPost, path: "/admin/auth/session", allow: http.MethodGet},
		{name: "logout", method: http.MethodGet, path: "/admin/auth/logout", allow: http.MethodPost},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(tc.method, "http://admin.test"+tc.path, nil)
			request.Header.Set("X-Request-ID", tc.name+"-method")
			recorder := httptest.NewRecorder()

			auth.ServeHTTP(recorder, request)

			if recorder.Header().Get("Allow") != tc.allow {
				t.Fatalf("Allow = %q, want %q", recorder.Header().Get("Allow"), tc.allow)
			}
			assertAuthError(t, recorder, http.StatusMethodNotAllowed, tc.name+"-method", "method_not_allowed")
		})
	}

	request := httptest.NewRequest(http.MethodGet, "http://admin.test/admin/auth/unknown", nil)
	request.Header.Set("X-Request-ID", "unknown-route")
	recorder := httptest.NewRecorder()
	auth.ServeHTTP(recorder, request)
	assertAuthError(t, recorder, http.StatusNotFound, "unknown-route", "not_found")
}

func TestSessionAuthenticatorLoginFailureCases(t *testing.T) {
	for _, tc := range []struct {
		name    string
		body    string
		origin  string
		prepare func(*SessionAuthenticator)
		status  int
		error   string
	}{
		{
			name:   "cross origin",
			body:   `{"username":"operator","password":"secret"}`,
			origin: "http://evil.test",
			status: http.StatusForbidden,
			error:  "forbidden",
		},
		{
			name:   "malformed body",
			body:   `{`,
			status: http.StatusBadRequest,
			error:  "invalid_request",
		},
		{
			name:   "invalid credentials",
			body:   `{"username":"operator","password":"wrong"}`,
			status: http.StatusUnauthorized,
			error:  "unauthorized",
		},
		{
			name: "session randomness failure",
			body: `{"username":"operator","password":"secret"}`,
			prepare: func(auth *SessionAuthenticator) {
				auth.rand = func([]byte) error { return errors.New("randomness unavailable") }
			},
			status: http.StatusInternalServerError,
			error:  "internal_error",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			static, err := NewStaticAuthenticator("service-token", []string{"tenant-a"})
			if err != nil {
				t.Fatal(err)
			}
			auth, err := NewSessionAuthenticator("operator", "secret", static)
			if err != nil {
				t.Fatal(err)
			}
			if tc.prepare != nil {
				tc.prepare(auth)
			}
			requestID := "login-" + tc.name
			request := httptest.NewRequest(http.MethodPost, "http://admin.test/admin/auth/login", bytes.NewBufferString(tc.body))
			request.Header.Set("X-Request-ID", requestID)
			if tc.origin != "" {
				request.Header.Set("Origin", tc.origin)
			}
			recorder := httptest.NewRecorder()

			auth.ServeHTTP(recorder, request)

			assertAuthError(t, recorder, tc.status, requestID, tc.error)
		})
	}
}

func assertAuthError(t *testing.T, recorder *httptest.ResponseRecorder, wantStatus int, wantRequestID, wantError string) {
	t.Helper()
	if recorder.Code != wantStatus {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, wantStatus, recorder.Body.String())
	}
	var envelope struct {
		RequestID string `json:"request_id"`
		Error     string `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if envelope.RequestID != wantRequestID || envelope.Error != wantError {
		t.Fatalf("error envelope = %+v, want request ID %q and error %q", envelope, wantRequestID, wantError)
	}
}

func TestStaticAuthenticatorConfigurationAndBoundary(t *testing.T) {
	for _, token := range []string{"", "bad\n-token"} {
		if _, err := NewStaticAuthenticator(token, []string{"tenant"}); !errors.Is(err, ErrUnauthenticated) {
			t.Errorf("token %q error = %v", token, err)
		}
	}
	if _, err := NewStaticAuthenticator("token", nil); !errors.Is(err, ErrForbidden) {
		t.Fatalf("empty scopes error = %v", err)
	}
	auth, err := NewStaticAuthenticator("token", []string{"tenant-a", "*", "tenant-b"})
	if err != nil {
		t.Fatal(err)
	}
	if !auth.principal.Global || !auth.principal.Allows("", true) || !auth.principal.Allows("tenant-a", true) || !auth.principal.Allows("tenant-a", false) || !auth.principal.Allows("missing", false) || len(auth.principal.ScopeIDs()) != 1 || auth.principal.ScopeIDs()[0] != "*" {
		t.Fatalf("unexpected principal scopes = %+v", auth.principal)
	}
	scoped, err := NewStaticAuthenticator("scoped-token", []string{"tenant-a"})
	if err != nil {
		t.Fatal(err)
	}
	if scoped.principal.Allows("tenant-b", false) || scoped.principal.Allows("", false) {
		t.Fatalf("scoped principal crossed tenant boundary = %+v", scoped.principal)
	}

	request := httptest.NewRequest(http.MethodGet, "/admin/v1", nil)
	for _, tc := range []struct {
		name string
		ctx  context.Context
		req  *http.Request
		want error
	}{
		{"nil context", nil, request, ErrUnauthenticated},
		{"nil request", context.Background(), nil, ErrUnauthenticated},
		{"missing bearer", context.Background(), request, ErrUnauthenticated},
		{"wrong token", context.Background(), func() *http.Request {
			r := request.Clone(context.Background())
			r.Header.Set("Authorization", "Bearer wrong")
			return r
		}(), ErrUnauthenticated},
		{"canceled", func() context.Context { c, cancel := context.WithCancel(context.Background()); cancel(); return c }(), request, context.Canceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, got := auth.Authenticate(tc.ctx, tc.req)
			if !errors.Is(got, tc.want) {
				t.Fatalf("Authenticate error = %v, want %v", got, tc.want)
			}
		})
	}
	request.Header.Set("Authorization", "Bearer token")
	principal, err := auth.Authenticate(context.Background(), request)
	if err != nil || !principal.Global {
		t.Fatalf("valid authentication = %+v, %v", principal, err)
	}
}
