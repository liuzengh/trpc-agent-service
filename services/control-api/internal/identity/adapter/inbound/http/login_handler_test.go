package httpadapter_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	identityhttp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/adapter/inbound/http"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
)

func TestLoginHandlerSetsOpaqueSessionCookie(t *testing.T) {
	gin.SetMode(gin.TestMode)
	expiresAt := time.Date(2026, time.September, 1, 10, 0, 0, 0, time.UTC)
	login := &loginStub{result: application.LoginResult{
		SessionToken: "opaque-session-token",
		ExpiresAt:    expiresAt,
		User: application.LoggedInUser{
			ID:          "user-1",
			Username:    "alice",
			DisplayName: "Alice",
		},
	}}
	router := gin.New()
	handler := identityhttp.NewLoginHandler(login, identityhttp.SessionCookie{
		Name:     "control_session",
		Path:     "/",
		Secure:   true,
		SameSite: http.SameSiteLaxMode,
	})
	handler.Register(router)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/auth/login",
		strings.NewReader(`{"username":"alice","password":"correct horse battery staple"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusOK, response.Body)
	}
	if login.command.Username != "alice" || login.command.Password != "correct horse battery staple" {
		t.Fatalf("login command = %#v", login.command)
	}
	cookies := response.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("cookies = %d, want 1", len(cookies))
	}
	cookie := cookies[0]
	if cookie.Name != "control_session" || cookie.Value != "opaque-session-token" {
		t.Fatalf("cookie = %#v", cookie)
	}
	if !cookie.HttpOnly || !cookie.Secure || cookie.SameSite != http.SameSiteLaxMode {
		t.Fatalf("cookie flags = %#v", cookie)
	}
	if !cookie.Expires.Equal(expiresAt) {
		t.Fatalf("cookie Expires = %v, want %v", cookie.Expires, expiresAt)
	}

	var body struct {
		User struct {
			ID          string `json:"id"`
			Username    string `json:"username"`
			DisplayName string `json:"display_name"`
		} `json:"user"`
		PasswordChangeRequired bool `json:"password_change_required"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.User.ID != "user-1" || body.User.Username != "alice" || body.User.DisplayName != "Alice" {
		t.Fatalf("response user = %#v", body.User)
	}
	if body.PasswordChangeRequired {
		t.Fatal("password_change_required = true, want false")
	}
	if strings.Contains(response.Body.String(), "opaque-session-token") {
		t.Fatal("response body exposes the session token")
	}
}

func TestLoginHandlerReturnsOnePublicCredentialError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	handler := identityhttp.NewLoginHandler(
		&loginStub{err: application.ErrInvalidCredentials},
		identityhttp.SessionCookie{Name: "control_session", Path: "/"},
	)
	handler.Register(router)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/auth/login",
		strings.NewReader(`{"username":"unknown","password":"wrong"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusUnauthorized, response.Body)
	}
	if response.Body.String() != `{"error":{"code":"INVALID_CREDENTIALS","message":"invalid username or password"}}` {
		t.Fatalf("body = %s", response.Body)
	}
	if len(response.Result().Cookies()) != 0 {
		t.Fatal("invalid login returned a cookie")
	}
}

func TestLoginHandlerRejectsMalformedRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	login := &loginStub{}
	router := gin.New()
	handler := identityhttp.NewLoginHandler(
		login,
		identityhttp.SessionCookie{Name: "control_session", Path: "/"},
	)
	handler.Register(router)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/auth/login",
		strings.NewReader(`{"username":"","password":""}`),
	)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusBadRequest, response.Body)
	}
	if login.calls != 0 {
		t.Fatalf("login calls = %d, want 0", login.calls)
	}
}

func TestLoginHandlerRejectsUnknownJSONFields(t *testing.T) {
	gin.SetMode(gin.TestMode)
	login := &loginStub{}
	router := gin.New()
	handler := identityhttp.NewLoginHandler(
		login,
		identityhttp.SessionCookie{Name: "control_session", Path: "/"},
	)
	handler.Register(router)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/auth/login",
		strings.NewReader(`{"username":"alice","password":"secret","role":"operator"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusBadRequest, response.Body)
	}
	if login.calls != 0 {
		t.Fatalf("login calls = %d, want 0", login.calls)
	}
}

func TestLoginHandlerRateLimitsRepeatedAttempts(t *testing.T) {
	gin.SetMode(gin.TestMode)
	login := &loginStub{err: application.ErrInvalidCredentials}
	router := gin.New()
	handler := identityhttp.NewLoginHandler(
		login,
		identityhttp.SessionCookie{Name: "control_session", Path: "/"},
	)
	handler.Register(router)

	for attempt := 1; attempt <= 11; attempt++ {
		request := httptest.NewRequest(
			http.MethodPost,
			"/v1/auth/login",
			strings.NewReader(`{"username":"alice","password":"wrong"}`),
		)
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)

		if attempt <= 10 && response.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d status = %d, want %d", attempt, response.Code, http.StatusUnauthorized)
		}
		if attempt == 11 {
			if response.Code != http.StatusTooManyRequests {
				t.Fatalf("attempt 11 status = %d, want %d", response.Code, http.StatusTooManyRequests)
			}
			if response.Header().Get("Retry-After") == "" {
				t.Fatal("attempt 11 has no Retry-After header")
			}
		}
	}
	if login.calls != 10 {
		t.Fatalf("login calls = %d, want 10", login.calls)
	}
}

type loginStub struct {
	result  application.LoginResult
	err     error
	command application.LoginCommand
	calls   int
}

func (s *loginStub) Execute(
	_ context.Context,
	command application.LoginCommand,
) (application.LoginResult, error) {
	s.calls++
	s.command = command
	return s.result, s.err
}

var _ identityhttp.PasswordLogin = (*loginStub)(nil)
