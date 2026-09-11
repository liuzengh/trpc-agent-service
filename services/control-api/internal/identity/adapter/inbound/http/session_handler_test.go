package httpadapter_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	identityhttp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/adapter/inbound/http"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
)

func TestSessionHandlerAuthenticatesAndReturnsCurrentUser(t *testing.T) {
	gin.SetMode(gin.TestMode)
	authenticate := &authenticateStub{identity: application.IdentityContext{
		UserID: "user-1", SessionID: "session-1", Username: "alice", DisplayName: "Alice",
	}}
	handler := identityhttp.NewSessionHandler(
		authenticate,
		&logoutStub{},
		&changePasswordStub{},
		identityhttp.SessionCookie{Name: "control_session", Path: "/", Secure: true},
	)
	router := gin.New()
	handler.Register(router)

	request := httptest.NewRequest(http.MethodGet, "/v1/me", nil)
	request.AddCookie(&http.Cookie{Name: "control_session", Value: "opaque-token"})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", response.Code, response.Body)
	}
	if authenticate.token != "opaque-token" {
		t.Fatalf("token = %q", authenticate.token)
	}
	want := `{"user":{"id":"user-1","username":"alice","display_name":"Alice"},"password_change_required":false}`
	if response.Body.String() != want {
		t.Fatalf("body = %s, want %s", response.Body, want)
	}
}

func TestSessionHandlerRejectsMissingSession(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := identityhttp.NewSessionHandler(
		&authenticateStub{}, &logoutStub{}, &changePasswordStub{},
		identityhttp.SessionCookie{Name: "control_session", Path: "/"},
	)
	router := gin.New()
	handler.Register(router)

	request := httptest.NewRequest(http.MethodGet, "/v1/me", nil)
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", response.Code)
	}
}

func TestSessionHandlerLogsOutAndClearsCookie(t *testing.T) {
	gin.SetMode(gin.TestMode)
	authenticate := &authenticateStub{identity: application.IdentityContext{
		UserID: "user-1", SessionID: "session-1",
	}}
	logout := &logoutStub{}
	handler := identityhttp.NewSessionHandler(
		authenticate, logout, &changePasswordStub{},
		identityhttp.SessionCookie{Name: "control_session", Path: "/", Secure: true},
	)
	router := gin.New()
	handler.Register(router)

	request := httptest.NewRequest(http.MethodPost, "/v1/auth/logout", nil)
	request.AddCookie(&http.Cookie{Name: "control_session", Value: "opaque-token"})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", response.Code, response.Body)
	}
	if logout.identity.SessionID != "session-1" {
		t.Fatalf("logout identity = %#v", logout.identity)
	}
	cookies := response.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Name != "control_session" || cookies[0].MaxAge >= 0 {
		t.Fatalf("clear cookie = %#v", cookies)
	}
}

func TestSessionHandlerChangesPassword(t *testing.T) {
	gin.SetMode(gin.TestMode)
	authenticate := &authenticateStub{identity: application.IdentityContext{
		UserID: "user-1", SessionID: "session-1", Restricted: true,
	}}
	change := &changePasswordStub{}
	handler := identityhttp.NewSessionHandler(
		authenticate, &logoutStub{}, change,
		identityhttp.SessionCookie{Name: "control_session", Path: "/"},
	)
	router := gin.New()
	handler.Register(router)

	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/me/change-password",
		strings.NewReader(`{"current_password":"temporary password","new_password":"a new strong password"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	request.AddCookie(&http.Cookie{Name: "control_session", Value: "opaque-token"})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", response.Code, response.Body)
	}
	if change.command.Identity.UserID != "user-1" || change.command.NewPassword != "a new strong password" {
		t.Fatalf("change command = %#v", change.command)
	}
}

type authenticateStub struct {
	identity application.IdentityContext
	err      error
	token    string
}

func (s *authenticateStub) Execute(
	_ context.Context,
	token string,
) (application.IdentityContext, error) {
	s.token = token
	return s.identity, s.err
}

type logoutStub struct {
	identity application.IdentityContext
	err      error
}

func (s *logoutStub) Execute(_ context.Context, identity application.IdentityContext) error {
	s.identity = identity
	return s.err
}

type changePasswordStub struct {
	command application.ChangePasswordCommand
	err     error
}

func (s *changePasswordStub) Execute(
	_ context.Context,
	command application.ChangePasswordCommand,
) error {
	s.command = command
	return s.err
}

var _ identityhttp.SessionAuthenticator = (*authenticateStub)(nil)
var _ identityhttp.SessionLogout = (*logoutStub)(nil)
var _ identityhttp.PasswordChange = (*changePasswordStub)(nil)
