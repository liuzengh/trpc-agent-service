// Package httpadapter exposes Identity use cases through the Control API HTTP
// protocol.
package httpadapter

import (
	"context"
	"errors"
	stdhttp "net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
)

// PasswordLogin is the application interface consumed by the HTTP adapter.
type PasswordLogin interface {
	Execute(context.Context, application.LoginCommand) (application.LoginResult, error)
}

// SessionCookie defines how the browser carries the opaque session token.
type SessionCookie struct {
	Name     string
	Path     string
	Domain   string
	Secure   bool
	SameSite stdhttp.SameSite
}

// LoginHandler translates the HTTP login contract to LoginWithPassword.
type LoginHandler struct {
	login   PasswordLogin
	cookie  SessionCookie
	limiter *loginRateLimiter
}

// NewLoginHandler returns the V1 password login HTTP adapter.
func NewLoginHandler(login PasswordLogin, cookie SessionCookie) *LoginHandler {
	if cookie.Path == "" {
		cookie.Path = "/"
	}
	if cookie.SameSite == stdhttp.SameSiteDefaultMode {
		cookie.SameSite = stdhttp.SameSiteLaxMode
	}
	return &LoginHandler{
		login:   login,
		cookie:  cookie,
		limiter: newLoginRateLimiter(10, time.Minute),
	}
}

// Register adds the Identity login route.
func (h *LoginHandler) Register(routes gin.IRoutes) {
	routes.POST("/v1/auth/login", h.handle)
}

type loginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginResponse struct {
	User                   userResponse `json:"user"`
	PasswordChangeRequired bool         `json:"password_change_required"`
}

type userResponse struct {
	ID          string `json:"id"`
	Username    string `json:"username"`
	DisplayName string `json:"display_name"`
}

type publicError struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (h *LoginHandler) handle(c *gin.Context) {
	request, err := decodeLoginRequest(c)
	if err != nil || !validLoginRequest(request) {
		writeError(c, stdhttp.StatusBadRequest, "INVALID_REQUEST", "username and password are required")
		return
	}
	// Proxy trust is disabled by process bootstrap, so ClientIP is derived from
	// the direct peer rather than attacker-controlled forwarding headers. Using
	// only the peer also bounds one client's map cardinality across usernames.
	limitKey := c.ClientIP()
	if allowed, retryAfter := h.limiter.allow(limitKey); !allowed {
		retrySeconds := int(retryAfter/time.Second) + 1
		c.Header("Retry-After", strconv.Itoa(retrySeconds))
		writeError(c, stdhttp.StatusTooManyRequests, "LOGIN_RATE_LIMITED", "too many login attempts")
		return
	}

	result, err := h.login.Execute(c.Request.Context(), application.LoginCommand{
		Username: request.Username,
		Password: request.Password,
	})
	if err != nil {
		if errors.Is(err, application.ErrInvalidCredentials) {
			writeError(c, stdhttp.StatusUnauthorized, "INVALID_CREDENTIALS", "invalid username or password")
			return
		}
		writeError(c, stdhttp.StatusInternalServerError, "INTERNAL_ERROR", "request could not be completed")
		return
	}

	stdhttp.SetCookie(c.Writer, &stdhttp.Cookie{
		Name:     h.cookie.Name,
		Value:    result.SessionToken,
		Path:     h.cookie.Path,
		Domain:   h.cookie.Domain,
		Expires:  result.ExpiresAt,
		HttpOnly: true,
		Secure:   h.cookie.Secure,
		SameSite: h.cookie.SameSite,
	})
	c.JSON(stdhttp.StatusOK, loginResponse{
		User: userResponse{
			ID:          result.User.ID,
			Username:    result.User.Username,
			DisplayName: result.User.DisplayName,
		},
		PasswordChangeRequired: result.PasswordChangeRequired,
	})
}

func decodeLoginRequest(c *gin.Context) (loginRequest, error) {
	var request loginRequest
	if err := decodeStrictJSON(c, &request, 4*1024); err != nil {
		return loginRequest{}, err
	}
	return request, nil
}

func validLoginRequest(request loginRequest) bool {
	username := strings.TrimSpace(request.Username)
	return username != "" && len(username) <= 256 &&
		request.Password != "" && len(request.Password) <= 1024
}

func writeError(c *gin.Context, status int, code, message string) {
	c.JSON(status, publicError{Error: errorBody{Code: code, Message: message}})
}
