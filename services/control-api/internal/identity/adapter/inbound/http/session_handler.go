package httpadapter

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	stdhttp "net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
)

type SessionAuthenticator interface {
	Execute(context.Context, string) (application.IdentityContext, error)
}

type SessionLogout interface {
	Execute(context.Context, application.IdentityContext) error
}

type PasswordChange interface {
	Execute(context.Context, application.ChangePasswordCommand) error
}

// SessionHandler owns the authenticated Identity HTTP surface and exposes its
// middleware to Admin and Tenant route groups.
type SessionHandler struct {
	authenticate SessionAuthenticator
	logout       SessionLogout
	change       PasswordChange
	cookie       SessionCookie
}

func NewSessionHandler(
	authenticate SessionAuthenticator,
	logout SessionLogout,
	change PasswordChange,
	cookie SessionCookie,
) *SessionHandler {
	if cookie.Path == "" {
		cookie.Path = "/"
	}
	if cookie.SameSite == stdhttp.SameSiteDefaultMode {
		cookie.SameSite = stdhttp.SameSiteLaxMode
	}
	return &SessionHandler{
		authenticate: authenticate,
		logout:       logout,
		change:       change,
		cookie:       cookie,
	}
}

func (h *SessionHandler) Register(routes gin.IRoutes) {
	routes.GET("/v1/me", h.Middleware(), h.getMe)
	routes.POST("/v1/auth/logout", h.Middleware(), h.logoutCurrentSession)
	routes.POST("/v1/me/change-password", h.Middleware(), h.changeCurrentPassword)
}

// Middleware authenticates the opaque cookie and attaches IdentityContext to
// the standard request context. It does not perform Admin or Tenant auth.
func (h *SessionHandler) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		cookie, err := c.Request.Cookie(h.cookie.Name)
		if err != nil || cookie.Value == "" {
			writeError(c, stdhttp.StatusUnauthorized, "UNAUTHENTICATED", "authentication required")
			c.Abort()
			return
		}
		identity, err := h.authenticate.Execute(c.Request.Context(), cookie.Value)
		if err != nil {
			if errors.Is(err, application.ErrUnauthenticated) {
				writeError(c, stdhttp.StatusUnauthorized, "UNAUTHENTICATED", "authentication required")
			} else {
				writeError(c, stdhttp.StatusInternalServerError, "INTERNAL_ERROR", "request could not be completed")
			}
			c.Abort()
			return
		}
		c.Request = c.Request.WithContext(application.WithIdentity(c.Request.Context(), identity))
		c.Next()
	}
}

func (h *SessionHandler) getMe(c *gin.Context) {
	identity, ok := application.IdentityFromContext(c.Request.Context())
	if !ok {
		writeError(c, stdhttp.StatusUnauthorized, "UNAUTHENTICATED", "authentication required")
		return
	}
	c.JSON(stdhttp.StatusOK, loginResponse{
		User: userResponse{
			ID:          identity.UserID,
			Username:    identity.Username,
			DisplayName: identity.DisplayName,
		},
		PasswordChangeRequired: identity.Restricted,
	})
}

func (h *SessionHandler) logoutCurrentSession(c *gin.Context) {
	identity, ok := application.IdentityFromContext(c.Request.Context())
	if !ok {
		writeError(c, stdhttp.StatusUnauthorized, "UNAUTHENTICATED", "authentication required")
		return
	}
	if err := h.logout.Execute(c.Request.Context(), identity); err != nil {
		writeError(c, stdhttp.StatusInternalServerError, "INTERNAL_ERROR", "request could not be completed")
		return
	}
	stdhttp.SetCookie(c.Writer, &stdhttp.Cookie{
		Name:     h.cookie.Name,
		Value:    "",
		Path:     h.cookie.Path,
		Domain:   h.cookie.Domain,
		Expires:  time.Unix(1, 0).UTC(),
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   h.cookie.Secure,
		SameSite: h.cookie.SameSite,
	})
	c.Status(stdhttp.StatusNoContent)
}

type changePasswordRequest struct {
	CurrentPassword string `json:"current_password"`
	NewPassword     string `json:"new_password"`
}

func (h *SessionHandler) changeCurrentPassword(c *gin.Context) {
	identity, ok := application.IdentityFromContext(c.Request.Context())
	if !ok {
		writeError(c, stdhttp.StatusUnauthorized, "UNAUTHENTICATED", "authentication required")
		return
	}
	var request changePasswordRequest
	if err := decodeStrictJSON(c, &request, 4*1024); err != nil ||
		request.CurrentPassword == "" || request.NewPassword == "" {
		writeError(c, stdhttp.StatusBadRequest, "INVALID_REQUEST", "current_password and new_password are required")
		return
	}
	err := h.change.Execute(c.Request.Context(), application.ChangePasswordCommand{
		Identity:        identity,
		CurrentPassword: request.CurrentPassword,
		NewPassword:     request.NewPassword,
	})
	if err != nil {
		switch {
		case errors.Is(err, application.ErrInvalidCurrentPassword):
			writeError(c, stdhttp.StatusBadRequest, "INVALID_CURRENT_PASSWORD", "current password is invalid")
		case errors.Is(err, application.ErrWeakPassword):
			writeError(c, stdhttp.StatusBadRequest, "WEAK_PASSWORD", "new password does not satisfy policy")
		default:
			writeError(c, stdhttp.StatusInternalServerError, "INTERNAL_ERROR", "request could not be completed")
		}
		return
	}
	c.Status(stdhttp.StatusNoContent)
}

func decodeStrictJSON(c *gin.Context, destination any, maxBytes int64) error {
	if !strings.HasPrefix(strings.ToLower(c.GetHeader("Content-Type")), "application/json") {
		return errors.New("content type must be application/json")
	}
	c.Request.Body = stdhttp.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
	decoder := json.NewDecoder(c.Request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request must contain exactly one JSON value")
	}
	return nil
}
