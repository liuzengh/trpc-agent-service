package httpadapter

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	identity "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/platformbackend/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/platformbackend/domain"
)

type Directory interface {
	List(context.Context, string, string) ([]domain.View, error)
}
type Handler struct{ directory Directory }

func NewHandler(d Directory) *Handler { return &Handler{d} }
func (h *Handler) Register(routes gin.IRoutes) {
	routes.GET("/v1/tenants/:tenant_id/runtime-backends", h.list)
}
func (h *Handler) list(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	actor, ok := identity.IdentityFromContext(c.Request.Context())
	if !ok {
		fail(c, http.StatusUnauthorized, "UNAUTHENTICATED")
		return
	}
	if actor.Restricted {
		fail(c, http.StatusForbidden, "PASSWORD_CHANGE_REQUIRED")
		return
	}
	items, err := h.directory.List(c.Request.Context(), c.Param("tenant_id"), actor.UserID)
	if err != nil {
		if errors.Is(err, application.ErrForbidden) {
			fail(c, http.StatusForbidden, "TENANT_FORBIDDEN")
		} else {
			fail(c, http.StatusServiceUnavailable, "BACKEND_DIRECTORY_UNAVAILABLE")
		}
		return
	}
	c.JSON(http.StatusOK, struct {
		Items []domain.View `json:"items"`
	}{items})
}
func fail(c *gin.Context, status int, code string) {
	c.JSON(status, gin.H{"error": gin.H{"code": code, "message": "backend directory request failed"}})
}
