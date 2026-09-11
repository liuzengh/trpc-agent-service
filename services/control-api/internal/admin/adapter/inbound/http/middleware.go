// Package httpadapter translates platform administration HTTP requests to use cases.
package httpadapter

import (
	"context"
	stdhttp "net/http"

	"github.com/gin-gonic/gin"

	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/admin/domain"
	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	identitydomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/domain"
	tenantapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/application"
)

type AdminService interface {
	IsOperator(context.Context, string) (bool, error)
	ListOperators(context.Context) ([]application.OperatorView, error)
	GrantOperator(context.Context, string, string) (domain.OperatorGrant, error)
	RevokeOperator(context.Context, string, string) error
	CreateUser(context.Context, identityapp.CreateManagedAccountCommand) (identitydomain.UserAccount, error)
	ListUsers(context.Context, identityapp.Page) (identityapp.AccountPage, error)
	ProvisionTenant(context.Context, tenantapp.ProvisionTenantCommand) (tenantapp.ProvisionTenantResult, error)
	ListTenants(context.Context, tenantapp.Page) (tenantapp.TenantPage, error)
}

type Handler struct {
	service AdminService
}

func NewHandler(service AdminService) *Handler {
	return &Handler{service: service}
}

func (h *Handler) Register(routes gin.IRoutes) {
	routes.GET("/v1/admin/capabilities", h.capabilities)
	routes.GET("/v1/admin/operators", h.listOperators)
	routes.POST("/v1/admin/operators", h.grantOperator)
	routes.DELETE("/v1/admin/operators/:user_id", h.revokeOperator)
	routes.GET("/v1/admin/users", h.listUsers)
	routes.POST("/v1/admin/users", h.createUser)
	routes.GET("/v1/admin/tenants", h.listTenants)
	routes.POST("/v1/admin/tenants", h.provisionTenant)
}

// AuthorizationMiddleware establishes the Admin Context after Identity has
// authenticated the request.
func (h *Handler) AuthorizationMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		identity, ok := identityapp.IdentityFromContext(c.Request.Context())
		if !ok {
			writeError(c, stdhttp.StatusUnauthorized, "UNAUTHENTICATED", "authentication required")
			c.Abort()
			return
		}
		if identity.Restricted {
			writeError(c, stdhttp.StatusForbidden, "PASSWORD_CHANGE_REQUIRED", "password change is required")
			c.Abort()
			return
		}
		active, err := h.service.IsOperator(c.Request.Context(), identity.UserID)
		if err != nil {
			writeError(c, stdhttp.StatusInternalServerError, "INTERNAL_ERROR", "request could not be completed")
			c.Abort()
			return
		}
		if !active {
			writeError(c, stdhttp.StatusForbidden, "ADMIN_FORBIDDEN", "platform administration is forbidden")
			c.Abort()
			return
		}
		c.Next()
	}
}
