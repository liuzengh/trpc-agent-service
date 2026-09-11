package httpadapter

import (
	stdhttp "net/http"
	"time"

	"github.com/gin-gonic/gin"

	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	tenantapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/application"
	tenantdomain "github.com/liuzengh/trpc-agent-service/services/control-api/internal/tenant/domain"
)

type provisionTenantRequest struct {
	Slug        string `json:"slug"`
	Name        string `json:"name"`
	OwnerUserID string `json:"owner_user_id"`
}

func (h *Handler) provisionTenant(c *gin.Context) {
	identity, _ := identityapp.IdentityFromContext(c.Request.Context())
	var request provisionTenantRequest
	if err := decodeStrictJSON(c, &request); err != nil {
		writeError(c, stdhttp.StatusBadRequest, "INVALID_REQUEST", "request body is invalid")
		return
	}
	result, err := h.service.ProvisionTenant(c.Request.Context(), tenantapp.ProvisionTenantCommand{
		Slug: request.Slug, Name: request.Name, OwnerUserID: request.OwnerUserID,
		ActorUserID: identity.UserID,
	})
	if err != nil {
		handleError(c, err)
		return
	}
	c.JSON(stdhttp.StatusCreated, tenantView(result.Tenant, result.Owner.UserID))
}

func (h *Handler) listTenants(c *gin.Context) {
	page, ok := parsePage(c)
	if !ok {
		return
	}
	result, err := h.service.ListTenants(c.Request.Context(), tenantapp.Page(page))
	if err != nil {
		writeInternal(c)
		return
	}
	tenants := make([]tenantResponse, 0, len(result.Tenants))
	for _, tenant := range result.Tenants {
		tenants = append(tenants, tenantView(tenant, ""))
	}
	c.JSON(stdhttp.StatusOK, gin.H{
		"tenants": tenants, "offset": page.Offset, "limit": page.Limit, "total": result.Total,
	})
}

type tenantResponse struct {
	ID          string                    `json:"id"`
	Slug        string                    `json:"slug"`
	Name        string                    `json:"name"`
	Status      tenantdomain.TenantStatus `json:"status"`
	OwnerUserID string                    `json:"owner_user_id,omitempty"`
	CreatedAt   time.Time                 `json:"created_at"`
	UpdatedAt   time.Time                 `json:"updated_at"`
}

func tenantView(tenant tenantdomain.Tenant, ownerUserID string) tenantResponse {
	return tenantResponse{
		ID: tenant.ID, Slug: tenant.Slug, Name: tenant.Name, Status: tenant.Status,
		OwnerUserID: ownerUserID, CreatedAt: tenant.CreatedAt, UpdatedAt: tenant.UpdatedAt,
	}
}
