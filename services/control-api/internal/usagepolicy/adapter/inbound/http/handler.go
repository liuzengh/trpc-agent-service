package httpadapter

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/gin-gonic/gin"
	governancev1 "github.com/liuzengh/trpc-agent-service/api/runtime/governance/v1"
	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/usagepolicy/application"
)

type Handler struct{ service *application.Service }

func New(service *application.Service) *Handler { return &Handler{service} }
func (h *Handler) Register(r gin.IRoutes) {
	r.GET("/v1/tenants/:tenant_id/usage-policy", h.get)
	r.PUT("/v1/tenants/:tenant_id/usage-policy", h.put)
}
func identity(c *gin.Context) (identityapp.IdentityContext, bool) {
	id, ok := identityapp.IdentityFromContext(c.Request.Context())
	if !ok {
		c.JSON(401, gin.H{"error": gin.H{"code": "AUTHENTICATION_REQUIRED"}})
		return id, false
	}
	if id.Restricted {
		c.JSON(403, gin.H{"error": gin.H{"code": "PASSWORD_CHANGE_REQUIRED"}})
		return id, false
	}
	return id, true
}
func (h *Handler) get(c *gin.Context) {
	id, ok := identity(c)
	if !ok {
		return
	}
	if c.Request.URL.RawQuery != "" {
		c.JSON(400, gin.H{"error": gin.H{"code": "USAGE_POLICY_INVALID"}})
		return
	}
	p, e := h.service.Get(c.Request.Context(), c.Param("tenant_id"), id.UserID)
	if e != nil {
		fail(c, e)
		return
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(200, p)
}

type replaceRequest struct {
	ExpectedRevision int64               `json:"expected_revision"`
	Policy           governancev1.Policy `json:"policy"`
}

func (h *Handler) put(c *gin.Context) {
	id, ok := identity(c)
	if !ok {
		return
	}
	if c.Request.URL.RawQuery != "" {
		c.JSON(400, gin.H{"error": gin.H{"code": "USAGE_POLICY_INVALID"}})
		return
	}
	raw, e := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, 128<<10))
	if e != nil {
		c.JSON(400, gin.H{"error": gin.H{"code": "USAGE_POLICY_INVALID"}})
		return
	}
	var in replaceRequest
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(&in) != nil {
		c.JSON(400, gin.H{"error": gin.H{"code": "USAGE_POLICY_INVALID"}})
		return
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		c.JSON(400, gin.H{"error": gin.H{"code": "USAGE_POLICY_INVALID"}})
		return
	}
	p, e := h.service.Replace(c.Request.Context(), c.Param("tenant_id"), id.UserID, c.GetHeader("Idempotency-Key"), in.ExpectedRevision, in.Policy)
	if e != nil {
		fail(c, e)
		return
	}
	c.Header("Cache-Control", "no-store")
	c.JSON(200, p)
}
func fail(c *gin.Context, e error) {
	switch {
	case errors.Is(e, application.ErrForbidden):
		c.JSON(403, gin.H{"error": gin.H{"code": "TENANT_FORBIDDEN"}})
	case errors.Is(e, application.ErrInvalid):
		c.JSON(400, gin.H{"error": gin.H{"code": "USAGE_POLICY_INVALID"}})
	case errors.Is(e, application.ErrConflict):
		c.JSON(409, gin.H{"error": gin.H{"code": "USAGE_POLICY_CONFLICT"}})
	default:
		c.JSON(503, gin.H{"error": gin.H{"code": "USAGE_POLICY_UNAVAILABLE"}})
	}
}
