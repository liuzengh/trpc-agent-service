package httpadapter

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	channelv1 "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
)

// PreflightService is the public diagnostic use-case seam, separate from normal
// account commands and runtime credentials. Authorization remains transactional
// in the application; HTTP only obtains the authenticated Session identity.
type PreflightService interface {
	Create(context.Context, application.Actor, string, string, channelv1.PreflightCreateRequest) (channelv1.PreflightCreated, error)
	Get(context.Context, application.Actor, string, string) (channelv1.PreflightView, error)
}

// PreflightAccountReader prevents malformed payloads from revealing an account
// in another tenant before the owning application performs its write check.
type PreflightAccountReader interface {
	GetAccount(context.Context, application.Actor, string) (application.AccountDetails, error)
}
type PreflightHandler struct {
	service  PreflightService
	accounts PreflightAccountReader
}

func NewPreflightHandler(service PreflightService, readers ...PreflightAccountReader) *PreflightHandler {
	h := &PreflightHandler{service: service}
	if len(readers) > 0 {
		h.accounts = readers[0]
	}
	return h
}
func (h *PreflightHandler) Register(routes gin.IRoutes) {
	routes.POST("/v1/tenants/:tenant_id/channel-accounts/:account_id/preflights", h.create)
	routes.GET("/v1/tenants/:tenant_id/channel-accounts/:account_id/preflights/:preflight_id", h.get)
}
func (h *PreflightHandler) create(c *gin.Context) {
	a, ok := actor(c)
	if !ok {
		return
	}
	if !h.accountVisible(c, a) {
		return
	}
	k, ok := key(c)
	if !ok {
		return
	}
	in, ok := decode[channelv1.PreflightCreateRequest](c, "preflight-create.schema.json", 4*1024)
	if !ok {
		return
	}
	if h.service == nil {
		preflightError(c, application.ErrDependencyUnavailable)
		return
	}
	result, err := h.service.Create(c.Request.Context(), a, c.Param("account_id"), k, in)
	if err != nil {
		preflightError(c, err)
		return
	}
	c.Header("Location", result.StatusURL)
	c.Header("Retry-After", "2")
	c.JSON(http.StatusAccepted, result)
}
func (h *PreflightHandler) get(c *gin.Context) {
	a, ok := actor(c)
	if !ok || !h.accountVisible(c, a) || !getOnly(c) {
		return
	}
	if !domain.ValidID(c.Param("preflight_id")) {
		writeError(c, http.StatusBadRequest, domain.InputInvalid, "/preflight_id")
		return
	}
	if c.Request.ContentLength != 0 || len(c.Request.TransferEncoding) != 0 {
		writeError(c, http.StatusBadRequest, domain.InputInvalid, "")
		return
	}
	if h.service == nil {
		preflightError(c, application.ErrDependencyUnavailable)
		return
	}
	result, err := h.service.Get(c.Request.Context(), a, c.Param("account_id"), c.Param("preflight_id"))
	if err != nil {
		preflightError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

// accountVisible applies the preflight-only concealment contract before
// decoding caller input. Do not map service write errors here: OWNER denial and
// commit-time Session revocation intentionally remain forbidden responses.
func (h *PreflightHandler) accountVisible(c *gin.Context, a application.Actor) bool {
	if h.accounts == nil {
		return true
	}
	_, err := h.accounts.GetAccount(c.Request.Context(), a, c.Param("account_id"))
	if errors.Is(err, application.ErrPermissionDenied) {
		err = application.ErrAccountNotFound
	}
	if err != nil {
		preflightError(c, err)
		return false
	}
	return true
}

func preflightError(c *gin.Context, err error) {
	var d *domain.Error
	if errors.As(err, &d) {
		switch d.Code {
		case "CHANNEL_PREFLIGHT_NOT_FOUND":
			writeError(c, 404, d.Code, d.Field)
			return
		case "CHANNEL_PREFLIGHT_PROVIDER_UNSUPPORTED", "CHANNEL_PREFLIGHT_CONNECTION_PROBE_CONFIRMATION_REQUIRED":
			writeError(c, 422, d.Code, d.Field)
			return
		case "CHANNEL_PREFLIGHT_RATE_LIMITED":
			c.Header("Retry-After", "2")
			writeError(c, 429, d.Code, d.Field)
			return
		}
	}
	handleError(c, err)
}
