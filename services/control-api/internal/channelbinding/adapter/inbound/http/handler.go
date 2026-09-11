package httpadapter

import (
	"context"
	"errors"
	"io"
	"mime"
	"net/url"
	"strconv"

	"github.com/gin-gonic/gin"
	channelv1 "github.com/liuzengh/trpc-agent-service/api/schemas/channel/v1"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/application"
	"github.com/liuzengh/trpc-agent-service/services/control-api/internal/channelbinding/domain"
	identityapp "github.com/liuzengh/trpc-agent-service/services/control-api/internal/identity/application"
)

type Commands interface {
	AuthorizeWrite(context.Context, application.Actor) error
	CreateAccount(context.Context, application.Actor, string, application.CreateAccountInput) (application.CommandResult, error)
	UpdateAccount(context.Context, application.Actor, string, string, application.UpdateAccountInput) (application.CommandResult, error)
	UpdateCredential(context.Context, application.Actor, string, string, string, application.UpdateCredentialInput) (application.CommandResult, error)
	SetAccountEnabled(context.Context, application.Actor, string, string, application.AccountEnabledInput) (application.CommandResult, error)
	CreateBinding(context.Context, application.Actor, string, application.CreateBindingInput) (application.CommandResult, error)
	SetBindingTarget(context.Context, application.Actor, string, string, application.BindingTargetInput) (application.CommandResult, error)
	SetBindingTraffic(context.Context, application.Actor, string, string, application.BindingTrafficInput) (application.CommandResult, error)
	SetBindingEnabled(context.Context, application.Actor, string, string, application.BindingEnabledInput) (application.CommandResult, error)
}
type Queries interface {
	GetAccount(context.Context, application.Actor, string) (application.AccountDetails, error)
	GetBinding(context.Context, application.Actor, string) (application.BindingDetails, error)
	ListAccounts(context.Context, application.Actor, application.Page) (application.AccountPage, error)
	ListBindings(context.Context, application.Actor, application.Page) (application.BindingPage, error)
}
type Handler struct {
	commands Commands
	queries  Queries
}

func NewHandler(commands Commands, queries Queries) *Handler { return &Handler{commands, queries} }
func (h *Handler) Register(routes gin.IRoutes) {
	routes.POST("/v1/tenants/:tenant_id/channel-accounts", h.authorizeWrite, h.createAccount)
	routes.GET("/v1/tenants/:tenant_id/channel-accounts", h.listAccounts)
	routes.GET("/v1/tenants/:tenant_id/channel-accounts/:account_id", h.getAccount)
	routes.PATCH("/v1/tenants/:tenant_id/channel-accounts/:account_id", h.authorizeWrite, h.updateAccount)
	routes.POST("/v1/tenants/:tenant_id/channel-accounts/:account_id/credentials/:purpose/update", h.authorizeWrite, h.updateCredential)
	routes.POST("/v1/tenants/:tenant_id/channel-accounts/:account_id/enabled", h.authorizeWrite, h.accountEnabled)
	routes.POST("/v1/tenants/:tenant_id/channel-bindings", h.authorizeWrite, h.createBinding)
	routes.GET("/v1/tenants/:tenant_id/channel-bindings", h.listBindings)
	routes.GET("/v1/tenants/:tenant_id/channel-bindings/:binding_id", h.getBinding)
	routes.POST("/v1/tenants/:tenant_id/channel-bindings/:binding_id/target", h.authorizeWrite, h.bindingTarget)
	routes.POST("/v1/tenants/:tenant_id/channel-bindings/:binding_id/traffic", h.authorizeWrite, h.bindingTraffic)
	routes.POST("/v1/tenants/:tenant_id/channel-bindings/:binding_id/enabled", h.authorizeWrite, h.bindingEnabled)
}
func actor(c *gin.Context) (application.Actor, bool) {
	c.Header("Cache-Control", "no-store")
	identity, ok := identityapp.IdentityFromContext(c.Request.Context())
	if !ok {
		writeError(c, 401, "UNAUTHENTICATED", "")
		return application.Actor{}, false
	}
	if identity.Restricted {
		writeError(c, 403, "CHANNEL_PERMISSION_DENIED", "")
		return application.Actor{}, false
	}
	for _, name := range []string{"tenant_id", "account_id", "binding_id"} {
		v := c.Param(name)
		if (name == "tenant_id" || v != "") && !domain.ValidID(v) {
			writeError(c, 400, "CHANNEL_INPUT_INVALID", "/"+name)
			return application.Actor{}, false
		}
	}
	return application.Actor{TenantID: c.Param("tenant_id"), UserID: identity.UserID}, true
}
func key(c *gin.Context) (string, bool) {
	if c.Request.URL.RawQuery != "" {
		writeError(c, 400, "CHANNEL_INPUT_INVALID", "/query")
		return "", false
	}
	values := c.Request.Header.Values("Idempotency-Key")
	if len(values) != 1 || len(values[0]) < 1 || len(values[0]) > 128 {
		writeError(c, 400, "CHANNEL_INPUT_INVALID", "/Idempotency-Key")
		return "", false
	}
	for _, r := range values[0] {
		if r < 33 || r > 126 {
			writeError(c, 400, "CHANNEL_INPUT_INVALID", "/Idempotency-Key")
			return "", false
		}
	}
	return values[0], true
}
func decode[T any](c *gin.Context, schema string, limit int64) (T, bool) {
	var input T
	media, _, err := mime.ParseMediaType(c.GetHeader("Content-Type"))
	if err != nil || media != "application/json" || c.GetHeader("Content-Encoding") != "" {
		writeError(c, 400, "CHANNEL_INPUT_INVALID", "")
		return input, false
	}
	raw, err := io.ReadAll(io.LimitReader(c.Request.Body, limit+1))
	defer clear(raw)
	if err != nil {
		writeError(c, 400, "CHANNEL_INPUT_INVALID", "")
		return input, false
	}
	if int64(len(raw)) > limit {
		writeError(c, 413, "CHANNEL_LIMIT_EXCEEDED", "")
		return input, false
	}
	if err = channelv1.Decode(schema, raw, &input); err != nil {
		code := "CHANNEL_INPUT_INVALID"
		if len(schema) > 8 && schema[:8] == "binding-" {
			code = "CHANNEL_BINDING_INPUT_INVALID"
		}
		writeError(c, 400, code, "")
		return input, false
	}
	return input, true
}
func respond(c *gin.Context, status int, value any, err error) {
	if err != nil {
		handleError(c, err)
		return
	}
	if r, ok := value.(application.CommandResult); ok {
		contract := "receive-modes-v1"
		if r.Account != nil && r.Account.Provider == domain.Telegram && r.Account.Config.ReceiveMode == "" {
			contract = "webhook-v1"
		}
		c.Header("X-Channel-Result-Contract", contract)
	}
	c.JSON(status, value)
}
func (h *Handler) createAccount(c *gin.Context) {
	a, ok := actor(c)
	if !ok {
		return
	}
	k, ok := key(c)
	if !ok {
		return
	}
	input, ok := decode[application.CreateAccountInput](c, "account-create.schema.json", 64*1024)
	if !ok {
		return
	}
	values := c.Request.Header.Values("X-Channel-Create-Contract")
	if len(values) > 0 {
		if len(values) != 1 || values[0] != "webhook-v1" {
			handleError(c, &domain.Error{Code: domain.InputInvalid, Field: "/X-Channel-Create-Contract"})
			return
		}
		input.LegacyWebhook = true
	}
	r, err := h.commands.CreateAccount(c.Request.Context(), a, k, input)
	respond(c, 201, r, err)
}
func (h *Handler) updateAccount(c *gin.Context) {
	a, ok := actor(c)
	if !ok {
		return
	}
	k, ok := key(c)
	if !ok {
		return
	}
	input, ok := decode[application.UpdateAccountInput](c, "account-update.schema.json", 64*1024)
	if !ok {
		return
	}
	r, err := h.commands.UpdateAccount(c.Request.Context(), a, c.Param("account_id"), k, input)
	respond(c, 200, r, err)
}
func (h *Handler) updateCredential(c *gin.Context) {
	a, ok := actor(c)
	if !ok {
		return
	}
	k, ok := key(c)
	if !ok {
		return
	}
	input, ok := decode[application.UpdateCredentialInput](c, "credential-update.schema.json", 64*1024)
	if !ok {
		return
	}
	r, err := h.commands.UpdateCredential(c.Request.Context(), a, c.Param("account_id"), c.Param("purpose"), k, input)
	respond(c, 200, r, err)
}
func (h *Handler) accountEnabled(c *gin.Context) {
	a, ok := actor(c)
	if !ok {
		return
	}
	k, ok := key(c)
	if !ok {
		return
	}
	input, ok := decode[application.AccountEnabledInput](c, "account-enabled.schema.json", 64*1024)
	if !ok {
		return
	}
	r, err := h.commands.SetAccountEnabled(c.Request.Context(), a, c.Param("account_id"), k, input)
	respond(c, 200, r, err)
}
func (h *Handler) createBinding(c *gin.Context) {
	a, ok := actor(c)
	if !ok {
		return
	}
	k, ok := key(c)
	if !ok {
		return
	}
	input, ok := decode[application.CreateBindingInput](c, "binding-create.schema.json", 16*1024)
	if !ok {
		return
	}
	r, err := h.commands.CreateBinding(c.Request.Context(), a, k, input)
	respond(c, 201, r, err)
}
func (h *Handler) bindingTarget(c *gin.Context) {
	a, ok := actor(c)
	if !ok {
		return
	}
	k, ok := key(c)
	if !ok {
		return
	}
	input, ok := decode[application.BindingTargetInput](c, "binding-target.schema.json", 16*1024)
	if !ok {
		return
	}
	r, err := h.commands.SetBindingTarget(c.Request.Context(), a, c.Param("binding_id"), k, input)
	respond(c, 200, r, err)
}
func (h *Handler) bindingTraffic(c *gin.Context) {
	a, ok := actor(c)
	if !ok {
		return
	}
	k, ok := key(c)
	if !ok {
		return
	}
	input, ok := decode[application.BindingTrafficInput](c, "binding-traffic.schema.json", 64*1024)
	if !ok {
		return
	}
	r, err := h.commands.SetBindingTraffic(c.Request.Context(), a, c.Param("binding_id"), k, input)
	respond(c, 200, r, err)
}
func (h *Handler) bindingEnabled(c *gin.Context) {
	a, ok := actor(c)
	if !ok {
		return
	}
	k, ok := key(c)
	if !ok {
		return
	}
	input, ok := decode[application.BindingEnabledInput](c, "binding-enabled.schema.json", 16*1024)
	if !ok {
		return
	}
	r, err := h.commands.SetBindingEnabled(c.Request.Context(), a, c.Param("binding_id"), k, input)
	respond(c, 200, r, err)
}
func getOnly(c *gin.Context) bool {
	if c.Request.URL.RawQuery != "" {
		writeError(c, 400, "CHANNEL_INPUT_INVALID", "/query")
		return false
	}
	return true
}
func (h *Handler) getAccount(c *gin.Context) {
	a, ok := actor(c)
	if !ok || !getOnly(c) {
		return
	}
	r, err := h.queries.GetAccount(c.Request.Context(), a, c.Param("account_id"))
	respond(c, 200, r, err)
}
func (h *Handler) getBinding(c *gin.Context) {
	a, ok := actor(c)
	if !ok || !getOnly(c) {
		return
	}
	r, err := h.queries.GetBinding(c.Request.Context(), a, c.Param("binding_id"))
	respond(c, 200, r, err)
}
func page(c *gin.Context) (application.Page, bool) {
	values, err := url.ParseQuery(c.Request.URL.RawQuery)
	if err != nil {
		writeError(c, 400, "CHANNEL_INPUT_INVALID", "/query")
		return application.Page{}, false
	}
	p := application.Page{}
	for k, v := range values {
		if len(v) != 1 || v[0] == "" {
			writeError(c, 400, "CHANNEL_INPUT_INVALID", "/query")
			return p, false
		}
		switch k {
		case "cursor":
			p.After = v[0]
		case "page_size":
			p.Limit, err = strconv.Atoi(v[0])
			if err != nil || p.Limit < 1 {
				writeError(c, 400, "CHANNEL_INPUT_INVALID", "/page_size")
				return p, false
			}
		default:
			writeError(c, 400, "CHANNEL_INPUT_INVALID", "/query")
			return p, false
		}
	}
	p, err = application.NormalizePage(p)
	if err != nil {
		handleError(c, err)
		return p, false
	}
	return p, true
}
func (h *Handler) listAccounts(c *gin.Context) {
	a, ok := actor(c)
	if !ok {
		return
	}
	p, ok := page(c)
	if !ok {
		return
	}
	r, err := h.queries.ListAccounts(c.Request.Context(), a, p)
	respond(c, 200, r, err)
}
func (h *Handler) listBindings(c *gin.Context) {
	a, ok := actor(c)
	if !ok {
		return
	}
	p, ok := page(c)
	if !ok {
		return
	}
	r, err := h.queries.ListBindings(c.Request.Context(), a, p)
	respond(c, 200, r, err)
}
func writeError(c *gin.Context, status int, code, field string) {
	c.JSON(status, gin.H{"error": gin.H{"code": code, "field": field, "message": "channel request was not completed"}})
}
func handleError(c *gin.Context, err error) {
	status, code, field := 503, "CHANNEL_DEPENDENCY_UNAVAILABLE", ""
	switch {
	case errors.Is(err, application.ErrPermissionDenied):
		status, code = 403, "CHANNEL_PERMISSION_DENIED"
	case errors.Is(err, application.ErrAccountNotFound):
		status, code = 404, "CHANNEL_ACCOUNT_NOT_FOUND"
	case errors.Is(err, application.ErrBindingNotFound):
		status, code = 404, "CHANNEL_BINDING_NOT_FOUND"
	case errors.Is(err, application.ErrTargetNotFound):
		status, code = 404, "CHANNEL_TARGET_NOT_FOUND"
	case errors.Is(err, application.ErrAccountAlreadyBound):
		status, code = 409, "CHANNEL_ACCOUNT_ALREADY_BOUND"
	case errors.Is(err, application.ErrIdentityConflict):
		status, code = 409, "CHANNEL_ACCOUNT_IDENTITY_CONFLICT"
	case errors.Is(err, application.ErrIdempotencyConflict):
		status, code = 409, "CHANNEL_IDEMPOTENCY_CONFLICT"
	}
	var d *domain.Error
	if errors.As(err, &d) {
		code, field = d.Code, d.Field
		switch code {
		case domain.InputInvalid:
			status = 400
		case domain.CredentialRequired:
			status = 422
		case "CHANNEL_LIMIT_EXCEEDED":
			status = 422
		case domain.SourceIntegrity, domain.TargetIntegrity:
			status = 500
		default:
			status = 409
		}
	}
	writeError(c, status, code, field)
}

func (h *Handler) authorizeWrite(c *gin.Context) {
	a, ok := actor(c)
	if !ok {
		c.Abort()
		return
	}
	if err := h.commands.AuthorizeWrite(c.Request.Context(), a); err != nil {
		handleError(c, err)
		c.Abort()
		return
	}
}
