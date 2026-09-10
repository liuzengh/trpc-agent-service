package admin

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	platformapproval "github.com/liuzengh/trpc-agent-service/trpcservice/approval"
	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const maxAdminRequestBytes = 1 << 20

// NewHTTPHandler creates a local-development System Admin handler using one
// independent control-plane bearer token. The token is never accepted as a
// data-plane API credential.
func NewHTTPHandler(api API, token string) (http.Handler, error) {
	return NewHTTPHandlerWithAuth(api, AdminAuthConfig{SystemAdminToken: token})
}

// NewHTTPHandlerWithAuth creates token-protected control-plane endpoints with
// server-derived roles and tenant scopes.
func NewHTTPHandlerWithAuth(api API, authConfig AdminAuthConfig) (http.Handler, error) {
	if api.Repository == nil {
		return nil, errors.New("admin repository is required")
	}
	credentials, err := authConfig.credentials()
	if err != nil {
		return nil, err
	}

	handler := adminHTTPHandler{api: api, credentials: credentials}
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/session", methodHandler(http.MethodGet, handler.session))
	mux.HandleFunc("/admin/v1/overview", methodHandler(http.MethodGet, handler.operations))
	mux.HandleFunc("/admin/v1/operations", methodHandler(http.MethodGet, handler.operations))
	mux.HandleFunc("/admin/v1/tenants", methodRouter(map[string]http.HandlerFunc{
		http.MethodGet:  handler.requireRoles(handler.listTenants, RoleSystemAdmin, RoleOperator, RoleAuditor),
		http.MethodPost: handler.requireRoles(handler.createTenant, RoleSystemAdmin),
	}))
	mux.HandleFunc("/admin/v1/apps", methodRouter(map[string]http.HandlerFunc{
		http.MethodGet:  handler.requireRoles(handler.listAgentApps, RoleSystemAdmin, RoleOperator, RoleAuditor),
		http.MethodPost: handler.requireRoles(handler.createAgentApp, RoleSystemAdmin, RoleOperator),
	}))
	mux.HandleFunc("/admin/v1/channel-bindings", methodRouter(map[string]http.HandlerFunc{
		http.MethodGet:  handler.requireRoles(handler.listChannelBindings, RoleSystemAdmin, RoleOperator, RoleAuditor),
		http.MethodPost: handler.requireRoles(handler.createChannelBinding, RoleSystemAdmin, RoleOperator),
	}))
	mux.HandleFunc("/admin/v1/channel-bindings/enable", methodHandler(http.MethodPost, handler.requireRoles(handler.enableChannelBinding, RoleSystemAdmin, RoleOperator)))
	mux.HandleFunc("/admin/v1/channel-bindings/suspend", methodHandler(http.MethodPost, handler.requireRoles(handler.suspendChannelBinding, RoleSystemAdmin, RoleOperator)))
	mux.HandleFunc("/admin/v1/configs", methodRouter(map[string]http.HandlerFunc{
		http.MethodGet:  handler.requireRoles(handler.listAppConfigs, RoleSystemAdmin, RoleOperator, RoleAuditor),
		http.MethodPost: handler.requireRoles(handler.publishAppConfig, RoleSystemAdmin, RoleOperator),
	}))
	mux.HandleFunc("/admin/v1/configs/activate", methodHandler(http.MethodPost, handler.requireRoles(handler.activateAppConfig, RoleSystemAdmin, RoleOperator)))
	mux.HandleFunc("/admin/v1/configs/rollback", methodHandler(http.MethodPost, handler.requireRoles(handler.rollbackAppConfig, RoleSystemAdmin, RoleOperator)))
	mux.HandleFunc("/admin/v1/configs/canary/enable", methodHandler(http.MethodPost, handler.requireRoles(handler.enableAppCanary, RoleSystemAdmin, RoleOperator)))
	mux.HandleFunc("/admin/v1/configs/canary/pause", methodHandler(http.MethodPost, handler.requireRoles(handler.pauseAppCanary, RoleSystemAdmin, RoleOperator)))
	mux.HandleFunc("/admin/v1/configs/canary/disable", methodHandler(http.MethodPost, handler.requireRoles(handler.disableAppCanary, RoleSystemAdmin, RoleOperator)))
	mux.HandleFunc("/admin/v1/configs/canary/rollback", methodHandler(http.MethodPost, handler.requireRoles(handler.rollbackAppCanary, RoleSystemAdmin, RoleOperator)))
	mux.HandleFunc("/admin/v1/configs/canary/promote", methodHandler(http.MethodPost, handler.requireRoles(handler.promoteAppCanary, RoleSystemAdmin, RoleOperator)))
	mux.HandleFunc("/admin/v1/configs/canary/evaluate", methodHandler(http.MethodPost, handler.requireRoles(handler.evaluateAppCanary, RoleSystemAdmin, RoleOperator)))
	mux.HandleFunc("/admin/v1/data-migrations", methodRouter(map[string]http.HandlerFunc{
		http.MethodGet:  handler.requireRoles(handler.listDataMigrations, RoleSystemAdmin, RoleOperator, RoleAuditor),
		http.MethodPost: handler.requireRoles(handler.createDataMigration, RoleSystemAdmin, RoleOperator),
	}))
	mux.HandleFunc("/admin/v1/data-migrations/begin", methodHandler(http.MethodPost, handler.requireRoles(handler.beginDataMigration, RoleSystemAdmin, RoleOperator)))
	mux.HandleFunc("/admin/v1/credentials", methodHandler(http.MethodPost, handler.requireRoles(handler.issueCredential, RoleSystemAdmin)))
	mux.HandleFunc("/admin/v1/credentials/revoke", methodHandler(http.MethodPost, handler.requireRoles(handler.revokeCredential, RoleSystemAdmin)))
	mux.HandleFunc("/admin/v1/executions", methodHandler(http.MethodGet, handler.requireRoles(handler.listExecutions, RoleSystemAdmin, RoleOperator, RoleAuditor)))
	mux.HandleFunc("/admin/v1/audit-events", methodHandler(http.MethodGet, handler.requireRoles(handler.listAuditEvents, RoleSystemAdmin, RoleOperator, RoleAuditor)))
	mux.HandleFunc("/admin/v1/approvals", methodHandler(http.MethodGet, handler.requireRoles(handler.listApprovals, RoleSystemAdmin, RoleOperator, RoleAuditor)))
	mux.HandleFunc("/admin/v1/approvals/", methodHandler(http.MethodPost, handler.requireRoles(handler.decideApproval, RoleSystemAdmin, RoleOperator)))
	return handler.authorize(mux), nil
}

func methodHandler(method string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.Header().Set("Allow", method)
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		next(w, r)
	}
}

func methodRouter(handlers map[string]http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		next, ok := handlers[r.Method]
		if !ok {
			allowed := make([]string, 0, len(handlers))
			for method := range handlers {
				allowed = append(allowed, method)
			}
			slices.Sort(allowed)
			w.Header().Set("Allow", strings.Join(allowed, ", "))
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		next(w, r)
	}
}

type adminHTTPHandler struct {
	api         API
	credentials []adminCredential
}

type createTenantRequest struct {
	Tenant tenant.Tenant `json:"tenant"`
}

type createAgentAppRequest struct {
	App           tenant.AgentApp  `json:"app"`
	InitialConfig tenant.AppConfig `json:"initial_config"`
}

type createChannelBindingRequest struct {
	Binding channels.Binding `json:"binding"`
}

type channelBindingStatusRequest struct {
	TenantID  string `json:"tenant_id"`
	AppID     string `json:"app_id"`
	BindingID string `json:"binding_id"`
}

type issueCredentialRequest struct {
	TenantID  string     `json:"tenant_id"`
	AppID     string     `json:"app_id"`
	ExpiresAt *time.Time `json:"expires_at"`
}

type publishAppConfigRequest struct {
	Config tenant.AppConfig `json:"config"`
}

type activateAppConfigRequest struct {
	TenantID string `json:"tenant_id"`
	AppID    string `json:"app_id"`
	Version  string `json:"version"`
}

type canaryRequest struct {
	TenantID   string `json:"tenant_id"`
	AppID      string `json:"app_id"`
	Version    string `json:"version,omitempty"`
	Percentage int    `json:"percentage,omitempty"`
}

type canaryEvaluationRequest struct {
	TenantID            string   `json:"tenant_id"`
	AppID               string   `json:"app_id"`
	Version             string   `json:"version"`
	Samples             int64    `json:"samples"`
	ErrorRate           float64  `json:"error_rate"`
	P95LatencyMS        int64    `json:"p95_latency_ms"`
	BudgetRejections    int64    `json:"budget_rejections"`
	MinimumSamples      int64    `json:"minimum_samples"`
	MaxErrorRate        *float64 `json:"max_error_rate,omitempty"`
	MaxP95LatencyMS     *int64   `json:"max_p95_latency_ms,omitempty"`
	MaxBudgetRejections *int64   `json:"max_budget_rejections,omitempty"`
	Action              string   `json:"action"`
}

type revokeCredentialRequest struct {
	TenantID     string `json:"tenant_id"`
	AppID        string `json:"app_id"`
	CredentialID string `json:"credential_id"`
}

type createDataMigrationRequest struct {
	TenantID      string           `json:"tenant_id"`
	AppID         string           `json:"app_id"`
	Domain        migration.Domain `json:"domain,omitempty"`
	SourceVersion string           `json:"source_config_version"`
	TargetVersion string           `json:"target_config_version"`
}

type beginDataMigrationRequest struct {
	TenantID      string    `json:"tenant_id"`
	AppID         string    `json:"app_id"`
	MigrationID   string    `json:"migration_id"`
	Owner         string    `json:"owner"`
	DrainDeadline time.Time `json:"drain_deadline"`
	LeaseDuration string    `json:"lease_duration"`
}

type issueCredentialResponse struct {
	Credential credentialResponse `json:"credential"`
}

type credentialResponse struct {
	ID        string     `json:"id"`
	TenantID  string     `json:"tenant_id"`
	AppID     string     `json:"app_id"`
	KeyPrefix string     `json:"key_prefix"`
	Status    string     `json:"status"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

type auditEventsResponse struct {
	Events []platformaudit.Event `json:"events"`
}

type approvalsResponse struct {
	Approvals []platformapproval.Record `json:"approvals"`
}

type tenantsResponse struct {
	Tenants []TenantView `json:"tenants"`
}

type appsResponse struct {
	Apps []AgentAppView `json:"apps"`
}

type appConfigsResponse struct {
	Configs []AppConfigView `json:"configs"`
}

type publishedAppConfigResponse struct {
	TenantID string `json:"tenant_id"`
	AppID    string `json:"app_id"`
	Version  string `json:"version"`
	Status   string `json:"status"`
}

type channelBindingsResponse struct {
	Bindings []ChannelBindingView `json:"bindings"`
}

type executionsResponse struct {
	Executions []ExecutionView `json:"executions"`
}

type dataMigrationsResponse struct {
	Migrations []migration.Record `json:"migrations"`
}

type adminSessionResponse struct {
	Role      AdminRole `json:"role"`
	ActorID   string    `json:"actor_id"`
	TenantIDs []string  `json:"tenant_ids,omitempty"`
}

type approvalDecisionRequest struct {
	TenantID string `json:"tenant_id"`
	AppID    string `json:"app_id"`
}

func (h adminHTTPHandler) authorize(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := h.authenticate(r)
		if !ok {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeJSONError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r.WithContext(withPrincipal(r.Context(), principal)))
	})
}

func (h adminHTTPHandler) session(w http.ResponseWriter, r *http.Request) {
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	writeJSON(w, http.StatusOK, adminSessionResponse{
		Role: principal.Role, ActorID: principal.ActorID, TenantIDs: slices.Clone(principal.TenantIDs),
	})
}

func (h adminHTTPHandler) operations(w http.ResponseWriter, r *http.Request) {
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	value, err := h.api.OperationsForPrincipal(r.Context(), principal)
	if err != nil {
		writeAdminOperationError(w, err, "read operations failed")
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h adminHTTPHandler) validAuthorization(r *http.Request) bool {
	_, ok := h.authenticate(r)
	return ok
}

func (h adminHTTPHandler) authenticate(r *http.Request) (AdminPrincipal, bool) {
	if r == nil {
		return AdminPrincipal{}, false
	}
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		return AdminPrincipal{}, false
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return AdminPrincipal{}, false
	}
	var principal AdminPrincipal
	matched := false
	for _, credential := range h.credentials {
		if subtle.ConstantTimeCompare([]byte(parts[1]), []byte(credential.token)) == 1 {
			principal = credential.principal
			matched = true
		}
	}
	return principal, matched
}

func (h adminHTTPHandler) requireRoles(next http.HandlerFunc, roles ...AdminRole) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		principal, ok := PrincipalFromContext(r.Context())
		if !ok {
			writeJSONError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		for _, role := range roles {
			if principal.Role == role {
				next(w, r)
				return
			}
		}
		writeJSONError(w, http.StatusForbidden, "forbidden")
	}
}

// authorizeTenant enforces the tenant boundary after decoding the operation
// scope. Role authorization alone is insufficient for operator and auditor
// credentials because the tenant is carried in the JSON command.
func (h adminHTTPHandler) authorizeTenant(w http.ResponseWriter, r *http.Request, scope tenant.Scope) bool {
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized")
		return false
	}
	if !principal.AllowsTenant(scope.TenantID) {
		writeJSONError(w, http.StatusForbidden, "forbidden")
		return false
	}
	return true
}

func parseListOptions(w http.ResponseWriter, r *http.Request, tenantRequired bool) (ListOptions, bool) {
	query := r.URL.Query()
	options := ListOptions{
		TenantID: strings.TrimSpace(query.Get("tenant_id")),
		AppID:    strings.TrimSpace(query.Get("app_id")),
	}
	if tenantRequired && options.TenantID == "" {
		writeJSONError(w, http.StatusBadRequest, "tenant_id is required")
		return ListOptions{}, false
	}
	if raw := strings.TrimSpace(query.Get("limit")); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit <= 0 || limit > maxAdminListLimit {
			writeJSONError(w, http.StatusBadRequest, "invalid list limit")
			return ListOptions{}, false
		}
		options.Limit = limit
	}
	return options, true
}

func (h adminHTTPHandler) listTenants(w http.ResponseWriter, r *http.Request) {
	options, ok := parseListOptions(w, r, false)
	if !ok {
		return
	}
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	values, err := h.api.ListTenantsForPrincipal(r.Context(), principal, options)
	if err != nil {
		writeAdminOperationError(w, err, "list tenants failed")
		return
	}
	writeJSON(w, http.StatusOK, tenantsResponse{Tenants: values})
}

func (h adminHTTPHandler) listAgentApps(w http.ResponseWriter, r *http.Request) {
	options, ok := parseListOptions(w, r, true)
	if !ok {
		return
	}
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	values, err := h.api.ListAgentAppsForPrincipal(r.Context(), principal, options)
	if err != nil {
		writeAdminOperationError(w, err, "list agent apps failed")
		return
	}
	writeJSON(w, http.StatusOK, appsResponse{Apps: values})
}

func (h adminHTTPHandler) listAppConfigs(w http.ResponseWriter, r *http.Request) {
	options, ok := parseListOptions(w, r, true)
	if !ok {
		return
	}
	if options.AppID == "" {
		writeJSONError(w, http.StatusBadRequest, "app_id is required")
		return
	}
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	values, err := h.api.ListAppConfigsForPrincipal(r.Context(), principal, options)
	if err != nil {
		writeAdminOperationError(w, err, "list app configs failed")
		return
	}
	writeJSON(w, http.StatusOK, appConfigsResponse{Configs: values})
}

func (h adminHTTPHandler) listChannelBindings(w http.ResponseWriter, r *http.Request) {
	options, ok := parseListOptions(w, r, true)
	if !ok {
		return
	}
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	values, err := h.api.ListChannelBindingsForPrincipal(r.Context(), principal, options)
	if err != nil {
		writeAdminOperationError(w, err, "list channel bindings failed")
		return
	}
	writeJSON(w, http.StatusOK, channelBindingsResponse{Bindings: values})
}

func (h adminHTTPHandler) listDataMigrations(w http.ResponseWriter, r *http.Request) {
	options, ok := parseListOptions(w, r, true)
	if !ok {
		return
	}
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	values, err := h.api.ListDataMigrationsForPrincipal(r.Context(), principal, options)
	if err != nil {
		writeAdminOperationError(w, err, "list data migrations failed")
		return
	}
	writeJSON(w, http.StatusOK, dataMigrationsResponse{Migrations: values})
}

func (h adminHTTPHandler) listExecutions(w http.ResponseWriter, r *http.Request) {
	options, ok := parseListOptions(w, r, true)
	if !ok {
		return
	}
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	values, err := h.api.ListExecutionsForPrincipal(r.Context(), principal, options)
	if err != nil {
		writeAdminOperationError(w, err, "list executions failed")
		return
	}
	writeJSON(w, http.StatusOK, executionsResponse{Executions: values})
}

func (h adminHTTPHandler) createTenant(w http.ResponseWriter, r *http.Request) {
	var request createTenantRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, "invalid tenant")
		return
	}
	if err := h.api.CreateTenant(r.Context(), request.Tenant); err != nil {
		writeAdminOperationError(w, err, "create tenant failed")
		return
	}
	writeJSON(w, http.StatusCreated, request.Tenant)
}

func (h adminHTTPHandler) createAgentApp(w http.ResponseWriter, r *http.Request) {
	var request createAgentAppRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, "invalid agent app")
		return
	}
	if !h.authorizeTenant(w, r, tenant.Scope{TenantID: request.App.TenantID}) {
		return
	}
	if err := h.api.CreateAgentApp(r.Context(), request.App, request.InitialConfig); err != nil {
		writeAdminOperationError(w, err, "create agent app failed")
		return
	}
	writeJSON(w, http.StatusCreated, request.App)
}

func (h adminHTTPHandler) createChannelBinding(w http.ResponseWriter, r *http.Request) {
	var request createChannelBindingRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, "invalid channel binding")
		return
	}
	if !h.authorizeTenant(w, r, tenant.Scope{TenantID: request.Binding.TenantID}) {
		return
	}
	binding, err := h.api.ProvisionChannelBinding(r.Context(), request.Binding)
	if err != nil {
		writeAdminOperationError(w, err, "create channel binding failed")
		return
	}
	writeJSON(w, http.StatusCreated, sanitizeBinding(binding))
}

func (h adminHTTPHandler) enableChannelBinding(w http.ResponseWriter, r *http.Request) {
	h.setChannelBindingStatus(w, r, channels.BindingActive)
}

func (h adminHTTPHandler) suspendChannelBinding(w http.ResponseWriter, r *http.Request) {
	h.setChannelBindingStatus(w, r, channels.BindingSuspended)
}

func (h adminHTTPHandler) setChannelBindingStatus(w http.ResponseWriter, r *http.Request, status channels.BindingStatus) {
	var request channelBindingStatusRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, "invalid channel binding status request")
		return
	}
	if !h.authorizeTenant(w, r, tenant.Scope{TenantID: request.TenantID}) {
		return
	}
	binding, err := h.api.SetChannelBindingStatus(r.Context(), tenant.Scope{
		TenantID: request.TenantID, AppID: request.AppID,
	}, request.BindingID, status)
	if err != nil {
		writeAdminOperationError(w, err, "set channel binding status failed")
		return
	}
	writeJSON(w, http.StatusOK, sanitizeBinding(binding))
}

func (h adminHTTPHandler) publishAppConfig(w http.ResponseWriter, r *http.Request) {
	var request publishAppConfigRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, "invalid app config")
		return
	}
	if !h.authorizeTenant(w, r, tenant.Scope{TenantID: request.Config.TenantID}) {
		return
	}
	if err := h.api.PublishAppConfig(r.Context(), request.Config); err != nil {
		writeAdminOperationError(w, err, "publish app config failed")
		return
	}
	writeJSON(w, http.StatusCreated, publishedAppConfigResponse{
		TenantID: request.Config.TenantID,
		AppID:    request.Config.AppID,
		Version:  request.Config.Version,
		Status:   "PUBLISHED",
	})
}

func (h adminHTTPHandler) activateAppConfig(w http.ResponseWriter, r *http.Request) {
	h.setActiveAppConfig(w, r, h.api.ActivateAppConfig, "activate app config failed", "invalid app config activation")
}

func (h adminHTTPHandler) rollbackAppConfig(w http.ResponseWriter, r *http.Request) {
	h.setActiveAppConfig(w, r, h.api.RollbackAppConfig, "rollback app config failed", "invalid app config rollback")
}

func (h adminHTTPHandler) setActiveAppConfig(
	w http.ResponseWriter,
	r *http.Request,
	mutate func(context.Context, tenant.Scope, string) error,
	failureMessage, inputMessage string,
) {
	var request activateAppConfigRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, inputMessage)
		return
	}
	scope := tenant.Scope{TenantID: request.TenantID, AppID: request.AppID}
	if !h.authorizeTenant(w, r, scope) {
		return
	}
	if err := mutate(r.Context(), scope, request.Version); err != nil {
		writeAdminOperationError(w, err, failureMessage)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h adminHTTPHandler) enableAppCanary(w http.ResponseWriter, r *http.Request) {
	var request canaryRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, "invalid canary enable request")
		return
	}
	scope := tenant.Scope{TenantID: request.TenantID, AppID: request.AppID}
	if !h.authorizeTenant(w, r, scope) {
		return
	}
	value, err := h.api.EnableAppCanary(r.Context(), scope, request.Version, request.Percentage)
	if err != nil {
		writeAdminOperationError(w, err, "enable app canary failed")
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h adminHTTPHandler) pauseAppCanary(w http.ResponseWriter, r *http.Request) {
	h.mutateAppCanary(w, r, h.api.PauseAppCanary, "pause app canary failed")
}

func (h adminHTTPHandler) disableAppCanary(w http.ResponseWriter, r *http.Request) {
	h.mutateAppCanary(w, r, h.api.DisableAppCanary, "disable app canary failed")
}

func (h adminHTTPHandler) rollbackAppCanary(w http.ResponseWriter, r *http.Request) {
	h.mutateAppCanary(w, r, h.api.RollbackAppCanary, "rollback app canary failed")
}

func (h adminHTTPHandler) promoteAppCanary(w http.ResponseWriter, r *http.Request) {
	h.mutateAppCanary(w, r, h.api.PromoteAppCanary, "promote app canary failed")
}

func (h adminHTTPHandler) evaluateAppCanary(w http.ResponseWriter, r *http.Request) {
	var request canaryEvaluationRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, "invalid canary evaluation request")
		return
	}
	scope := tenant.Scope{TenantID: request.TenantID, AppID: request.AppID}
	if !h.authorizeTenant(w, r, scope) {
		return
	}
	p95Latency, err := durationFromMilliseconds(request.P95LatencyMS)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid canary p95 latency")
		return
	}
	var maxP95Latency *time.Duration
	if request.MaxP95LatencyMS != nil {
		value, durationErr := durationFromMilliseconds(*request.MaxP95LatencyMS)
		if durationErr != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid canary p95 latency threshold")
			return
		}
		maxP95Latency = &value
	}
	result, err := h.api.EvaluateAppCanary(
		r.Context(),
		scope,
		request.Version,
		tenant.CanaryObservations{
			Samples:          request.Samples,
			ErrorRate:        request.ErrorRate,
			P95Latency:       p95Latency,
			BudgetRejections: request.BudgetRejections,
		},
		tenant.CanaryRule{
			MinimumSamples:      request.MinimumSamples,
			MaxErrorRate:        request.MaxErrorRate,
			MaxP95Latency:       maxP95Latency,
			MaxBudgetRejections: request.MaxBudgetRejections,
			Action:              tenant.CanaryAction(request.Action),
		},
	)
	if err != nil {
		writeAdminOperationError(w, err, "evaluate app canary failed")
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func durationFromMilliseconds(value int64) (time.Duration, error) {
	if value < 0 {
		return 0, errors.New("duration must be non-negative")
	}
	const maxDurationMilliseconds = int64((1<<63 - 1) / int64(time.Millisecond))
	if value > maxDurationMilliseconds {
		return 0, errors.New("duration is too large")
	}
	duration := time.Duration(value) * time.Millisecond
	return duration, nil
}

func (h adminHTTPHandler) mutateAppCanary(
	w http.ResponseWriter,
	r *http.Request,
	operation func(context.Context, tenant.Scope) (tenant.AgentApp, error),
	failureMessage string,
) {
	var request canaryRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, "invalid canary request")
		return
	}
	scope := tenant.Scope{TenantID: request.TenantID, AppID: request.AppID}
	if !h.authorizeTenant(w, r, scope) {
		return
	}
	value, err := operation(r.Context(), scope)
	if err != nil {
		writeAdminOperationError(w, err, failureMessage)
		return
	}
	writeJSON(w, http.StatusOK, value)
}

func (h adminHTTPHandler) createDataMigration(w http.ResponseWriter, r *http.Request) {
	var request createDataMigrationRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, "invalid data migration")
		return
	}
	scope := tenant.Scope{
		TenantID: request.TenantID,
		AppID:    request.AppID,
	}
	if !h.authorizeTenant(w, r, scope) {
		return
	}
	domain := request.Domain
	if domain == "" {
		domain = migration.DomainSession
	}
	var record migration.Record
	var err error
	switch domain {
	case migration.DomainSession:
		record, err = h.api.CreateDataMigration(r.Context(), scope, request.SourceVersion, request.TargetVersion)
	case migration.DomainKnowledge:
		record, err = h.api.CreateKnowledgeMigration(r.Context(), scope, request.SourceVersion, request.TargetVersion)
	default:
		writeJSONError(w, http.StatusBadRequest, "invalid data migration")
		return
	}
	if err != nil {
		writeAdminOperationError(w, err, "create data migration failed")
		return
	}
	writeJSON(w, http.StatusCreated, record)
}

func (h adminHTTPHandler) beginDataMigration(w http.ResponseWriter, r *http.Request) {
	var request beginDataMigrationRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, "invalid data migration")
		return
	}
	if !h.authorizeTenant(w, r, tenant.Scope{TenantID: request.TenantID}) {
		return
	}
	leaseDuration := time.Duration(0)
	if strings.TrimSpace(request.LeaseDuration) != "" {
		parsed, err := time.ParseDuration(request.LeaseDuration)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid data migration")
			return
		}
		leaseDuration = parsed
	}
	record, err := h.api.BeginDataMigration(r.Context(), tenant.Scope{
		TenantID: request.TenantID,
		AppID:    request.AppID,
	}, request.MigrationID, request.Owner, request.DrainDeadline, leaseDuration)
	if err != nil {
		writeAdminOperationError(w, err, "begin data migration failed")
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func (h adminHTTPHandler) issueCredential(w http.ResponseWriter, r *http.Request) {
	var request issueCredentialRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, "invalid credential request")
		return
	}
	scope := tenant.Scope{TenantID: request.TenantID, AppID: request.AppID}
	expiresAt := time.Time{}
	if request.ExpiresAt != nil {
		expiresAt = *request.ExpiresAt
	}
	issued, err := h.api.IssueCredential(r.Context(), scope, expiresAt)
	if err != nil {
		writeAdminOperationError(w, err, "issue credential failed")
		return
	}
	var responseExpiresAt *time.Time
	if !issued.Credential.ExpiresAt.IsZero() {
		value := issued.Credential.ExpiresAt
		responseExpiresAt = &value
	}
	writeJSON(w, http.StatusCreated, issueCredentialResponse{
		Credential: credentialResponse{
			ID:        issued.Credential.ID,
			TenantID:  issued.Credential.TenantID,
			AppID:     issued.Credential.AppID,
			KeyPrefix: issued.Credential.KeyPrefix,
			Status:    string(issued.Credential.Status),
			ExpiresAt: responseExpiresAt,
		},
	})
}

func (h adminHTTPHandler) revokeCredential(w http.ResponseWriter, r *http.Request) {
	var request revokeCredentialRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, "invalid credential revocation")
		return
	}
	scope := tenant.Scope{TenantID: request.TenantID, AppID: request.AppID}
	if err := h.api.RevokeCredential(r.Context(), scope, request.CredentialID); err != nil {
		writeAdminOperationError(w, err, "revoke credential failed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h adminHTTPHandler) listAuditEvents(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limit := 0
	if rawLimit := strings.TrimSpace(query.Get("limit")); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed <= 0 || parsed > 1000 {
			writeJSONError(w, http.StatusBadRequest, "invalid audit limit")
			return
		}
		limit = parsed
	}
	offset := 0
	if rawOffset := strings.TrimSpace(query.Get("offset")); rawOffset != "" {
		parsed, err := strconv.Atoi(rawOffset)
		if err != nil || parsed < 0 || parsed > 1_000_000 {
			writeJSONError(w, http.StatusBadRequest, "invalid audit offset")
			return
		}
		offset = parsed
	}
	filter := platformaudit.Query{
		TenantID:  strings.TrimSpace(query.Get("tenant_id")),
		AppID:     strings.TrimSpace(query.Get("app_id")),
		EventType: strings.TrimSpace(query.Get("event_type")),
		ToolName:  strings.TrimSpace(query.Get("tool_name")),
		TraceID:   strings.TrimSpace(query.Get("trace_id")),
		Limit:     limit,
		Offset:    offset,
	}
	if raw := strings.TrimSpace(query.Get("created_after")); raw != "" {
		value, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid audit created_after")
			return
		}
		filter.CreatedAfter = &value
	}
	if raw := strings.TrimSpace(query.Get("created_before")); raw != "" {
		value, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid audit created_before")
			return
		}
		filter.CreatedBefore = &value
	}
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	events, err := h.api.ListAuditEventsForPrincipal(r.Context(), principal, filter)
	if err != nil {
		writeAdminOperationError(w, err, "list audit events failed")
		return
	}
	writeJSON(w, http.StatusOK, auditEventsResponse{Events: events})
}

func (h adminHTTPHandler) listApprovals(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limit := 0
	if raw := strings.TrimSpace(query.Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed <= 0 || parsed > 1000 {
			writeJSONError(w, http.StatusBadRequest, "invalid approval limit")
			return
		}
		limit = parsed
	}
	status := platformapproval.Status(strings.TrimSpace(query.Get("status")))
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	approvals, err := h.api.ListApprovalsForPrincipal(r.Context(), principal, platformapproval.Query{
		TenantID: strings.TrimSpace(query.Get("tenant_id")),
		AppID:    strings.TrimSpace(query.Get("app_id")),
		Status:   status,
		Limit:    limit,
	})
	if err != nil {
		writeAdminOperationError(w, err, "list approvals failed")
		return
	}
	writeJSON(w, http.StatusOK, approvalsResponse{Approvals: approvals})
}

func (h adminHTTPHandler) decideApproval(w http.ResponseWriter, r *http.Request) {
	principal, ok := PrincipalFromContext(r.Context())
	if !ok {
		writeJSONError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/admin/v1/approvals/"), "/"), "/")
	if len(parts) != 2 || parts[0] == "" || (parts[1] != "approve" && parts[1] != "deny") {
		http.NotFound(w, r)
		return
	}
	var request approvalDecisionRequest
	request.TenantID = strings.TrimSpace(r.URL.Query().Get("tenant_id"))
	request.AppID = strings.TrimSpace(r.URL.Query().Get("app_id"))
	if r.Body != nil && r.Body != http.NoBody && r.ContentLength != 0 {
		if !decodeJSON(w, r, &request) {
			writeJSONError(w, http.StatusBadRequest, "invalid approval decision")
			return
		}
	}
	status := platformapproval.StatusDenied
	if parts[1] == "approve" {
		status = platformapproval.StatusApproved
	}
	if !principal.AllowsTenant(strings.TrimSpace(request.TenantID)) {
		writeJSONError(w, http.StatusForbidden, "forbidden")
		return
	}
	record, err := h.api.DecideApproval(r.Context(), tenant.Scope{
		TenantID: strings.TrimSpace(request.TenantID),
		AppID:    strings.TrimSpace(request.AppID),
	}, parts[0], status)
	if err != nil {
		writeAdminOperationError(w, err, "decide approval failed")
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	if r == nil || r.Body == nil {
		return false
	}
	body := http.MaxBytesReader(w, r.Body, maxAdminRequestBytes)
	defer func() {
		if err := body.Close(); err != nil {
			log.Printf("close admin request body: %v", err)
		}
	}()
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return false
	}
	var trailing any
	return decoder.Decode(&trailing) == io.EOF
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write admin json response: %v", err)
	}
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, struct {
		Error string `json:"error"`
	}{Error: message})
}

func writeAdminOperationError(w http.ResponseWriter, err error, message string) {
	status := http.StatusInternalServerError
	if errors.Is(err, ErrForbidden) {
		status = http.StatusForbidden
	}
	var inputErr *inputError
	if errors.As(err, &inputErr) {
		status = http.StatusBadRequest
	}
	if errors.Is(err, platformapproval.ErrNotFound) {
		status = http.StatusNotFound
	}
	if errors.Is(err, platformapproval.ErrAlreadyDecided) || errors.Is(err, platformapproval.ErrExpired) {
		status = http.StatusConflict
	}
	if errors.Is(err, tenant.ErrCanaryDecisionStale) {
		status = http.StatusConflict
	}
	writeJSONError(w, status, message)
}
