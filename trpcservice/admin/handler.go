package admin

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"

	"github.com/liuzengh/trpc-agent-service/trpcservice/approval"
	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/background"
	"github.com/liuzengh/trpc-agent-service/trpcservice/console"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/toolexec"
)

type Handler struct {
	loginLimit loginLimiter
	service    *Service
	principals []Principal
}

func NewHandler(service *Service, token string) (*Handler, error) {
	return NewHandlerWithPrincipals(service, []Principal{{
		Name: "legacy-superadmin", Token: token, Role: RoleSuperAdmin,
	}})
}

func NewHandlerWithPrincipals(service *Service, principals []Principal) (*Handler, error) {
	if service == nil {
		return nil, errors.New("admin service is required")
	}
	if err := ValidatePrincipals(principals); err != nil {
		return nil, err
	}
	return &Handler{service: service, principals: append([]Principal(nil), principals...)}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if h.serveUI(w, r) {
		return
	}
	if r.URL.Path == "/admin/login" {
		h.login(w, r)
		return
	}
	principal, csrf, authErr := h.authenticateRequest(r)
	if authErr != nil {
		if !errors.Is(authErr, errLogin) {
			h.writeResult(w, 0, nil, authErr)
			return
		}
		w.Header().Set("WWW-Authenticate", "Bearer")
		adminJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	r = r.WithContext(contextWithPrincipal(r.Context(), principal))
	if !sameOrigin(r) {
		adminJSON(w, http.StatusForbidden, map[string]string{"error": "cross-origin admin request rejected"})
		return
	}
	if r.URL.Path == "/admin/session" && r.Method == http.MethodGet {
		adminJSON(w, 200, publicIdentity(principal, csrf))
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		adminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if csrf != "" && (r.Header.Get("Origin") == "" || subtle.ConstantTimeCompare([]byte(csrf), []byte(r.Header.Get("X-CSRF-Token"))) != 1) {
		adminJSON(w, 403, map[string]string{"error": "CSRF validation failed"})
		return
	}
	switch r.URL.Path {
	case "/admin/releases/list":
		h.handleReleases(w, r)
	case "/admin/jobs/list":
		h.handleJobsList(w, r)
	case "/admin/channel-bindings/diagnostics":
		h.handleChannelDiagnostics(w, r)
	case "/admin/apps/settings":
		h.handleAppSettings(w, r)
	case "/admin/resources/list":
		h.handleResources(w, r)
	case "/admin/system/status", "/admin/system/probe":
		h.handleSystem(w, r)
	case "/admin/runs/list", "/admin/runs/get":
		h.handleRuns(w, r)
	case "/admin/apps/onboard":
		h.onboardApp(w, r)
	case "/admin/debug/sessions", "/admin/debug/latest", "/admin/debug/send", "/admin/debug/get", "/admin/debug/decision", "/admin/debug/cancel", "/admin/debug/events", "/admin/debug/live":
		h.handleDebug(w, r)
	case "/admin/workspace":
		h.handleWorkspace(w, r)
	case "/admin/drafts/get", "/admin/drafts/save", "/admin/drafts/reset", "/admin/drafts/publish", "/admin/drafts/readiness":
		h.handleDrafts(w, r)
	case "/admin/logout":
		h.logout(w, r)
	case "/admin/me":
		adminJSON(w, http.StatusOK, map[string]any{"name": principal.Name, "role": principal.Role, "tenant_ids": principal.TenantIDs})
	case "/admin/catalog/list":
		h.handleCatalog(w, r)
	case "/admin/skills/list":
		var input struct {
			TenantID string `json:"tenant_id"`
		}
		if !decodeAdmin(w, r, &input) || !h.require(w, r, input.TenantID, PermissionRead) {
			return
		}
		adminJSON(w, http.StatusOK, map[string]any{"items": h.service.skills.List(input.TenantID)})
	case "/admin/outbound-parts/list", "/admin/outbound-parts/reconcile":
		h.handleOutboundParts(w, r)
	case "/admin/channel-rejections/list", "/admin/channel-checkpoints/list", "/admin/channel-checkpoints/recover":
		h.handleChannelState(w, r)
	case "/admin/tool-executions/list", "/admin/tool-operations/list", "/admin/tool-operations/get", "/admin/tool-operations/reconcile":
		h.handleToolOperations(w, r)
	case "/admin/tenants/get":
		var input struct {
			TenantID string `json:"tenant_id"`
		}
		if !decodeAdmin(w, r, &input) || !h.require(w, r, input.TenantID, PermissionRead) {
			return
		}
		value, err := h.service.repository.GetTenant(r.Context(), input.TenantID)
		h.writeResult(w, http.StatusOK, value, err)
	case "/admin/apps/get":
		var input struct {
			TenantID string `json:"tenant_id"`
			AppID    string `json:"app_id"`
		}
		if !decodeAdmin(w, r, &input) || !h.require(w, r, input.TenantID, PermissionRead) {
			return
		}
		value, err := h.service.repository.GetAgentApp(r.Context(), input.TenantID, input.AppID)
		h.writeResult(w, http.StatusOK, value, err)
	case "/admin/revisions/get":
		var input struct {
			TenantID   string `json:"tenant_id"`
			RevisionID string `json:"revision_id"`
		}
		if !decodeAdmin(w, r, &input) || !h.require(w, r, input.TenantID, PermissionRead) {
			return
		}
		value, err := h.service.repository.GetRevision(r.Context(), input.TenantID, input.RevisionID)
		h.writeResult(w, http.StatusOK, value, err)
	case "/admin/channel-bindings/get":
		var input struct {
			TenantID  string `json:"tenant_id"`
			BindingID string `json:"binding_id"`
		}
		if !decodeAdmin(w, r, &input) || !h.require(w, r, input.TenantID, PermissionRead) {
			return
		}
		value, err := h.service.repository.GetChannelBinding(r.Context(), input.TenantID, input.BindingID)
		h.writeResult(w, http.StatusOK, value, err)
	case "/admin/backend-bindings/list":
		var input struct {
			TenantID string `json:"tenant_id"`
			AppID    string `json:"app_id"`
		}
		if !decodeAdmin(w, r, &input) || !h.require(w, r, input.TenantID, PermissionRead) {
			return
		}
		value, err := h.service.repository.ListBackendBindings(r.Context(), input.TenantID, input.AppID)
		h.writeResult(w, http.StatusOK, value, err)
	case "/admin/tenants":
		var input controlplane.Tenant
		if !decodeAdmin(w, r, &input) {
			return
		}
		if !h.require(w, r, input.ID, PermissionTenantCreate) {
			return
		}
		value, err := h.service.CreateTenant(r.Context(), input)
		h.writeResult(w, http.StatusCreated, value, err)
	case "/admin/tenants/policies":
		var input TenantPolicyInput
		if !decodeAdmin(w, r, &input) || !h.require(w, r, input.TenantID, PermissionWrite) {
			return
		}
		value, err := h.service.UpdateTenantPolicies(r.Context(), input)
		h.writeResult(w, http.StatusOK, value, err)
	case "/admin/apps":
		var input controlplane.AgentApp
		if !decodeAdmin(w, r, &input) {
			return
		}
		if !h.require(w, r, input.TenantID, PermissionWrite) {
			return
		}
		value, err := h.service.CreateAgentApp(r.Context(), input)
		h.writeResult(w, http.StatusCreated, value, err)
	case "/admin/revisions/validate":
		var input controlplane.AgentRevision
		if !decodeAdmin(w, r, &input) || !h.require(w, r, input.TenantID, PermissionWrite) {
			return
		}
		value, err := h.service.ValidateRevision(r.Context(), input)
		h.writeResult(w, http.StatusOK, value, err)
	case "/admin/revisions":
		var input controlplane.AgentRevision
		if !decodeAdmin(w, r, &input) {
			return
		}
		if !h.require(w, r, input.TenantID, PermissionWrite) {
			return
		}
		value, err := h.service.CreateRevision(r.Context(), input)
		h.writeResult(w, http.StatusCreated, value, err)
	case "/admin/revisions/publish":
		var input struct {
			TenantID        string `json:"tenant_id"`
			AppID           string `json:"app_id"`
			RevisionID      string `json:"revision_id"`
			ExpectedVersion int64  `json:"expected_version"`
		}
		if !decodeAdmin(w, r, &input) {
			return
		}
		if !h.require(w, r, input.TenantID, PermissionWrite) {
			return
		}
		value, err := h.service.PublishRevision(
			r.Context(), input.TenantID, input.AppID, input.RevisionID, input.ExpectedVersion,
		)
		h.writeResult(w, http.StatusOK, value, err)
	case "/admin/apps/rollout":
		var input struct {
			TenantID        string          `json:"tenant_id"`
			AppID           string          `json:"app_id"`
			RolloutPolicy   json.RawMessage `json:"rollout_policy"`
			ExpectedVersion int64           `json:"expected_version"`
		}
		if !decodeAdmin(w, r, &input) || !h.require(w, r, input.TenantID, PermissionWrite) {
			return
		}
		value, err := h.service.UpdateRolloutPolicy(
			r.Context(), input.TenantID, input.AppID,
			input.RolloutPolicy, input.ExpectedVersion,
		)
		h.writeResult(w, http.StatusOK, value, err)
	case "/admin/channel-bindings":
		var input controlplane.ChannelBinding
		if !decodeAdmin(w, r, &input) {
			return
		}
		if !h.require(w, r, input.TenantID, PermissionWrite) {
			return
		}
		value, err := h.service.CreateChannelBinding(r.Context(), input)
		h.writeResult(w, http.StatusCreated, value, err)
	case "/admin/channel-bindings/update":
		var input struct {
			TenantID        string          `json:"tenant_id"`
			BindingID       string          `json:"binding_id"`
			Config          json.RawMessage `json:"config"`
			Status          string          `json:"status"`
			ExpectedVersion int64           `json:"expected_version"`
		}
		if !decodeAdmin(w, r, &input) || !h.require(w, r, input.TenantID, PermissionWrite) {
			return
		}
		value, err := h.service.UpdateChannelBinding(
			r.Context(), input.TenantID, input.BindingID,
			input.Config, input.Status, input.ExpectedVersion,
		)
		h.writeResult(w, http.StatusOK, value, err)
	case "/admin/backend-bindings":
		var input controlplane.BackendBinding
		if !decodeAdmin(w, r, &input) {
			return
		}
		if !h.require(w, r, input.TenantID, PermissionWrite) {
			return
		}
		value, err := h.service.CreateBackendBinding(r.Context(), input)
		h.writeResult(w, http.StatusCreated, value, err)
	case "/admin/backend-migrations":
		var input controlplane.BackendMigration
		if !decodeAdmin(w, r, &input) {
			return
		}
		if !h.require(w, r, input.TenantID, PermissionWrite) {
			return
		}
		value, err := h.service.CreateBackendMigration(r.Context(), input)
		h.writeResult(w, http.StatusCreated, value, err)
	case "/admin/backend-migrations/transition":
		var input struct {
			TenantID        string          `json:"tenant_id"`
			MigrationID     string          `json:"migration_id"`
			NextState       string          `json:"next_state"`
			ExpectedVersion int64           `json:"expected_version"`
			Checkpoint      json.RawMessage `json:"checkpoint"`
			Verification    json.RawMessage `json:"verification"`
		}
		if !decodeAdmin(w, r, &input) {
			return
		}
		if !h.require(w, r, input.TenantID, PermissionOperate) {
			return
		}
		value, err := h.service.TransitionBackendMigration(
			r.Context(), input.TenantID, input.MigrationID, input.NextState,
			input.ExpectedVersion, input.Checkpoint, input.Verification,
		)
		h.writeResult(w, http.StatusOK, value, err)
	case "/admin/backend-migrations/get":
		var input struct {
			TenantID    string `json:"tenant_id"`
			MigrationID string `json:"migration_id"`
		}
		if !decodeAdmin(w, r, &input) {
			return
		}
		if !h.require(w, r, input.TenantID, PermissionRead) {
			return
		}
		value, err := h.service.repository.GetBackendMigration(
			r.Context(), input.TenantID, input.MigrationID,
		)
		h.writeResult(w, http.StatusOK, value, err)
	case "/admin/backend-migrations/backfill-memory":
		h.handleMemoryMigrationJob(w, r, background.JobMemoryBackfill)
	case "/admin/backend-migrations/backfill-knowledge":
		h.handleKnowledgeMigrationJob(w, r, background.JobKnowledgeBackfill)
	case "/admin/backend-migrations/verify-knowledge":
		h.handleKnowledgeMigrationJob(w, r, background.JobKnowledgeVerify)
	case "/admin/backend-migrations/knowledge-status":
		h.handleKnowledgeMigrationStatus(w, r)
	case "/admin/backend-migrations/verify-memory":
		h.handleMemoryMigrationJob(w, r, background.JobMemoryVerify)
	case "/admin/backend-migrations/backfill-session":
		h.handleSessionMigrationJob(w, r, background.JobSessionBackfill)
	case "/admin/backend-migrations/verify-session":
		h.handleSessionMigrationJob(w, r, background.JobSessionVerify)
	case "/admin/knowledge/documents":
		var input KnowledgeDocumentInput
		if !decodeAdmin(w, r, &input) {
			return
		}
		if !h.require(w, r, input.TenantID, PermissionWrite) {
			return
		}
		result, err := h.service.SubmitKnowledgeDocument(r.Context(), input)
		status := http.StatusOK
		if result.Queued {
			status = http.StatusAccepted
		}
		h.writeResult(w, status, result, err)
	case "/admin/knowledge/documents/delete":
		var input struct {
			TenantID    string `json:"tenant_id"`
			AppID       string `json:"app_id"`
			RevisionID  string `json:"revision_id"`
			DocumentID  string `json:"document_id"`
			OperationID string `json:"operation_id"`
		}
		if !decodeAdmin(w, r, &input) {
			return
		}
		if !h.require(w, r, input.TenantID, PermissionWrite) {
			return
		}
		result, err := h.service.SubmitKnowledgeDelete(
			r.Context(), input.TenantID, input.AppID, input.RevisionID,
			input.DocumentID, input.OperationID,
		)
		status := http.StatusOK
		if result.Queued {
			status = http.StatusAccepted
		}
		h.writeResult(w, status, result, err)
	case "/admin/jobs/get":
		var input struct {
			TenantID string `json:"tenant_id"`
			JobID    string `json:"job_id"`
		}
		if !decodeAdmin(w, r, &input) {
			return
		}
		if !h.require(w, r, input.TenantID, PermissionRead) {
			return
		}
		value, err := h.service.GetBackgroundJob(r.Context(), input.TenantID, input.JobID)
		h.writeResult(w, http.StatusOK, value, err)
	case "/admin/jobs/retry":
		var input struct {
			TenantID string `json:"tenant_id"`
			JobID    string `json:"job_id"`
		}
		if !decodeAdmin(w, r, &input) {
			return
		}
		if !h.require(w, r, input.TenantID, PermissionOperate) {
			return
		}
		err := h.service.RetryBackgroundJob(r.Context(), input.TenantID, input.JobID)
		h.writeResult(w, http.StatusOK, map[string]string{"status": "pending"}, err)
	case "/admin/audit/query":
		var input struct {
			TenantID string `json:"tenant_id"`
			Decision string `json:"decision"`
			TraceID  string `json:"trace_id"`
			Limit    int    `json:"limit"`
		}
		if !decodeAdmin(w, r, &input) || !h.require(w, r, input.TenantID, PermissionRead) {
			return
		}
		value, err := h.service.QueryAudit(r.Context(), audit.Query{
			TenantID: input.TenantID, Decision: input.Decision,
			TraceID: input.TraceID, Limit: input.Limit,
		})
		h.writeResult(w, http.StatusOK, value, err)
	default:
		adminJSON(w, http.StatusNotFound, map[string]string{"error": "Admin route not found"})
	}
}

func (h *Handler) handleSessionMigrationJob(
	w http.ResponseWriter,
	r *http.Request,
	jobType string,
) {
	var input struct {
		TenantID    string                                 `json:"tenant_id"`
		MigrationID string                                 `json:"migration_id"`
		OperationID string                                 `json:"operation_id"`
		Sessions    []platformstorage.SessionMigrationItem `json:"sessions"`
	}
	if !decodeAdmin(w, r, &input) {
		return
	}
	if !h.require(w, r, input.TenantID, PermissionOperate) {
		return
	}
	value, err := h.service.SubmitSessionMigrationJob(
		r.Context(), input.TenantID, input.MigrationID,
		jobType, input.OperationID, input.Sessions,
	)
	h.writeResult(w, http.StatusAccepted, value, err)
}

func (h *Handler) handleMemoryMigrationJob(
	w http.ResponseWriter,
	r *http.Request,
	jobType string,
) {
	var input struct {
		TenantID    string   `json:"tenant_id"`
		MigrationID string   `json:"migration_id"`
		OperationID string   `json:"operation_id"`
		UserIDs     []string `json:"user_ids"`
	}
	if !decodeAdmin(w, r, &input) {
		return
	}
	if !h.require(w, r, input.TenantID, PermissionOperate) {
		return
	}
	value, err := h.service.SubmitMemoryMigrationJob(
		r.Context(), input.TenantID, input.MigrationID,
		jobType, input.OperationID, input.UserIDs,
	)
	h.writeResult(w, http.StatusAccepted, value, err)
}

func (h *Handler) require(
	w http.ResponseWriter,
	r *http.Request,
	tenantID string,
	permission Permission,
) bool {
	principal, _ := r.Context().Value(principalContextKey{}).(Principal)
	if principal.Allows(permission, tenantID) {
		return true
	}
	adminJSON(w, http.StatusForbidden, map[string]string{"error": "forbidden"})
	return false
}

func (h *Handler) writeResult(w http.ResponseWriter, success int, value any, err error) {
	if err == nil {
		adminJSON(w, success, value)
		return
	}
	var validation *ValidationError
	if errors.As(err, &validation) {
		status := http.StatusBadRequest
		if errors.Is(err, secret.ErrForbidden) {
			status = http.StatusForbidden
		}
		adminJSON(w, status, map[string]any{"error": "Agent 配置检查未通过", "code": "configuration_invalid", "validation": validation.Report})
		return
	}
	var status int
	switch {
	case errors.Is(err, gateway.ErrRunMissing):
		status = http.StatusNotFound
	case errors.Is(err, approval.ErrForbidden):
		status = http.StatusForbidden
	case errors.Is(err, approval.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, approval.ErrConflict):
		status = http.StatusConflict
	case errors.Is(err, approval.ErrExpired):
		status = http.StatusGone
	case errors.Is(err, console.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, console.ErrConflict):
		status = http.StatusConflict
	case errors.Is(err, console.ErrUnavailable):
		adminJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "控制台存储尚未就绪，请检查数据库迁移与角色权限", "code": "console_unavailable"})
		return
	case errors.Is(err, secret.ErrForbidden):
		status = http.StatusForbidden
	case errors.Is(err, toolexec.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, toolexec.ErrConflict), errors.Is(err, toolexec.ErrOperationConflict):
		status = http.StatusConflict
	case errors.Is(err, ErrInvalid):
		status = http.StatusBadRequest
	case errors.Is(err, controlplane.ErrConflict):
		status = http.StatusConflict
	case errors.Is(err, controlplane.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, background.ErrJobNotFound):
		status = http.StatusNotFound
	case errors.Is(err, background.ErrJobConflict):
		status = http.StatusConflict
	default:
		status = http.StatusInternalServerError
	}
	if status >= 500 {
		log.Printf("Admin operation failed: %v", err)
		adminJSON(w, status, map[string]string{"error": "Admin operation failed"})
		return
	}
	adminJSON(w, status, map[string]string{"error": err.Error()})
}

func decodeAdmin(w http.ResponseWriter, r *http.Request, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		adminJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON body"})
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		adminJSON(w, http.StatusBadRequest, map[string]string{"error": "body must contain one JSON object"})
		return false
	}
	return true
}

func adminJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("encode Admin response: %v", err)
	}
}
