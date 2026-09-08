package admin

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/background"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformstorage "github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/toolexec"
)

type Handler struct {
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
	principal, authorized := authenticate(h.principals, r.Header.Get("Authorization"))
	if !authorized {
		w.Header().Set("WWW-Authenticate", "Bearer")
		adminJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	r = r.WithContext(contextWithPrincipal(r.Context(), principal))
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		adminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	switch r.URL.Path {
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
	var status int
	switch {
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
