package admin

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/background"
	"github.com/liuzengh/trpc-agent-service/trpcservice/controlplane"
)

type Handler struct {
	service *Service
	token   string
}

func NewHandler(service *Service, token string) (*Handler, error) {
	if service == nil || len(token) < 24 {
		return nil, errors.New("Admin service and strong bearer token are required")
	}
	return &Handler{service: service, token: token}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !h.authorized(r) {
		w.Header().Set("WWW-Authenticate", "Bearer")
		adminJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		adminJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	switch r.URL.Path {
	case "/admin/tenants":
		var input controlplane.Tenant
		if !decodeAdmin(w, r, &input) {
			return
		}
		value, err := h.service.CreateTenant(r.Context(), input)
		h.writeResult(w, http.StatusCreated, value, err)
	case "/admin/apps":
		var input controlplane.AgentApp
		if !decodeAdmin(w, r, &input) {
			return
		}
		value, err := h.service.CreateAgentApp(r.Context(), input)
		h.writeResult(w, http.StatusCreated, value, err)
	case "/admin/revisions":
		var input controlplane.AgentRevision
		if !decodeAdmin(w, r, &input) {
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
		value, err := h.service.PublishRevision(
			r.Context(), input.TenantID, input.AppID, input.RevisionID, input.ExpectedVersion,
		)
		h.writeResult(w, http.StatusOK, value, err)
	case "/admin/channel-bindings":
		var input controlplane.ChannelBinding
		if !decodeAdmin(w, r, &input) {
			return
		}
		value, err := h.service.CreateChannelBinding(r.Context(), input)
		h.writeResult(w, http.StatusCreated, value, err)
	case "/admin/backend-bindings":
		var input controlplane.BackendBinding
		if !decodeAdmin(w, r, &input) {
			return
		}
		value, err := h.service.CreateBackendBinding(r.Context(), input)
		h.writeResult(w, http.StatusCreated, value, err)
	case "/admin/knowledge/documents":
		var input KnowledgeDocumentInput
		if !decodeAdmin(w, r, &input) {
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
		err := h.service.RetryBackgroundJob(r.Context(), input.TenantID, input.JobID)
		h.writeResult(w, http.StatusOK, map[string]string{"status": "pending"}, err)
	default:
		adminJSON(w, http.StatusNotFound, map[string]string{"error": "Admin route not found"})
	}
}

func (h *Handler) authorized(r *http.Request) bool {
	header := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	provided := strings.TrimSpace(strings.TrimPrefix(header, prefix))
	return subtle.ConstantTimeCompare([]byte(provided), []byte(h.token)) == 1
}

func (h *Handler) writeResult(w http.ResponseWriter, success int, value any, err error) {
	if err == nil {
		adminJSON(w, success, value)
		return
	}
	var status int
	switch {
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
