package admin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/knowledgebase"
	servicelog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/recovery"
	"github.com/liuzengh/trpc-agent-service/trpcservice/repository"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storagemigration"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/knowledge"
)

// Handler exposes the configuration administration HTTP API.
type Handler struct {
	service   *Service
	redactor  *servicelog.Redactor
	knowledge func(context.Context, string, string) (*knowledgebase.Service, error)
}

// HandlerOption customizes optional administration surfaces.
type HandlerOption func(*Handler)

// WithKnowledgeResolver enables authenticated tenant/app knowledge ingestion
// and search without accepting storage or secret settings from the client.
func WithKnowledgeResolver(resolver func(context.Context, string, string) (*knowledgebase.Service, error)) HandlerOption {
	return func(handler *Handler) { handler.knowledge = resolver }
}

// NewHandler creates an HTTP handler. Authentication must be applied by the
// caller, for example with Authenticator.Wrap.
func NewHandler(service *Service, options ...HandlerOption) (*Handler, error) {
	if service == nil {
		return nil, errors.New("admin: nil service")
	}
	handler := &Handler{service: service, redactor: servicelog.NewRedactor(nil, nil)}
	for _, option := range options {
		if option != nil {
			option(handler)
		}
	}
	return handler, nil
}

// ServeHTTP handles validate, publish, version list, current version, and
// rollback operations. The tenant scope always comes from the URL path, which
// the authentication layer has already authorized.
func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	parts := strings.Split(strings.Trim(request.URL.Path, "/"), "/")
	if len(parts) < 4 || parts[0] != "v1" || parts[1] != "tenants" {
		http.NotFound(writer, request)
		return
	}
	tenantID := parts[2]
	if parts[3] == "apps" {
		handler.serveKnowledge(writer, request, tenantID, parts)
		return
	}
	if parts[3] == "storage" {
		handler.serveStorage(writer, request, tenantID, parts)
		return
	}
	if parts[3] == "operations" {
		handler.serveRecovery(writer, request, tenantID, parts)
		return
	}
	if parts[3] != "configs" {
		http.NotFound(writer, request)
		return
	}
	action := "list"
	if len(parts) == 5 {
		action = parts[4]
	} else if len(parts) != 4 {
		http.NotFound(writer, request)
		return
	}
	switch action {
	case "validate":
		if request.Method != http.MethodPost {
			methodNotAllowed(writer)
			return
		}
		payload, ok := readPayload(writer, request)
		if !ok {
			return
		}
		file, err := handler.service.Validate(payload)
		if err != nil || file.Tenants[0].ID != tenantID {
			if err == nil {
				err = errors.New("tenant scope does not match payload")
			}
			handler.writeError(writer, http.StatusUnprocessableEntity, err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"valid": true})
	case "publish":
		if request.Method != http.MethodPost {
			methodNotAllowed(writer)
			return
		}
		expected, ok := versionParameter(writer, request, "expected_version")
		if !ok {
			return
		}
		payload, ok := readPayload(writer, request)
		if !ok {
			return
		}
		record, err := handler.service.Publish(request.Context(), tenantID, expected, payload)
		if err != nil {
			handler.writeServiceError(writer, err)
			return
		}
		writeJSON(writer, http.StatusCreated, metadata(record))
	case "rollback":
		if request.Method != http.MethodPost {
			methodNotAllowed(writer)
			return
		}
		expected, ok := versionParameter(writer, request, "expected_version")
		if !ok {
			return
		}
		target, ok := versionParameter(writer, request, "target_version")
		if !ok {
			return
		}
		record, err := handler.service.Rollback(request.Context(), tenantID, expected, target)
		if err != nil {
			handler.writeServiceError(writer, err)
			return
		}
		writeJSON(writer, http.StatusCreated, metadata(record))
	case "list":
		if request.Method != http.MethodGet {
			methodNotAllowed(writer)
			return
		}
		records, err := handler.service.Versions(request.Context(), tenantID)
		if err != nil {
			handler.writeServiceError(writer, err)
			return
		}
		result := make([]map[string]any, len(records))
		for i := range records {
			result[i] = metadata(records[i])
		}
		writeJSON(writer, http.StatusOK, result)
	case "current":
		if request.Method != http.MethodGet {
			methodNotAllowed(writer)
			return
		}
		record, err := handler.service.Current(request.Context(), tenantID)
		if err != nil {
			handler.writeServiceError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, metadata(record))
	default:
		http.NotFound(writer, request)
	}
}

func (handler *Handler) serveRecovery(writer http.ResponseWriter, request *http.Request, tenantID string, parts []string) {
	if len(parts) < 5 || len(parts) > 6 {
		http.NotFound(writer, request)
		return
	}
	kind := recovery.Kind(parts[4])
	if kind != recovery.KindInbox && kind != recovery.KindOutbox {
		http.NotFound(writer, request)
		return
	}
	if len(parts) == 5 {
		if request.Method != http.MethodGet {
			methodNotAllowed(writer)
			return
		}
		statuses, ok := recoveryStatuses(writer, request, kind)
		if !ok {
			return
		}
		limit := 50
		if raw := request.URL.Query().Get("limit"); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil || value < 1 || value > 200 {
				writeJSON(writer, http.StatusBadRequest, map[string]string{"error": "limit must be between 1 and 200"})
				return
			}
			limit = value
		}
		items, err := handler.service.RecoveryItems(request.Context(), tenantID, kind, statuses, limit)
		if err != nil {
			handler.writeRecoveryError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, items)
		return
	}
	if request.Method != http.MethodPost {
		methodNotAllowed(writer)
		return
	}
	var body struct {
		ID             string          `json:"id"`
		ExpectedStatus recovery.Status `json:"expected_status"`
		Decision       recovery.Status `json:"decision"`
		Reason         string          `json:"reason"`
		Acknowledge    bool            `json:"acknowledge_duplicate_risk"`
	}
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 1<<14))
	decoder.DisallowUnknownFields()
	decodeErr := decoder.Decode(&body)
	if decodeErr == nil {
		var trailing any
		if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
			decodeErr = errors.New("trailing JSON value")
		}
	}
	if decodeErr != nil || strings.TrimSpace(body.ID) == "" || len(body.ID) > 512 || len(strings.TrimSpace(body.Reason)) < 3 || len(body.Reason) > 512 {
		writeJSON(writer, http.StatusBadRequest, map[string]string{"error": "id and a reason between 3 and 512 characters are required"})
		return
	}
	var (
		item recovery.Item
		err  error
	)
	switch parts[5] {
	case "redrive":
		if body.Decision != "" || body.Acknowledge {
			writeJSON(writer, http.StatusBadRequest, map[string]string{"error": "decision fields are not valid for DLQ redrive"})
			return
		}
		item, err = handler.service.RedriveRecovery(request.Context(), tenantID, kind, body.ID, body.ExpectedStatus, strings.TrimSpace(body.Reason))
	case "resolve":
		if kind != recovery.KindOutbox || (body.Decision == recovery.StatusRetry && !body.Acknowledge) {
			writeJSON(writer, http.StatusBadRequest, map[string]string{"error": "retrying uncertain delivery requires acknowledge_duplicate_risk=true"})
			return
		}
		item, err = handler.service.ResolveUncertain(request.Context(), tenantID, body.ID, body.ExpectedStatus, body.Decision, strings.TrimSpace(body.Reason))
	default:
		http.NotFound(writer, request)
		return
	}
	if err != nil {
		handler.writeRecoveryError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, item)
}

func recoveryStatuses(writer http.ResponseWriter, request *http.Request, kind recovery.Kind) ([]recovery.Status, bool) {
	raw := strings.TrimSpace(request.URL.Query().Get("status"))
	if raw == "" {
		if kind == recovery.KindInbox {
			return []recovery.Status{recovery.StatusDLQ}, true
		}
		return []recovery.Status{recovery.StatusDLQ, recovery.StatusUncertain}, true
	}
	parts := strings.Split(raw, ",")
	statuses := make([]recovery.Status, 0, len(parts))
	seen := make(map[recovery.Status]bool)
	for _, part := range parts {
		status := recovery.Status(strings.TrimSpace(part))
		valid := status == recovery.StatusDLQ || (kind == recovery.KindOutbox && status == recovery.StatusUncertain)
		if !valid || seen[status] {
			writeJSON(writer, http.StatusBadRequest, map[string]string{"error": "unsupported or duplicate recovery status"})
			return nil, false
		}
		seen[status] = true
		statuses = append(statuses, status)
	}
	return statuses, true
}

func (handler *Handler) writeRecoveryError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, recovery.ErrConflict):
		handler.writeError(writer, http.StatusConflict, err)
	case errors.Is(err, recovery.ErrNotFound):
		handler.writeError(writer, http.StatusNotFound, err)
	case errors.Is(err, recovery.ErrInvalid):
		handler.writeError(writer, http.StatusUnprocessableEntity, err)
	default:
		writeJSON(writer, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
	}
}

func (handler *Handler) serveKnowledge(writer http.ResponseWriter, request *http.Request, tenantID string, parts []string) {
	if len(parts) != 7 || parts[4] == "" || parts[5] != "knowledge" || handler.knowledge == nil {
		http.NotFound(writer, request)
		return
	}
	service, err := handler.knowledge(request.Context(), tenantID, parts[4])
	if err != nil || service == nil {
		if err == nil {
			err = errors.New("admin: knowledge service is unavailable")
		}
		handler.writeError(writer, http.StatusServiceUnavailable, err)
		return
	}
	defer service.Close()
	switch parts[6] {
	case "documents":
		if request.Method != http.MethodPost {
			methodNotAllowed(writer)
			return
		}
		var body struct {
			DocumentID string         `json:"document_id"`
			Name       string         `json:"name"`
			Content    string         `json:"content"`
			Metadata   map[string]any `json:"metadata"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 2<<20))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&body); err != nil {
			writeJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
			return
		}
		ids, err := service.Ingest(request.Context(), knowledgebase.IngestRequest{DocumentID: body.DocumentID, Name: body.Name, Content: body.Content, Metadata: body.Metadata})
		if err != nil {
			handler.writeError(writer, http.StatusUnprocessableEntity, err)
			return
		}
		writeJSON(writer, http.StatusCreated, map[string]any{"document_id": body.DocumentID, "chunks": len(ids), "chunk_ids": ids})
	case "search":
		if request.Method != http.MethodPost {
			methodNotAllowed(writer)
			return
		}
		var body struct {
			Query      string  `json:"query"`
			MaxResults int     `json:"max_results"`
			MinScore   float64 `json:"min_score"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 1<<16))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&body); err != nil {
			writeJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
			return
		}
		result, err := service.Search(request.Context(), &knowledge.SearchRequest{Query: body.Query, MaxResults: body.MaxResults, MinScore: body.MinScore})
		if err != nil {
			handler.writeError(writer, http.StatusUnprocessableEntity, err)
			return
		}
		writeJSON(writer, http.StatusOK, result.Documents)
	default:
		http.NotFound(writer, request)
	}
}

func (handler *Handler) serveStorage(writer http.ResponseWriter, request *http.Request, tenantID string, parts []string) {
	if len(parts) < 5 || parts[4] != "migrations" {
		http.NotFound(writer, request)
		return
	}
	if len(parts) == 5 {
		switch request.Method {
		case http.MethodGet:
			jobs, err := handler.service.ListMigrations(request.Context(), tenantID)
			if err != nil {
				handler.writeMigrationError(writer, err)
				return
			}
			result := make([]map[string]any, len(jobs))
			for index := range jobs {
				result[index] = migrationMetadata(jobs[index])
			}
			writeJSON(writer, http.StatusOK, result)
		case http.MethodPost:
			var body struct {
				AppID    string                  `json:"app_id"`
				Domain   storagemigration.Domain `json:"domain"`
				Expected tenant.ConfigVersion    `json:"expected_version"`
			}
			decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 1<<16))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&body); err != nil {
				writeJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
				return
			}
			job, err := handler.service.PlanMigration(request.Context(), tenantID, body.AppID, body.Domain, body.Expected)
			if err != nil {
				handler.writeMigrationError(writer, err)
				return
			}
			writeJSON(writer, http.StatusCreated, migrationMetadata(job))
		default:
			methodNotAllowed(writer)
		}
		return
	}
	jobID := parts[5]
	if len(parts) == 6 {
		if request.Method != http.MethodGet {
			methodNotAllowed(writer)
			return
		}
		job, err := handler.service.GetMigration(request.Context(), tenantID, jobID)
		if err != nil {
			handler.writeMigrationError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, migrationMetadata(job))
		return
	}
	if len(parts) == 7 && parts[6] == "cancel" {
		if request.Method != http.MethodPost {
			methodNotAllowed(writer)
			return
		}
		if err := handler.service.CancelMigration(request.Context(), tenantID, jobID); err != nil {
			handler.writeMigrationError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"tenant_id": tenantID, "migration_id": jobID, "status": storagemigration.StatusCanceled})
		return
	}
	http.NotFound(writer, request)
}

func migrationMetadata(job storagemigration.Job) map[string]any {
	result := map[string]any{"tenant_id": job.TenantID, "migration_id": job.JobID, "app_id": job.AppID, "config_version": job.ConfigVersion, "domain": job.Domain, "status": job.Status, "source_rows": job.SourceRows, "copied_rows": job.CopiedRows, "attempts": job.Attempts, "created_by": job.CreatedBy, "created_at": job.CreatedAt, "updated_at": job.UpdatedAt}
	if job.LastErrorType != "" {
		result["error_type"] = job.LastErrorType
	}
	if job.CompletedAt != nil {
		result["completed_at"] = *job.CompletedAt
	}
	return result
}

func (handler *Handler) writeMigrationError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, repository.ErrVersionConflict), errors.Is(err, storagemigration.ErrConflict):
		handler.writeError(writer, http.StatusConflict, err)
	case errors.Is(err, storagemigration.ErrNotFound):
		handler.writeError(writer, http.StatusNotFound, err)
	case errors.Is(err, ErrInvalidConfig):
		handler.writeError(writer, http.StatusUnprocessableEntity, err)
	default:
		writeJSON(writer, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
	}
}

func readPayload(writer http.ResponseWriter, request *http.Request) ([]byte, bool) {
	payload, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, 1<<20))
	if err != nil {
		writeJSON(writer, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return nil, false
	}
	return payload, true
}

func versionParameter(writer http.ResponseWriter, request *http.Request, name string) (tenant.ConfigVersion, bool) {
	value, err := strconv.ParseUint(request.URL.Query().Get(name), 10, 64)
	if err != nil {
		writeJSON(writer, http.StatusBadRequest, map[string]string{"error": name + " must be an unsigned integer"})
		return 0, false
	}
	return tenant.ConfigVersion(value), true
}

// metadata is the only API projection of a config version. It intentionally
// excludes the payload so SecretRef material and internal config never leave
// the control plane through the Admin API.
func metadata(record repository.ConfigRecord) map[string]any {
	result := map[string]any{"tenant_id": record.TenantID, "version": record.Version, "content_hash": record.SHA256, "created_by": record.CreatedBy, "published_at": record.CreatedAt}
	if record.RolledBackFrom != nil {
		result["rollback_of"] = *record.RolledBackFrom
	}
	return result
}

func (handler *Handler) writeServiceError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, repository.ErrVersionConflict):
		handler.writeError(writer, http.StatusConflict, err)
	case errors.Is(err, repository.ErrNotFound):
		handler.writeError(writer, http.StatusNotFound, err)
	case errors.Is(err, ErrInvalidConfig):
		handler.writeError(writer, http.StatusUnprocessableEntity, err)
	default:
		writeJSON(writer, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
	}
}
func methodNotAllowed(writer http.ResponseWriter) {
	writeJSON(writer, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
}

// writeError redacts error text so resolved secrets can never leak through
// HTTP error responses.
func (handler *Handler) writeError(writer http.ResponseWriter, status int, err error) {
	writeJSON(writer, status, map[string]string{"error": handler.redactor.RedactString(err.Error())})
}

// writeError is used by the authentication layer before a Handler exists.
func writeError(writer http.ResponseWriter, status int, err error) {
	writeJSON(writer, status, map[string]string{"error": servicelog.NewRedactor(nil, nil).RedactString(err.Error())})
}
func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
