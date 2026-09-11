package web

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

type knowledgeMigrationRequest struct {
	TenantID        string `json:"tenant_id"`
	AppCode         string `json:"app_code"`
	MigrationID     string `json:"migration_id,omitempty"`
	Generation      uint64 `json:"generation,omitempty"`
	Action          string `json:"action,omitempty"`
	TargetProfileID string `json:"target_profile_id,omitempty"`
}

func (c *consoleAPI) knowledgeMigrations(writer http.ResponseWriter, request *http.Request) {
	if c.dependencies.KnowledgeMigrationStore == nil || c.dependencies.KnowledgeMigrator == nil {
		writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"error": "knowledge migration service is not configured"})
		return
	}
	switch request.Method {
	case http.MethodGet:
		c.listKnowledgeMigrations(writer, request)
	case http.MethodPost:
		c.startKnowledgeMigration(writer, request)
	case http.MethodPatch:
		c.mutateKnowledgeMigration(writer, request)
	default:
		methodNotAllowed(writer, "GET, POST, PATCH")
	}
}

func (c *consoleAPI) listKnowledgeMigrations(writer http.ResponseWriter, request *http.Request) {
	query := request.URL.Query()
	tenantID, appCode := resolveTenantParam(request), strings.TrimSpace(query.Get("app"))
	if tenantID == "" || appCode == "" {
		badRequest(writer, "tenant and app are required")
		return
	}
	if !requireTenantWrite(writer, request, tenantID) {
		return
	}
	limit := 20
	if raw := strings.TrimSpace(query.Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 100 {
			badRequest(writer, "limit must be between 1 and 100")
			return
		}
		limit = parsed
	}
	items, err := c.dependencies.KnowledgeMigrationStore.ListKnowledgeMigrations(request.Context(), tenantID, appCode, limit)
	if err != nil {
		serverError(writer, "list knowledge migrations", err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"migrations": items})
}

func (c *consoleAPI) startKnowledgeMigration(writer http.ResponseWriter, request *http.Request) {
	var body knowledgeMigrationRequest
	if !decodeLimitedJSON(writer, request, &body, 64<<10) {
		return
	}
	body.TenantID, body.AppCode = strings.TrimSpace(body.TenantID), strings.TrimSpace(body.AppCode)
	body.TargetProfileID = strings.TrimSpace(body.TargetProfileID)
	if body.TenantID == "" || body.AppCode == "" || body.TargetProfileID == "" {
		badRequest(writer, "tenant_id, app_code, and target_profile_id are required")
		return
	}
	if !requireTenantWrite(writer, request, body.TenantID) {
		return
	}
	status, err := c.dependencies.KnowledgeMigrator.StartKnowledgeMigration(request.Context(), body.TenantID, body.AppCode, body.TargetProfileID)
	if err != nil {
		if errors.Is(err, storage.ErrKnowledgeMigrationConflict) {
			writeJSON(writer, http.StatusConflict, map[string]any{"error": err.Error()})
			return
		}
		badRequest(writer, err.Error())
		return
	}
	writeJSON(writer, http.StatusCreated, status)
}

func (c *consoleAPI) mutateKnowledgeMigration(writer http.ResponseWriter, request *http.Request) {
	var body knowledgeMigrationRequest
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 64<<10))
	if err := decoder.Decode(&body); err != nil {
		badRequest(writer, "invalid knowledge migration mutation")
		return
	}
	body.TenantID, body.AppCode = strings.TrimSpace(body.TenantID), strings.TrimSpace(body.AppCode)
	body.MigrationID, body.Action = strings.TrimSpace(body.MigrationID), strings.ToLower(strings.TrimSpace(body.Action))
	if body.TenantID == "" || body.AppCode == "" || body.MigrationID == "" || body.Generation == 0 {
		badRequest(writer, "tenant_id, app_code, migration_id, and generation are required")
		return
	}
	if !requireTenantWrite(writer, request, body.TenantID) {
		return
	}
	var (
		status storage.KnowledgeMigrationStatus
		err    error
	)
	switch body.Action {
	case "advance":
		status, err = c.dependencies.KnowledgeMigrator.AdvanceKnowledgeMigration(request.Context(), body.TenantID, body.AppCode, body.MigrationID, body.Generation)
	case "rollback":
		status, err = c.dependencies.KnowledgeMigrator.RollbackKnowledgeMigration(request.Context(), body.TenantID, body.AppCode, body.MigrationID, body.Generation)
	default:
		badRequest(writer, "action must be advance or rollback")
		return
	}
	if err != nil {
		if errors.Is(err, storage.ErrKnowledgeMigrationConflict) {
			writeJSON(writer, http.StatusConflict, map[string]any{"error": err.Error()})
			return
		}
		badRequest(writer, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, status)
}

func (c *consoleAPI) knowledgeWritesAllowed(writer http.ResponseWriter, request *http.Request, tenantID, appCode string) bool {
	if c.dependencies.KnowledgeMigrationStore == nil {
		return true
	}
	status, active, err := c.dependencies.KnowledgeMigrationStore.ActiveKnowledgeMigration(request.Context(), tenantID, appCode)
	if err != nil {
		serverError(writer, "check knowledge migration", err)
		return false
	}
	if active {
		writeJSON(writer, http.StatusConflict, map[string]any{
			"error":        "knowledge writes are paused during backend migration",
			"migration_id": status.ID, "phase": status.Phase,
		})
		return false
	}
	return true
}
