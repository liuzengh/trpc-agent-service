package web

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

type sessionMigrationRequest struct {
	TenantID        string `json:"tenant_id"`
	AppCode         string `json:"app_code"`
	MigrationID     string `json:"migration_id,omitempty"`
	Generation      uint64 `json:"generation,omitempty"`
	Action          string `json:"action,omitempty"`
	TargetProfileID string `json:"target_profile_id,omitempty"`
}

func (c *consoleAPI) sessionMigrations(writer http.ResponseWriter, request *http.Request) {
	if c.dependencies.SessionMigrationStore == nil || c.dependencies.SessionMigrator == nil {
		writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"error": "Session migration service is not configured"})
		return
	}
	switch request.Method {
	case http.MethodGet:
		c.listSessionMigrations(writer, request)
	case http.MethodPost:
		c.startSessionMigration(writer, request)
	case http.MethodPatch:
		c.mutateSessionMigration(writer, request)
	default:
		methodNotAllowed(writer, "GET, POST, PATCH")
	}
}

func (c *consoleAPI) listSessionMigrations(writer http.ResponseWriter, request *http.Request) {
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
	items, err := c.dependencies.SessionMigrationStore.ListSessionMigrations(request.Context(), tenantID, appCode, limit)
	if err != nil {
		serverError(writer, "list Session migrations", err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"migrations": items})
}

func (c *consoleAPI) startSessionMigration(writer http.ResponseWriter, request *http.Request) {
	var body sessionMigrationRequest
	if err := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 64<<10)).Decode(&body); err != nil {
		badRequest(writer, "invalid Session migration request")
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
	status, err := c.dependencies.SessionMigrator.StartSessionMigration(request.Context(), body.TenantID, body.AppCode, body.TargetProfileID)
	if err != nil {
		if errors.Is(err, storage.ErrSessionMigrationConflict) {
			writeJSON(writer, http.StatusConflict, map[string]any{"error": err.Error()})
			return
		}
		badRequest(writer, err.Error())
		return
	}
	writeJSON(writer, http.StatusCreated, status)
}

func (c *consoleAPI) mutateSessionMigration(writer http.ResponseWriter, request *http.Request) {
	var body sessionMigrationRequest
	if err := json.NewDecoder(http.MaxBytesReader(writer, request.Body, 64<<10)).Decode(&body); err != nil {
		badRequest(writer, "invalid Session migration mutation")
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
		status storage.SessionMigrationStatus
		err    error
	)
	switch body.Action {
	case "advance":
		status, err = c.dependencies.SessionMigrator.AdvanceSessionMigration(request.Context(), body.TenantID, body.AppCode, body.MigrationID, body.Generation)
	case "rollback":
		status, err = c.dependencies.SessionMigrator.RollbackSessionMigration(request.Context(), body.TenantID, body.AppCode, body.MigrationID, body.Generation)
	default:
		badRequest(writer, "action must be advance or rollback")
		return
	}
	if err != nil {
		if errors.Is(err, storage.ErrSessionMigrationConflict) {
			writeJSON(writer, http.StatusConflict, map[string]any{"error": err.Error()})
			return
		}
		badRequest(writer, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, status)
}
