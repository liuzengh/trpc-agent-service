package platform

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"sort"
	"strings"
	"time"
)

type migrationResult struct {
	ID                string `json:"id"`
	TenantID          string `json:"-"`
	Status            string `json:"status"`
	DryRun            bool   `json:"dry_run"`
	Sessions          int    `json:"sessions"`
	ProcessedSessions int    `json:"processed_sessions"`
	SourceCount       int    `json:"source_count"`
	DestinationCount  int    `json:"destination_count"`
	Checksum          string `json:"checksum,omitempty"`
	Resumed           bool   `json:"resumed"`
	Matched           bool   `json:"matched"`
	Message           string `json:"message,omitempty"`
}

func (h *AdminHandler) handleDataResource(w http.ResponseWriter, r *http.Request, parts []string) {
	tenant, ok := trustedTenant(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "identity_required", "development identity is required")
		return
	}
	if h.isClosing() {
		writeError(w, http.StatusServiceUnavailable, "service_closing", "service is closing")
		return
	}
	if len(parts) > 0 && parts[0] == "migrations" {
		h.handleMigration(w, r, tenant, parts[1:])
		return
	}
	store, releaseStore, err := h.acquireStore(r.Context(), tenant.TenantID)
	if err != nil {
		writeStorageError(w, err)
		return
	}
	defer releaseStore()
	if len(parts) == 0 {
		writeError(w, http.StatusNotFound, "not_found", "resource was not found")
		return
	}
	switch parts[0] {
	case "storage":
		h.handleStorage(w, r, tenant, store, parts[1:])
	case "sessions":
		h.handleSessionData(w, r, store, tenant, parts[1:])
	case "memory":
		h.handleMemoryData(w, r, store, tenant, parts[1:])
	case "artifacts":
		h.handleArtifactData(w, r, store, tenant)
	case "knowledge":
		h.handleKnowledgeData(w, r, store, tenant)
	default:
		writeError(w, http.StatusNotFound, "not_found", "resource was not found")
	}
}

func (h *AdminHandler) handleArtifactData(w http.ResponseWriter, r *http.Request, store DataStore, tenant TenantContext) {
	artifacts, ok := store.(ArtifactStore)
	if !ok {
		writeError(w, http.StatusNotImplemented, "artifact_backend_unsupported", "Artifact backend is not configured")
		return
	}
	if r.Method == http.MethodGet && r.URL.Query().Get("content_id") != "" {
		loader, ok := store.(interface {
			LoadArtifactContent(context.Context, string, string) ([]byte, error)
		})
		if !ok {
			writeError(w, http.StatusNotImplemented, "artifact_backend_unsupported", "Artifact content is not configured")
			return
		}
		content, err := loader.LoadArtifactContent(r.Context(), tenant.TenantID, r.URL.Query().Get("content_id"))
		if err != nil {
			writeStorageError(w, err)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write(content)
		return
	}
	switch r.Method {
	case http.MethodGet:
		items, err := artifacts.ListArtifacts(r.Context(), tenant.TenantID, strings.TrimSpace(r.URL.Query().Get("session_id")))
		if err != nil {
			writeStorageError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	case http.MethodPost:
		if !canOperate(tenant.Role) {
			writeError(w, http.StatusForbidden, "forbidden", "operator role is required")
			return
		}
		var item Artifact
		if err := decodeStrict(r, &item); err != nil || item.SessionID == "" || item.Name == "" || item.ContentRef == "" {
			writeError(w, http.StatusBadRequest, "invalid_artifact", "Artifact metadata is invalid")
			return
		}
		item.TenantID = tenant.TenantID
		created, err := artifacts.PutArtifact(r.Context(), item)
		if err != nil {
			writeStorageError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, created)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET or POST")
	}
}

func (h *AdminHandler) handleKnowledgeData(w http.ResponseWriter, r *http.Request, store DataStore, tenant TenantContext) {
	knowledge, ok := store.(KnowledgeStore)
	if !ok {
		writeError(w, http.StatusNotImplemented, "knowledge_backend_unsupported", "Knowledge backend is not configured")
		return
	}
	switch r.Method {
	case http.MethodGet:
		items, err := knowledge.ListKnowledge(r.Context(), tenant.TenantID, strings.TrimSpace(r.URL.Query().Get("app_id")))
		if err != nil {
			writeStorageError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	case http.MethodPost:
		if !canOperate(tenant.Role) {
			writeError(w, http.StatusForbidden, "forbidden", "operator role is required")
			return
		}
		var item KnowledgeRecord
		if err := decodeStrict(r, &item); err != nil || item.AgentAppID == "" || item.Source == "" || item.Content == "" {
			writeError(w, http.StatusBadRequest, "invalid_knowledge", "Knowledge record is invalid")
			return
		}
		if _, found, err := h.platform.app(r.Context(), tenant.TenantID, item.AgentAppID); err != nil || !found {
			if writeControlPlaneError(w, err) {
				return
			}
			writeError(w, http.StatusNotFound, "agent_app_not_found", "Agent App was not found")
			return
		}
		item.TenantID = tenant.TenantID
		created, err := knowledge.PutKnowledge(r.Context(), item)
		if err != nil {
			writeStorageError(w, err)
			return
		}
		writeJSON(w, http.StatusCreated, created)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET or POST")
	}
}

func (h *AdminHandler) handleStorage(w http.ResponseWriter, r *http.Request, tenant TenantContext, store DataStore, parts []string) {
	if len(parts) != 1 || parts[0] != "backend" {
		writeError(w, http.StatusNotFound, "not_found", "resource was not found")
		return
	}
	if r.Method == http.MethodGet {
		h.mu.Lock()
		available := make([]string, 0, len(h.backendCatalog))
		for id := range h.backendCatalog {
			available = append(available, id)
		}
		h.mu.Unlock()
		sort.Strings(available)
		healthCtx, cancel := boundedStorageContext(r.Context())
		health := store.Health(healthCtx)
		cancel()
		selectedID := health.Backend
		selections, err := h.platform.loadBackendSelections(r.Context())
		if err != nil {
			writeControlPlaneError(w, err)
			return
		}
		if selections[tenant.TenantID].ProfileID != "" {
			selectedID = selections[tenant.TenantID].ProfileID
		}
		writeJSON(w, http.StatusOK, map[string]any{"backend": selectedID, "health": health, "available_backends": available})
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET or POST")
		return
	}
	if !canMutate(tenant.Role) {
		writeError(w, http.StatusForbidden, "forbidden", "tenant administrator role is required")
		return
	}
	var req struct {
		Backend string `json:"backend"`
	}
	if err := decodeStrict(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_backend", "backend is required")
		return
	}
	h.mu.Lock()
	selection, configured := h.backendCatalog[req.Backend]
	h.mu.Unlock()
	if !configured {
		writeError(w, http.StatusBadRequest, "backend_not_configured", "backend is not configured by the server")
		return
	}
	store, err := newBackendStore(selection)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_backend", "backend is not available")
		return
	}
	if err := h.selectBackend(r.Context(), tenant.TenantID, selection, store); err != nil {
		if closer, ok := store.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
		if writeControlPlaneError(w, err) {
			return
		}
		writeError(w, http.StatusInternalServerError, "backend_selection_not_persisted", "backend selection could not be persisted")
		return
	}
	healthCtx, cancel := boundedStorageContext(r.Context())
	health := store.Health(healthCtx)
	cancel()
	writeJSON(w, http.StatusOK, map[string]any{"backend": req.Backend, "health": health})
}

func (h *AdminHandler) handleSessionData(w http.ResponseWriter, r *http.Request, store DataStore, tenant TenantContext, parts []string) {
	if len(parts) < 1 {
		writeError(w, http.StatusBadRequest, "session_required", "session id is required")
		return
	}
	sessionID := parts[0]
	if len(parts) == 1 && r.Method == http.MethodGet {
		stateCtx, cancel := boundedStorageContext(r.Context())
		state, err := store.GetSessionState(stateCtx, tenant.TenantID, sessionID)
		cancel()
		if errors.Is(err, ErrNotFound) {
			writeError(w, http.StatusNotFound, "session_not_found", "session was not found")
			return
		}
		if err != nil {
			writeStorageError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, state)
		return
	}
	if len(parts) == 2 && parts[1] == "events" && r.Method == http.MethodGet {
		eventsCtx, cancel := boundedStorageContext(r.Context())
		events, err := store.ListSessionEvents(eventsCtx, tenant.TenantID, sessionID, 0)
		cancel()
		if err != nil {
			writeStorageError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": events})
		return
	}
	writeError(w, http.StatusNotFound, "not_found", "resource was not found")
}

func (h *AdminHandler) handleMemoryData(w http.ResponseWriter, r *http.Request, store DataStore, tenant TenantContext, parts []string) {
	if len(parts) != 1 {
		writeError(w, http.StatusBadRequest, "session_required", "session id is required")
		return
	}
	sessionID := parts[0]
	switch r.Method {
	case http.MethodGet:
		memoryCtx, cancel := boundedStorageContext(r.Context())
		items, err := store.ListMemory(memoryCtx, tenant.TenantID, sessionID)
		cancel()
		if err != nil {
			writeStorageError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
	case http.MethodPost:
		if !canOperate(tenant.Role) {
			writeError(w, http.StatusForbidden, "forbidden", "operator role is required")
			return
		}
		var req struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}
		if err := decodeStrict(r, &req); err != nil || req.Key == "" {
			writeError(w, http.StatusBadRequest, "invalid_memory", "key and value are required")
			return
		}
		item := MemoryRecord{TenantID: tenant.TenantID, SessionID: sessionID, Key: req.Key, Value: req.Value}
		putCtx, cancelPut := boundedStorageContext(r.Context())
		putErr := store.PutMemory(putCtx, item)
		cancelPut()
		if putErr != nil {
			writeStorageError(w, putErr)
			return
		}
		listCtx, cancelList := boundedStorageContext(r.Context())
		items, err := store.ListMemory(listCtx, tenant.TenantID, sessionID)
		cancelList()
		if err != nil {
			writeStorageError(w, err)
			return
		}
		created := MemoryRecord{TenantID: tenant.TenantID, SessionID: sessionID, Key: req.Key}
		for _, persisted := range items {
			if persisted.Key == req.Key {
				created = persisted
				break
			}
		}
		if created.ID == "" {
			writeError(w, http.StatusServiceUnavailable, "storage_error", "memory record was not persisted")
			return
		}
		writeJSON(w, http.StatusCreated, created)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be GET or POST")
	}
}

func (h *AdminHandler) handleMigration(w http.ResponseWriter, r *http.Request, tenant TenantContext, parts []string) {
	if len(parts) == 1 && r.Method == http.MethodGet {
		h.mu.Lock()
		result, ok := h.migrations[parts[0]]
		h.mu.Unlock()
		if !ok {
			selections, err := h.platform.loadBackendSelections(r.Context())
			if err != nil {
				writeControlPlaneError(w, err)
				return
			}
			if selection := selections[tenant.TenantID]; selection.MigrationID == parts[0] {
				result, ok = migrationResult{ID: parts[0], TenantID: tenant.TenantID, Status: "recovery_required"}, true
			}
		}
		if !ok || result.TenantID != tenant.TenantID {
			writeError(w, http.StatusNotFound, "migration_not_found", "migration was not found")
			return
		}
		writeJSON(w, http.StatusOK, result)
		return
	}
	if len(parts) == 1 && r.Method == http.MethodDelete {
		if !canMutate(tenant.Role) {
			writeError(w, http.StatusForbidden, "forbidden", "tenant administrator role is required")
			return
		}
		h.mu.Lock()
		running := h.migrationRunning
		sourceAddress := h.migrationSourceAddress
		h.mu.Unlock()
		if running {
			writeError(w, http.StatusConflict, "migration_in_progress", "the local migration is still running")
			return
		}
		control, ok := h.platform.(backendMigrationControl)
		if !ok {
			writeError(w, http.StatusNotImplemented, "migration_recovery_unsupported", "migration control is unavailable")
			return
		}
		selections, err := h.platform.loadBackendSelections(r.Context())
		if err != nil {
			writeControlPlaneError(w, err)
			return
		}
		selection := selections[tenant.TenantID]
		if selection.MigrationID == "" || selection.MigrationID != parts[0] {
			writeError(w, http.StatusNotFound, "migration_not_found", "migration was not found")
			return
		}
		if selection.Backend != "redis" || selection.Address != sourceAddress {
			writeError(w, http.StatusConflict, "migration_source_mismatch", "the locked migration source does not match server configuration")
			return
		}
		source := NewRedisStore(sourceAddress)
		defer source.Close()
		if err := control.finishBackendMigration(r.Context(), tenant.TenantID, parts[0], nil); err != nil {
			writeControlPlaneError(w, err)
			return
		}
		if err := source.ResumeTenantWrites(r.Context(), tenant.TenantID); err != nil {
			// Restore admission blocking when the source could not be unlocked.
			restoreCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = control.beginBackendMigration(restoreCtx, tenant.TenantID, parts[0], backendSelection{Backend: "redis", Address: sourceAddress})
			cancel()
			writeStorageError(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if len(parts) != 0 {
		writeError(w, http.StatusNotFound, "not_found", "resource was not found")
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method must be POST")
		return
	}
	if !canMutate(tenant.Role) {
		writeError(w, http.StatusForbidden, "forbidden", "tenant administrator role is required")
		return
	}
	var req struct {
		DryRun    bool `json:"dry_run"`
		BatchSize int  `json:"batch_size"`
		Cutover   bool `json:"cutover"`
	}
	if err := decodeStrict(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_migration", "dry_run and batch_size must be valid")
		return
	}
	h.mu.Lock()
	if h.closing {
		h.mu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "service_closing", "service is closing")
		return
	}
	sourceAddress, destinationPath, checkpointPath := h.migrationSourceAddress, h.migrationDestinationPath, h.migrationCheckpointPath
	h.mu.Unlock()
	if sourceAddress == "" || destinationPath == "" {
		writeError(w, http.StatusServiceUnavailable, "migration_not_configured", "migration endpoints are configured by the server")
		return
	}
	idBytes := make([]byte, 8)
	_, _ = rand.Read(idBytes)
	id := "migration-" + hex.EncodeToString(idBytes)
	result := migrationResult{ID: id, TenantID: tenant.TenantID, Status: "running", DryRun: req.DryRun}
	h.mu.Lock()
	if h.closing {
		h.mu.Unlock()
		writeError(w, http.StatusServiceUnavailable, "service_closing", "service is closing")
		return
	}
	if h.migrationRunning {
		h.mu.Unlock()
		writeError(w, http.StatusConflict, "migration_in_progress", "another migration is already running")
		return
	}
	var migrationControl backendMigrationControl
	if req.Cutover && !req.DryRun {
		var ok bool
		migrationControl, ok = h.platform.(backendMigrationControl)
		if !ok {
			h.mu.Unlock()
			writeError(w, http.StatusNotImplemented, "migration_cutover_unsupported", "migration control is unavailable")
			return
		}
		if err := migrationControl.beginBackendMigration(r.Context(), tenant.TenantID, id, backendSelection{Backend: "redis", Address: sourceAddress}); err != nil {
			h.mu.Unlock()
			writeError(w, http.StatusConflict, "migration_source_mismatch", "select the plain Redis source before cutover")
			return
		}
	}
	h.migrationRunning = true
	h.migrations[id] = result
	h.migrationWG.Add(1)
	h.mu.Unlock()
	go func() {
		defer h.migrationWG.Done()
		cutoverDone := false
		defer func() {
			if migrationControl != nil && !cutoverDone {
				c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				_ = migrationControl.finishBackendMigration(c, tenant.TenantID, id, nil)
			}
		}()
		defer func() { h.mu.Lock(); h.migrationRunning = false; h.mu.Unlock() }()
		ctx, cancel := context.WithTimeout(h.migrationCtx, 10*time.Minute)
		defer cancel()
		source := NewRedisStore(sourceAddress)
		defer source.Close()
		destination, err := NewSQLiteStore(destinationPath)
		if err == nil {
			defer destination.Close()
		}
		var report MigrationReport
		if err == nil {
			var cutover func(context.Context) error
			if req.Cutover && !req.DryRun {
				cutover = func(cutoverCtx context.Context) error {
					selection := backendSelection{Backend: "sqlite", Address: destinationPath}
					if err := migrationControl.finishBackendMigration(cutoverCtx, tenant.TenantID, id, &selection); err != nil {
						return err
					}
					cutoverDone = true
					return nil
				}
			}
			report, err = MigrateRedisToSQL(ctx, source, destination, MigrationOptions{Cutover: cutover, TenantID: tenant.TenantID, DryRun: req.DryRun, BatchSize: req.BatchSize, CheckpointPath: checkpointPath, Progress: func(progress MigrationReport) {
				h.mu.Lock()
				h.migrations[id] = migrationResult{ID: id, TenantID: tenant.TenantID, Status: "running", DryRun: req.DryRun, Sessions: progress.Sessions, ProcessedSessions: progress.ProcessedSessions, SourceCount: progress.SourceCount, DestinationCount: progress.DestinationCount, Checksum: progress.Checksum, Resumed: progress.Resumed}
				h.mu.Unlock()
			}})
		}
		updated := migrationResult{ID: id, TenantID: tenant.TenantID, Status: report.Status, DryRun: req.DryRun, Sessions: report.Sessions, ProcessedSessions: report.ProcessedSessions, SourceCount: report.SourceCount, DestinationCount: report.DestinationCount, Checksum: report.Checksum, Resumed: report.Resumed, Matched: report.Matched}
		if err != nil {
			updated.Status = "failed"
			updated.Message = "migration failed"
		}
		h.mu.Lock()
		h.migrations[id] = updated
		h.mu.Unlock()
	}()
	writeJSON(w, http.StatusAccepted, result)
}

func isDataPath(path string) bool {
	return strings.HasPrefix(path, "/api/v1/admin/storage") || strings.HasPrefix(path, "/api/v1/admin/sessions") || strings.HasPrefix(path, "/api/v1/admin/memory") || strings.HasPrefix(path, "/api/v1/admin/artifacts") || strings.HasPrefix(path, "/api/v1/admin/knowledge") || strings.HasPrefix(path, "/api/v1/admin/migrations")
}
