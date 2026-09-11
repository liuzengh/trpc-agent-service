package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	agentartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	agentmemory "trpc.group/trpc-go/trpc-agent-go/memory"
)

const (
	maxKnowledgeContentBytes = 1 << 20
	maxKnowledgeRequestBytes = 2 << 20
	maxKnowledgeUploadBytes  = int64(storage.MaxArtifactBytes) + 1<<20
)

type knowledgeRequest struct {
	TenantID   string            `json:"tenant_id"`
	AppCode    string            `json:"app_code"`
	DocumentID string            `json:"document_id"`
	Name       string            `json:"name,omitempty"`
	Content    string            `json:"content,omitempty"`
	SourceType string            `json:"source_type,omitempty"`
	SourceURL  string            `json:"source_url,omitempty"`
	Branch     string            `json:"branch,omitempty"`
	ChunkSize  int               `json:"chunk_size,omitempty"`
	Overlap    int               `json:"overlap,omitempty"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

type ArtifactServiceProvider interface {
	ArtifactService(context.Context, config.TenantConfig) (agentartifact.Service, error)
}

func (c *consoleAPI) memory(writer http.ResponseWriter, request *http.Request) {
	if c.dependencies.AgentMemory == nil {
		writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"error": "agent memory service is not configured"})
		return
	}
	if request.Method == http.MethodDelete {
		c.deleteMemory(writer, request)
		return
	}
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet+", "+http.MethodDelete)
		return
	}
	query := request.URL.Query()
	tenantID, appCode := resolveTenantParam(request), strings.TrimSpace(query.Get("app"))
	if !requireTenantRead(writer, request, tenantID) {
		return
	}
	application, ok := c.activeApplication(writer, request, tenantID, appCode)
	if !ok {
		return
	}
	reader, err := c.agentMemoryReader(request.Context(), tenantID, application.AppCode)
	if err != nil {
		if errors.Is(err, storage.ErrMemoryDirectAccessUnsupported) {
			writeJSON(writer, http.StatusOK, map[string]any{"memories": []any{}, "managed_externally": true})
			return
		}
		badRequest(writer, err.Error())
		return
	}
	user, _ := sessionUser(request)
	subjectID := user.PlatformUserID
	if requestedSubject := strings.TrimSpace(query.Get("subject")); requestedSubject != "" && requestedSubject != subjectID {
		writeJSON(writer, http.StatusForbidden, map[string]any{"error": "forbidden: personal preferences are only visible to their owner"})
		return
	}
	limit := 20
	if raw := query.Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 100 {
			badRequest(writer, "limit must be between 1 and 100")
			return
		}
		limit = parsed
	}
	kind, err := parseMemoryKind(query.Get("kind"))
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	timeAfter, err := parseOptionalTime(query.Get("time_after"))
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	timeBefore, err := parseOptionalTime(query.Get("time_before"))
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	orderByEventTime := query.Get("order") == "event_time"
	userKey := agentmemory.UserKey{AppName: application.AppName(), UserID: subjectID}
	var memories []*agentmemory.Entry
	if textQuery := strings.TrimSpace(query.Get("query")); textQuery != "" {
		memories, err = reader.SearchMemories(
			request.Context(), userKey, textQuery,
			agentmemory.WithSearchOptions(agentmemory.SearchOptions{
				Query:            textQuery,
				Kind:             kind,
				TimeAfter:        timeAfter,
				TimeBefore:       timeBefore,
				MaxResults:       limit,
				OrderByEventTime: orderByEventTime,
				Deduplicate:      true,
			}),
		)
	} else {
		memories, err = reader.ReadMemories(request.Context(), userKey, limit)
		if err == nil {
			memories = filterMemoryEntries(memories, kind, timeAfter, timeBefore, orderByEventTime)
		}
	}
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"memories": memories})
}

func (c *consoleAPI) deleteMemory(writer http.ResponseWriter, request *http.Request) {
	query := request.URL.Query()
	tenantID, appCode := resolveTenantParam(request), strings.TrimSpace(query.Get("app"))
	memoryID := strings.TrimSpace(query.Get("memory_id"))
	if memoryID == "" {
		badRequest(writer, "memory_id is required")
		return
	}
	if !requireTenantRead(writer, request, tenantID) {
		return
	}
	application, ok := c.activeApplication(writer, request, tenantID, appCode)
	if !ok {
		return
	}
	service, err := c.agentMemoryService(request.Context(), tenantID, application.AppCode)
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	user, _ := sessionUser(request)
	if err := service.DeleteMemory(request.Context(), agentmemory.Key{
		AppName: application.AppName(), UserID: user.PlatformUserID, MemoryID: memoryID,
	}); err != nil {
		badRequest(writer, err.Error())
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func parseMemoryKind(raw string) (agentmemory.Kind, error) {
	switch strings.TrimSpace(raw) {
	case "", "all":
		return "", nil
	case string(agentmemory.KindFact):
		return agentmemory.KindFact, nil
	case string(agentmemory.KindEpisode):
		return agentmemory.KindEpisode, nil
	default:
		return "", fmt.Errorf("kind must be fact or episode")
	}
}

func parseOptionalTime(raw string) (*time.Time, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	if parsed, err := time.Parse(time.RFC3339, raw); err == nil {
		return &parsed, nil
	}
	if parsed, err := time.Parse("2006-01-02", raw); err == nil {
		return &parsed, nil
	}
	return nil, fmt.Errorf("time must be RFC3339 or YYYY-MM-DD")
}

func filterMemoryEntries(entries []*agentmemory.Entry, kind agentmemory.Kind, timeAfter, timeBefore *time.Time, orderByEventTime bool) []*agentmemory.Entry {
	filtered := make([]*agentmemory.Entry, 0, len(entries))
	for _, entry := range entries {
		if entry == nil || entry.Memory == nil {
			continue
		}
		if kind != "" && entry.Memory.Kind != kind {
			continue
		}
		if timeAfter != nil && (entry.Memory.EventTime == nil || entry.Memory.EventTime.Before(*timeAfter)) {
			continue
		}
		if timeBefore != nil && (entry.Memory.EventTime == nil || entry.Memory.EventTime.After(*timeBefore)) {
			continue
		}
		filtered = append(filtered, entry)
	}
	if !orderByEventTime {
		return filtered
	}
	sort.SliceStable(filtered, func(i, j int) bool {
		left, right := filtered[i].Memory.EventTime, filtered[j].Memory.EventTime
		switch {
		case left == nil && right == nil:
			return false
		case left == nil:
			return false
		case right == nil:
			return true
		default:
			return left.Before(*right)
		}
	})
	return filtered
}

func (c *consoleAPI) knowledge(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodPost:
		if strings.HasPrefix(strings.ToLower(request.Header.Get("Content-Type")), "multipart/form-data") {
			c.enqueueKnowledgeUpload(writer, request)
			return
		}
		var body knowledgeRequest
		if !decodeLimitedJSON(writer, request, &body, maxKnowledgeRequestBytes) {
			return
		}
		tenantID := strings.TrimSpace(body.TenantID)
		if !requireTenantWrite(writer, request, tenantID) {
			return
		}
		appCode := strings.TrimSpace(body.AppCode)
		documentID := strings.TrimSpace(body.DocumentID)
		if appCode == "" || documentID == "" {
			badRequest(writer, "app_code and document_id are required")
			return
		}
		application, ok := c.activeApplication(writer, request, tenantID, appCode)
		if !ok {
			return
		}
		if !c.knowledgeWritesAllowed(writer, request, tenantID, appCode) {
			return
		}
		sourceType := strings.ToLower(strings.TrimSpace(body.SourceType))
		if c.dependencies.KnowledgeIngest == nil {
			writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"error": "knowledge ingest queue is not configured"})
			return
		}
		metadata := make(map[string]string, len(body.Metadata)+3)
		for key, value := range body.Metadata {
			metadata[key] = value
		}
		var filename, contentType string
		var data []byte
		if sourceType != "" {
			if sourceType != "url" && sourceType != "repo" {
				badRequest(writer, "source_type must be 'url' or 'repo'")
				return
			}
			sourceURL := strings.TrimSpace(body.SourceURL)
			if sourceURL == "" {
				badRequest(writer, "source_url is required for source ingestion")
				return
			}
			if c.dependencies.KnowledgeSourcePolicy == nil {
				writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"error": "knowledge source policy is not configured"})
				return
			}
			if err := c.dependencies.KnowledgeSourcePolicy.ValidateRemoteSource(request.Context(), sourceType, sourceURL); err != nil {
				badRequest(writer, err.Error())
				return
			}
			metadata["source_type"] = sourceType
			metadata["source_url"] = sourceURL
			if body.Branch != "" {
				metadata["branch"] = strings.TrimSpace(body.Branch)
			}
			filename = sourceURL
			contentType = "application/x-" + sourceType
			data = []byte(sourceURL)
		} else {
			if strings.TrimSpace(body.Content) == "" {
				badRequest(writer, "content is required for text ingestion")
				return
			}
			if len([]byte(body.Content)) > maxKnowledgeContentBytes {
				writeJSON(writer, http.StatusRequestEntityTooLarge, map[string]any{"error": "knowledge content exceeds 1 MiB"})
				return
			}
			metadata["source_type"] = "text"
			filename = documentID + ".txt"
			contentType = "text/plain; charset=utf-8"
			data = []byte(body.Content)
		}
		name := strings.TrimSpace(body.Name)
		if name == "" {
			name = documentID
			if sourceType != "" {
				name = strings.TrimSpace(body.SourceURL)
			}
		}
		backend, err := c.resolveKnowledgeBackend(request.Context(), application)
		if err != nil {
			badRequest(writer, err.Error())
			return
		}
		jobID, err := c.dependencies.KnowledgeIngest.EnqueueKnowledgeIngest(request.Context(), storage.KnowledgeIngestRequest{
			TenantID: tenantID, AppCode: appCode, DocumentID: documentID, Name: name,
			Filename: filename, ContentType: contentType, Data: data,
			ChunkSize: body.ChunkSize, Overlap: body.Overlap, Metadata: metadata,
			ProfileID: application.Storage.Knowledge.ProfileID,
			Backend:   normalizedKnowledgeBackend(backend),
		})
		if err != nil {
			if errors.Is(err, storage.ErrKnowledgeIngestActive) {
				writeJSON(writer, http.StatusConflict, map[string]any{"error": "document is already being indexed"})
				return
			}
			badRequest(writer, err.Error())
			return
		}
		writeJSON(writer, http.StatusAccepted, map[string]any{"job_id": jobID, "status": "indexing"})
	case http.MethodDelete:
		query := request.URL.Query()
		tenantID, appCode := resolveTenantParam(request), strings.TrimSpace(query.Get("app"))
		documentID := strings.TrimSpace(query.Get("document_id"))
		if documentID == "" {
			badRequest(writer, "document_id is required")
			return
		}
		if !requireTenantWrite(writer, request, tenantID) {
			return
		}
		knowledge, application, ok := c.knowledgeAdmin(writer, request, tenantID, appCode)
		if !ok {
			return
		}
		if !c.knowledgeWritesAllowed(writer, request, tenantID, appCode) {
			return
		}
		if c.dependencies.KnowledgeIngest != nil {
			if err := c.dependencies.KnowledgeIngest.CancelKnowledgeIngest(request.Context(), tenantID, appCode, documentID); err != nil {
				serverError(writer, "cancel knowledge indexing", err)
				return
			}
		}
		if err := knowledge.DeleteKnowledgeDocument(request.Context(), application, documentID); err != nil {
			badRequest(writer, err.Error())
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(writer, "POST, DELETE")
	}
}

func (c *consoleAPI) enqueueKnowledgeUpload(writer http.ResponseWriter, request *http.Request) {
	if c.dependencies.KnowledgeIngest == nil {
		writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"error": "knowledge ingest queue is not configured"})
		return
	}
	request.Body = http.MaxBytesReader(writer, request.Body, maxKnowledgeUploadBytes)
	if err := request.ParseMultipartForm(storage.MaxArtifactBytes); err != nil {
		badRequest(writer, fmt.Sprintf("parse knowledge upload: %v", err))
		return
	}
	defer request.MultipartForm.RemoveAll()
	tenantID := strings.TrimSpace(request.FormValue("tenant_id"))
	appCode := strings.TrimSpace(request.FormValue("app_code"))
	documentID := strings.TrimSpace(request.FormValue("document_id"))
	if !requireTenantWrite(writer, request, tenantID) {
		return
	}
	if documentID == "" {
		badRequest(writer, "document_id is required")
		return
	}
	_, application, ok := c.knowledgeAdmin(writer, request, tenantID, appCode)
	if !ok {
		return
	}
	if !c.knowledgeWritesAllowed(writer, request, tenantID, appCode) {
		return
	}
	file, header, err := request.FormFile("file")
	if err != nil {
		badRequest(writer, "knowledge file is required")
		return
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, int64(storage.MaxArtifactBytes)+1))
	if err != nil {
		badRequest(writer, fmt.Sprintf("read knowledge file: %v", err))
		return
	}
	if len(data) == 0 {
		badRequest(writer, "knowledge file is empty")
		return
	}
	if int64(len(data)) > storage.MaxArtifactBytes {
		writeJSON(writer, http.StatusRequestEntityTooLarge, map[string]any{"error": "knowledge file exceeds 16 MiB"})
		return
	}
	chunkSize, err := optionalPositiveInt(request.FormValue("chunk_size"))
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	overlap, err := optionalPositiveInt(request.FormValue("overlap"))
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	metadata := map[string]string{}
	if raw := strings.TrimSpace(request.FormValue("metadata")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &metadata); err != nil {
			badRequest(writer, "metadata must be a JSON object of strings")
			return
		}
	}
	metadata["source_type"] = "file"
	name := strings.TrimSpace(request.FormValue("name"))
	if name == "" {
		name = header.Filename
	}
	backend, err := c.resolveKnowledgeBackend(request.Context(), application)
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	jobID, err := c.dependencies.KnowledgeIngest.EnqueueKnowledgeIngest(request.Context(), storage.KnowledgeIngestRequest{
		TenantID: tenantID, AppCode: appCode, DocumentID: documentID, Name: name,
		Filename: header.Filename, ContentType: header.Header.Get("Content-Type"), Data: data,
		ChunkSize: chunkSize, Overlap: overlap, Metadata: metadata,
		ProfileID: application.Storage.Knowledge.ProfileID,
		Backend:   normalizedKnowledgeBackend(backend),
	})
	if err != nil {
		if errors.Is(err, storage.ErrKnowledgeIngestActive) {
			writeJSON(writer, http.StatusConflict, map[string]any{"error": "document is already being indexed"})
			return
		}
		badRequest(writer, err.Error())
		return
	}
	writeJSON(writer, http.StatusAccepted, map[string]any{"job_id": jobID, "status": "indexing"})
}

func normalizedKnowledgeBackend(backend config.BackendConfig) config.BackendConfig {
	backend.Driver = strings.ToLower(strings.TrimSpace(backend.Driver))
	if backend.Driver == "" {
		backend.Driver = "pgvector"
	}
	backend.ConnectionRef = strings.TrimSpace(backend.ConnectionRef)
	return backend
}

func (c *consoleAPI) resolveKnowledgeBackend(ctx context.Context, application config.TenantConfig) (config.BackendConfig, error) {
	if c.dependencies.BackendProfiles == nil {
		return config.BackendConfig{}, errors.New("backend profile store is not configured")
	}
	backend, err := c.dependencies.BackendProfiles.ResolveTenantBackend(
		ctx, application.TenantID, storage.BackendDomainKnowledge, application.Storage.Knowledge.ProfileID,
	)
	if err != nil {
		return config.BackendConfig{}, fmt.Errorf("resolve Knowledge backend profile: %w", err)
	}
	return backend, nil
}

func optionalPositiveInt(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("chunk_size and overlap must be non-negative integers")
	}
	return value, nil
}

func (c *consoleAPI) knowledgeDocuments(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	query := request.URL.Query()
	tenantID, appCode := resolveTenantParam(request), strings.TrimSpace(query.Get("app"))
	if !requireTenantRead(writer, request, tenantID) {
		return
	}
	knowledge, _, ok := c.knowledgeAdmin(writer, request, tenantID, appCode)
	if !ok {
		return
	}
	documents, err := knowledge.ListKnowledgeDocuments(request.Context(), tenantID, appCode)
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	response := make([]map[string]any, 0, len(documents))
	for _, doc := range documents {
		response = append(response, map[string]any{
			"tenant_id":    doc.TenantID,
			"app_code":     doc.AppCode,
			"document_id":  doc.DocumentID,
			"name":         doc.Name,
			"status":       doc.Status,
			"total_chunks": doc.TotalChunks,
			"metadata":     doc.Metadata,
			"updated_at":   doc.UpdatedAt,
		})
	}
	writeJSON(writer, http.StatusOK, map[string]any{"documents": response})
}

func (c *consoleAPI) artifacts(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	if c.dependencies.ArtifactServices == nil {
		writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"error": "agent artifact service is not configured"})
		return
	}
	query := request.URL.Query()
	tenantID, appCode := resolveTenantParam(request), strings.TrimSpace(query.Get("app"))
	if !requireTenantRead(writer, request, tenantID) {
		return
	}
	if c.dependencies.State == nil {
		serverError(writer, "resolve artifact session", errors.New("state store is not configured"))
		return
	}
	application, ok := c.activeApplication(writer, request, tenantID, appCode)
	if !ok {
		return
	}
	service, err := c.dependencies.ArtifactServices.ArtifactService(request.Context(), application)
	if err != nil {
		serverError(writer, "resolve agent artifact service", err)
		return
	}
	info := agentartifact.SessionInfo{AppName: application.AppName(), SessionID: strings.TrimSpace(query.Get("session"))}
	if info.SessionID == "" {
		badRequest(writer, "session is required")
		return
	}
	entry, err := c.dependencies.State.GetSession(request.Context(), tenantID, info.SessionID)
	if err != nil {
		if errors.Is(err, storage.ErrSessionNotFound) {
			notFound(writer, "session does not exist")
			return
		}
		serverError(writer, "resolve artifact session", err)
		return
	}
	user, _ := sessionUser(request)
	contentAudit := !canReadSession(user, entry)
	if contentAudit && !canAuditConversationContent(user, tenantID) {
		writeJSON(writer, http.StatusForbidden, map[string]any{"error": "forbidden: session artifacts are not visible"})
		return
	}
	if entry.AppCode != appCode {
		badRequest(writer, "session does not belong to the selected application")
		return
	}
	info.UserID = entry.SubjectID
	filename := strings.TrimSpace(query.Get("filename"))
	if filename == "" {
		keys, err := service.ListArtifactKeys(request.Context(), info)
		if err != nil {
			badRequest(writer, err.Error())
			return
		}
		visible := keys[:0]
		for _, key := range keys {
			if !strings.HasPrefix(key, "input/") {
				visible = append(visible, key)
			}
		}
		if contentAudit {
			if err := c.recordConversationArtifactRead(request, user, entry); err != nil {
				serverError(writer, "record conversation artifact audit", err)
				return
			}
		}
		writeJSON(writer, http.StatusOK, map[string]any{"artifacts": visible})
		return
	}
	versions, err := service.ListVersions(request.Context(), info, filename)
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	if len(versions) == 0 {
		notFound(writer, "artifact does not exist")
		return
	}
	targetVersion := versions[len(versions)-1]
	if raw := strings.TrimSpace(query.Get("version")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			badRequest(writer, "artifact version must be a non-negative integer")
			return
		}
		targetVersion = parsed
	}
	artifact, err := service.LoadArtifact(request.Context(), info, filename, &targetVersion)
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	if artifact == nil {
		notFound(writer, "artifact version does not exist")
		return
	}
	if contentAudit {
		if err := c.recordConversationArtifactRead(request, user, entry); err != nil {
			serverError(writer, "record conversation artifact audit", err)
			return
		}
	}
	writer.Header().Set("Content-Type", artifact.MimeType)
	writer.Header().Set("X-Artifact-Version", strconv.Itoa(targetVersion))
	writer.Header().Set("X-Content-Type-Options", "nosniff")
	writer.Header().Set("Cache-Control", "private, no-store")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(artifact.Data)
}

func (c *consoleAPI) knowledgeAdmin(writer http.ResponseWriter, request *http.Request, tenantID, appCode string) (KnowledgeAdmin, config.TenantConfig, bool) {
	if c.dependencies.Knowledge == nil {
		writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"error": "knowledge store is not configured"})
		return nil, config.TenantConfig{}, false
	}
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(appCode) == "" {
		badRequest(writer, "tenant and app are required")
		return nil, config.TenantConfig{}, false
	}
	application, ok := c.activeApplication(writer, request, tenantID, appCode)
	if !ok {
		return nil, config.TenantConfig{}, false
	}
	return c.dependencies.Knowledge, application, true
}

func metadataString(metadata map[string]any, key string) string {
	value, _ := metadata[key].(string)
	return value
}

func metadataInt(metadata map[string]any, key string) int {
	switch value := metadata[key].(type) {
	case int:
		return value
	case int64:
		return int(value)
	case float64:
		return int(value)
	case json.Number:
		parsed, _ := strconv.Atoi(value.String())
		return parsed
	default:
		return 0
	}
}

func (c *consoleAPI) activeApplication(writer http.ResponseWriter, request *http.Request, tenantID, appCode string) (config.TenantConfig, bool) {
	tenantID, appCode = strings.TrimSpace(tenantID), strings.TrimSpace(appCode)
	if tenantID == "" || appCode == "" {
		badRequest(writer, "tenant and app are required")
		return config.TenantConfig{}, false
	}
	snapshot, err := c.dependencies.Configurations.GetActive(request.Context(), tenantID, appCode)
	if errors.Is(err, tenant.ErrNotFound) {
		notFound(writer, "tenant application does not exist")
		return config.TenantConfig{}, false
	}
	if err != nil {
		serverError(writer, "resolve tenant application", err)
		return config.TenantConfig{}, false
	}
	return snapshot.Config, true
}

func decodeLimitedJSON(writer http.ResponseWriter, request *http.Request, target any, limit int64) bool {
	request.Body = http.MaxBytesReader(writer, request.Body, limit)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(writer, http.StatusRequestEntityTooLarge, map[string]any{"error": fmt.Sprintf("request body exceeds %d bytes", limit)})
			return false
		}
		badRequest(writer, fmt.Sprintf("decode request body: %v", err))
		return false
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		badRequest(writer, "request body must contain exactly one JSON value")
		return false
	}
	return true
}
