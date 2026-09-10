package web

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	agentsession "trpc.group/trpc-go/trpc-agent-go/session"
)

func (c *consoleAPI) archiveSession(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return
	}
	var body struct {
		TenantID   string `json:"tenant_id"`
		SessionKey string `json:"session_key"`
	}
	if err := decodeJSONBody(request, &body); err != nil {
		badRequest(writer, err.Error())
		return
	}
	if !requireTenantRead(writer, request, body.TenantID) {
		return
	}
	if c.dependencies.SessionManager == nil {
		serverError(writer, "archive session", errors.New("session manager is not configured"))
		return
	}
	if c.dependencies.State == nil {
		serverError(writer, "archive session", errors.New("state store is not configured"))
		return
	}
	entry, err := c.dependencies.State.GetSession(request.Context(), body.TenantID, body.SessionKey)
	if err != nil {
		if errors.Is(err, storage.ErrSessionNotFound) {
			notFound(writer, "session does not exist")
			return
		}
		serverError(writer, "read session before archive", err)
		return
	}
	user, _ := sessionUser(request)
	if !canWriteTenant(user, body.TenantID) && !canReadSession(user, entry) {
		writeJSON(writer, http.StatusForbidden, map[string]any{"error": "forbidden: session is not visible"})
		return
	}
	if c.dependencies.AgentSessions != nil {
		service, err := c.agentSessionService(request.Context(), body.TenantID, entry.AppCode)
		if err != nil {
			serverError(writer, "resolve agent Session backend before archive", err)
			return
		}
		frameworkSession, err := service.GetSession(request.Context(), agentsession.Key{
			AppName: body.TenantID + "/" + entry.AppCode, UserID: entry.SubjectID, SessionID: entry.SessionKey,
		})
		if err != nil {
			serverError(writer, "read agent session before archive", err)
			return
		}
		if err := service.EnqueueSummaryJob(request.Context(), frameworkSession, agentsession.SummaryFilterKeyAllContents, true); err != nil {
			serverError(writer, "enqueue session summary", err)
			return
		}
	}
	if err := c.dependencies.SessionManager.ArchiveSession(request.Context(), body.TenantID, body.SessionKey); err != nil {
		if errors.Is(err, storage.ErrSessionNotFound) {
			notFound(writer, "session does not exist")
			return
		}
		serverError(writer, "archive session", err)
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func (c *consoleAPI) channelStatuses(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	tenantID := resolveTenantParam(request)
	if tenantID == "" {
		badRequest(writer, "tenant query parameter or X-Active-Tenant header is required")
		return
	}
	if !requireTenantWrite(writer, request, tenantID) {
		return
	}
	if c.dependencies.ChannelStatuses == nil {
		writeJSON(writer, http.StatusOK, map[string]any{"channels": []channels.BindingStatus{}})
		return
	}
	statuses, err := c.dependencies.ChannelStatuses.ListBindingStatuses(request.Context(), tenantID, strings.TrimSpace(request.URL.Query().Get("app")))
	if err != nil {
		serverError(writer, "list channel statuses", err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"channels": statuses})
}

func (c *consoleAPI) listClaims(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	tenantID := resolveTenantParam(request)
	if tenantID == "" {
		badRequest(writer, "tenant query parameter or X-Active-Tenant header is required")
		return
	}
	if !requireTenantWrite(writer, request, tenantID) {
		return
	}
	if c.dependencies.Claims == nil {
		serverError(writer, "list claims", errors.New("claim lister is not configured"))
		return
	}
	appCode := strings.TrimSpace(request.URL.Query().Get("app"))
	claims, err := c.dependencies.Claims.ListClaims(request.Context(), tenantID, appCode, consoleListLimit)
	if err != nil {
		serverError(writer, "list claims", err)
		return
	}
	items := make([]executionListItem, 0, len(claims))
	for _, claim := range claims {
		items = append(items, executionListItem{Channel: claim.Channel, BindingID: claim.BindingID, MessageID: claim.MessageID, Status: claim.Status, TraceID: claim.TraceID, UpdatedAt: claim.UpdatedAt, AppCode: claim.AppCode})
	}
	if c.dependencies.State != nil && len(claims) > 0 {
		refs := make([]storage.ExecutionTraceRef, len(claims))
		for index, claim := range claims {
			refs[index] = storage.ExecutionTraceRef{Channel: claim.Channel, BindingID: claim.BindingID, MessageID: claim.MessageID}
		}
		records, err := c.dependencies.State.ListExecutionTraces(request.Context(), tenantID, refs)
		if err != nil {
			serverError(writer, "list execution traces", err)
			return
		}
		byKey := make(map[string]storage.ExecutionTraceRecord, len(records))
		for _, record := range records {
			byKey[record.Channel+"\x00"+record.BindingID+"\x00"+record.MessageID] = record
		}
		for index, claim := range claims {
			record, ok := byKey[claim.Channel+"\x00"+claim.BindingID+"\x00"+claim.MessageID]
			if !ok {
				continue
			}
			if items[index].AppCode == "" {
				items[index].AppCode = record.AppCode
			}
			items[index].StartedAt = maybeTime(record.Trace.StartedAt)
			items[index].EndedAt = maybeTime(record.Trace.EndedAt)
			items[index].Failed = executionTraceFailed(record.Trace)
		}
	}
	writeJSON(writer, http.StatusOK, map[string]any{"claims": items})
}

type executionListItem struct {
	Channel   string     `json:"channel"`
	BindingID string     `json:"binding_id"`
	MessageID string     `json:"message_id"`
	Status    string     `json:"status"`
	TraceID   string     `json:"trace_id"`
	UpdatedAt time.Time  `json:"updated_at"`
	AppCode   string     `json:"app_code,omitempty"`
	StartedAt *time.Time `json:"started_at,omitempty"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
	Failed    bool       `json:"failed,omitempty"`
}

func maybeTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	copied := value
	return &copied
}

func executionTraceFailed(trace storage.AgentExecutionTrace) bool {
	if trace.Status == "failed" {
		return true
	}
	for _, step := range trace.Steps {
		if step.Failed {
			return true
		}
	}
	return false
}

func (c *consoleAPI) listAudit(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	tenantID := resolveTenantParam(request)
	if tenantID == "" {
		badRequest(writer, "tenant query parameter or X-Active-Tenant header is required")
		return
	}
	if !requireTenantWrite(writer, request, tenantID) {
		return
	}
	if c.dependencies.State == nil {
		serverError(writer, "list audit", errors.New("state store is not configured"))
		return
	}
	events, err := c.dependencies.State.ListAudit(request.Context(), tenantID, request.URL.Query().Get("trace"))
	if err != nil {
		serverError(writer, "list audit", err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"events": events})
}

func (c *consoleAPI) listOutbox(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	tenantID := resolveTenantParam(request)
	if tenantID == "" {
		badRequest(writer, "tenant query parameter or X-Active-Tenant header is required")
		return
	}
	if !requireTenantWrite(writer, request, tenantID) {
		return
	}
	if c.dependencies.State == nil {
		serverError(writer, "list outbox", errors.New("state store is not configured"))
		return
	}
	events, err := c.dependencies.State.ListPendingOutbox(request.Context(), tenantID, consoleListLimit)
	if err != nil {
		serverError(writer, "list outbox", err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"events": events})
}

func (c *consoleAPI) executionTrace(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	query := request.URL.Query()
	tenantID, channel, bindingID, eventID, sessionKey := query.Get("tenant"), query.Get("channel"), query.Get("binding_id"), query.Get("event_id"), query.Get("session_key")
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(channel) == "" || strings.TrimSpace(bindingID) == "" || strings.TrimSpace(eventID) == "" {
		badRequest(writer, "tenant, channel, binding_id, and event_id query parameters are required")
		return
	}
	if !requireTenantWrite(writer, request, tenantID) {
		return
	}
	trace := map[string]any{"event_id": eventID, "attempts": 0, "tool_executions": []any{}}
	if c.dependencies.State != nil {
		agentTrace, err := c.dependencies.State.GetExecutionTrace(request.Context(), tenantID, channel, bindingID, eventID)
		switch {
		case err == nil:
			trace["agent_trace"] = agentTrace.Trace
			trace["trace_id"] = agentTrace.TraceID
			if agentTrace.Trace.Status != "" {
				trace["status"] = agentTrace.Trace.Status
			}
		case errors.Is(err, storage.ErrExecutionTraceNotFound):
		default:
			serverError(writer, "get agent execution trace", err)
			return
		}
	}
	if c.dependencies.Attempts != nil && strings.TrimSpace(sessionKey) != "" {
		attempts, err := c.dependencies.Attempts.ListAttempts(request.Context(), tenantID, sessionKey, eventID)
		if err != nil {
			serverError(writer, "list attempts", err)
			return
		}
		trace["attempts"] = attempts
	}
	if c.dependencies.ToolExecutions != nil {
		toolExecutions, err := c.dependencies.ToolExecutions.ListExecutions(request.Context(), tenantID, eventID)
		if err != nil {
			serverError(writer, "list tool executions", err)
			return
		}
		trace["tool_executions"] = toolExecutions
	}
	if c.dependencies.Claims != nil {
		claims, err := c.dependencies.Claims.ListClaims(request.Context(), tenantID, "", consoleListLimit)
		if err != nil {
			serverError(writer, "list claims", err)
			return
		}
		for _, claim := range claims {
			if claim.Channel != channel || claim.BindingID != bindingID || claim.MessageID != eventID {
				continue
			}
			trace["claim"] = claim
			if _, ok := trace["trace_id"]; !ok && strings.TrimSpace(claim.TraceID) != "" {
				trace["trace_id"] = claim.TraceID
			}
			if _, ok := trace["status"]; !ok {
				if claim.Status == "completed" {
					trace["status"] = "completed"
				} else {
					trace["status"] = "incomplete"
				}
			}
			break
		}
	}
	var outboxReplies []storage.OutboxEvent
	if c.dependencies.Replies != nil {
		if outboxEvent, err := c.dependencies.Replies.FindOutboxByRequestID(request.Context(), tenantID, eventID); err == nil {
			outboxReplies = []storage.OutboxEvent{outboxEvent}
		}
	}
	if len(outboxReplies) == 0 && strings.TrimSpace(sessionKey) != "" {
		if c.dependencies.State == nil {
			serverError(writer, "list execution outbox", errors.New("state store is not configured"))
			return
		}
		pending, err := c.dependencies.State.ListPendingOutbox(request.Context(), tenantID, consoleListLimit)
		if err == nil {
			for _, pendingEvent := range pending {
				if pendingEvent.AggregateKey == sessionKey {
					outboxReplies = append(outboxReplies, pendingEvent)
				}
			}
		}
	}
	trace["outbox"] = outboxReplies
	writeJSON(writer, http.StatusOK, trace)
}
