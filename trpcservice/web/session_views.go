package web

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	agentsession "trpc.group/trpc-go/trpc-agent-go/session"
)

type sessionView struct {
	storage.Session
	MessageCount uint64 `json:"message_count"`
}

type personalSessionView struct {
	sessionView
	Summary string `json:"summary,omitempty"`
	Preview string `json:"preview,omitempty"`
}

const maxSessionListPreviewEvents = 1000

func (c *consoleAPI) listMySessions(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	tenantID := resolveTenantParam(request)
	if tenantID == "" {
		badRequest(writer, "tenant is required")
		return
	}
	if !requireTenantRead(writer, request, tenantID) {
		return
	}
	user, _ := sessionUser(request)
	c.writeSessionViews(writer, request, tenantID, true, func(entry storage.Session) bool {
		return entry.OwnerPlatformUserID == user.PlatformUserID && sessionScope(entry) == "direct"
	})
}

func (c *consoleAPI) listTenantSessions(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet)
		return
	}
	tenantID := resolveTenantParam(request)
	if tenantID == "" {
		badRequest(writer, "tenant is required")
		return
	}
	if !requireTenantRead(writer, request, tenantID) {
		return
	}
	user, _ := sessionUser(request)
	if !canWriteTenant(user, tenantID) {
		writeJSON(writer, http.StatusForbidden, map[string]any{"error": "forbidden: tenant sessions require tenant administrator role"})
		return
	}
	c.writeSessionViews(writer, request, tenantID, false, func(storage.Session) bool { return true })
}

func (c *consoleAPI) writeSessionViews(writer http.ResponseWriter, request *http.Request, tenantID string, includePersonalContent bool, baseFilter func(storage.Session) bool) {
	if c.dependencies.Sessions == nil {
		serverError(writer, "list sessions", errors.New("session lister is not configured"))
		return
	}
	entries, err := c.dependencies.Sessions.ListSessions(request.Context(), tenantID, consoleListLimit)
	if err != nil {
		serverError(writer, "list sessions", err)
		return
	}
	query := request.URL.Query()
	appCode := strings.TrimSpace(query.Get("app"))
	channel := strings.TrimSpace(query.Get("channel"))
	status := strings.TrimSpace(query.Get("status"))
	scope := strings.TrimSpace(query.Get("scope"))
	owner := strings.TrimSpace(query.Get("owner_platform_user_id"))
	unlinked := strings.EqualFold(strings.TrimSpace(query.Get("identity")), "unlinked")
	limit := defaultSessionListLimit
	if raw := strings.TrimSpace(query.Get("limit")); raw != "" {
		parsed, parseErr := strconv.Atoi(raw)
		if parseErr != nil || parsed <= 0 || parsed > consoleListLimit {
			badRequest(writer, fmt.Sprintf("limit must be between 1 and %d", consoleListLimit))
			return
		}
		limit = parsed
	}
	metadata := make([]sessionView, 0, min(limit, len(entries)))
	personal := make([]personalSessionView, 0, min(limit, len(entries)))
	sessionServices := make(map[string]agentsession.Service)
	for _, entry := range entries {
		if !baseFilter(entry) || !matchSessionViewFilters(entry, appCode, channel, status, scope, owner, unlinked) {
			continue
		}
		view := sessionView{Session: entry, MessageCount: entry.Revision}
		// ResolveSession creates the platform route before the first Worker run.
		// A request can stop after that point (validation failure, client abort,
		// publish failure), leaving a revision-0 shell with no framework Session.
		// It is not conversation history yet and must not make the whole personal
		// session list fail.
		if includePersonalContent && entry.Revision == 0 {
			continue
		}
		if includePersonalContent && c.dependencies.AgentSessions != nil && entry.AppCode != "" && entry.SubjectID != "" && entry.SessionKey != "" {
			personalView := personalSessionView{sessionView: view}
			service := sessionServices[entry.AppCode]
			if service == nil {
				var serviceErr error
				service, serviceErr = c.agentSessionService(request.Context(), tenantID, entry.AppCode)
				if serviceErr != nil {
					serverError(writer, "resolve agent Session backend", serviceErr)
					return
				}
				sessionServices[entry.AppCode] = service
			}
			preview, summary, getErr := loadPersonalSessionListContent(request.Context(), service, agentsession.Key{
				AppName: tenantID + "/" + entry.AppCode, UserID: entry.SubjectID, SessionID: entry.SessionKey,
			})
			if getErr != nil {
				serverError(writer, "read agent Session for personal session list", getErr)
				return
			}
			personalView.Preview = preview
			personalView.Summary = summary
			personal = append(personal, personalView)
		} else if includePersonalContent {
			personal = append(personal, personalSessionView{sessionView: view})
		} else {
			metadata = append(metadata, view)
		}
		if (includePersonalContent && len(personal) >= limit) || (!includePersonalContent && len(metadata) >= limit) {
			break
		}
	}
	if includePersonalContent {
		writeJSON(writer, http.StatusOK, map[string]any{"sessions": personal})
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"sessions": metadata})
}

func loadPersonalSessionListContent(ctx context.Context, service agentsession.Service, key agentsession.Key) (string, string, error) {
	batch := minSessionEventScanBatch
	preview := ""
	summary := ""
	for offset := 0; offset < maxSessionListPreviewEvents; {
		limit := min(batch, maxSessionListPreviewEvents-offset)
		frameworkSession, err := service.GetSession(ctx, key, agentsession.WithGetSessionEventPage(offset, limit))
		if errors.Is(err, agentsession.ErrEventPageUnsupported) {
			frameworkSession, err = service.GetSession(ctx, key)
			if err != nil {
				return "", "", err
			}
			if frameworkSession == nil {
				return "", "", fmt.Errorf("session %q is unavailable", key.SessionID)
			}
			if value, ok := service.GetSessionSummaryText(ctx, frameworkSession); ok {
				summary = value
			}
			return chatPreview(projectChatMessages(frameworkSession)), summary, nil
		}
		if err != nil {
			return "", "", err
		}
		if frameworkSession == nil {
			return "", "", fmt.Errorf("session %q is unavailable", key.SessionID)
		}
		if offset == 0 {
			if value, ok := service.GetSessionSummaryText(ctx, frameworkSession); ok {
				summary = value
			}
		}
		events := snapshotSessionEvents(frameworkSession)
		if len(events) == 0 {
			break
		}
		if pagePreview := chatPreview(projectChatMessages(frameworkSession)); pagePreview != "" {
			// Event pages walk backwards from the newest events. Older pages
			// therefore replace newer candidates so the list keeps its existing
			// "first visible user message" title semantics.
			preview = pagePreview
		}
		offset += len(events)
		if len(events) < limit {
			break
		}
	}
	return preview, summary, nil
}

func matchSessionViewFilters(entry storage.Session, appCode, channel, status, scope, owner string, unlinked bool) bool {
	if appCode != "" && entry.AppCode != appCode {
		return false
	}
	if status != "" && entry.Status != status {
		return false
	}
	if scope != "" && sessionScope(entry) != scope {
		return false
	}
	if owner != "" && entry.OwnerPlatformUserID != owner {
		return false
	}
	if unlinked && entry.OwnerPlatformUserID != "" {
		return false
	}
	if channel != "" {
		for _, conversation := range entry.Conversations {
			if conversation.Channel == channel {
				return true
			}
		}
		return false
	}
	return true
}

func sessionScope(entry storage.Session) string {
	for _, conversation := range entry.Conversations {
		if conversation.Scope == "group" {
			return "group"
		}
	}
	return "direct"
}
