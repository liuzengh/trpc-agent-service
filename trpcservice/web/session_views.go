package web

import (
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
	for _, entry := range entries {
		if !baseFilter(entry) || !matchSessionViewFilters(entry, appCode, channel, status, scope, owner, unlinked) {
			continue
		}
		view := sessionView{Session: entry, MessageCount: entry.Revision}
		if includePersonalContent && c.dependencies.AgentSessions != nil && entry.AppCode != "" && entry.SubjectID != "" && entry.SessionKey != "" {
			personalView := personalSessionView{sessionView: view}
			service, serviceErr := c.agentSessionService(request.Context(), tenantID, entry.AppCode)
			if serviceErr != nil {
				serverError(writer, "resolve agent Session backend", serviceErr)
				return
			}
			frameworkSession, getErr := service.GetSession(request.Context(), agentsession.Key{
				AppName: tenantID + "/" + entry.AppCode, UserID: entry.SubjectID, SessionID: entry.SessionKey,
			})
			if getErr == nil && frameworkSession != nil {
				personalView.Preview = chatPreview(projectChatMessages(frameworkSession))
				if summary, ok := service.GetSessionSummaryText(request.Context(), frameworkSession); ok {
					personalView.Summary = summary
				}
			}
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
