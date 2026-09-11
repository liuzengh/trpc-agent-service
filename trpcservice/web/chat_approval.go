package web

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/governance"
)

type WebApprovalResolver interface {
	ResolveForRequester(context.Context, string, string, string, bool) (governance.PendingApproval, error)
}

type webApprovalCardPublisher interface {
	PublishApprovalCard(context.Context, string, string, string, channels.InteractiveCard) (string, error)
}

type webApprovalRequest struct {
	TenantID string `json:"tenant_id"`
	ActionID string `json:"action_id"`
}

func (c *consoleAPI) resolveChatApproval(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return
	}
	var body webApprovalRequest
	if err := decodeJSONBody(request, &body); err != nil {
		badRequest(writer, err.Error())
		return
	}
	body.TenantID, body.ActionID = strings.TrimSpace(body.TenantID), strings.TrimSpace(body.ActionID)
	if body.TenantID == "" || body.ActionID == "" {
		badRequest(writer, "tenant_id and action_id are required")
		return
	}
	if !requireTenantRead(writer, request, body.TenantID) {
		return
	}
	user, ok := sessionUser(request)
	if !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	token, approved, ok := governance.ParseApprovalAction(body.ActionID)
	if !ok {
		badRequest(writer, "approval action is invalid")
		return
	}
	if c.dependencies.Approvals == nil {
		writeJSON(writer, http.StatusServiceUnavailable, map[string]any{"error": "web approval is unavailable"})
		return
	}
	resolved, err := c.dependencies.Approvals.ResolveForRequester(request.Context(), body.TenantID, token, user.PlatformUserID, approved)
	if err != nil {
		switch {
		case errors.Is(err, governance.ErrApprovalExpired):
			writeJSON(writer, http.StatusGone, map[string]any{"error": "approval expired"})
		case errors.Is(err, governance.ErrApprovalResolved):
			writeJSON(writer, http.StatusConflict, map[string]any{"error": "approval already resolved"})
		case errors.Is(err, governance.ErrApprovalRouteMismatch):
			writeJSON(writer, http.StatusForbidden, map[string]any{"error": "forbidden: approval belongs to another user"})
		default:
			serverError(writer, "resolve web approval", err)
		}
		return
	}
	card := governance.ApprovalResultCard(resolved, approved)
	if publisher, ok := c.dependencies.ReplySubscriber.(webApprovalCardPublisher); ok {
		_, _ = publisher.PublishApprovalCard(request.Context(), resolved.TenantID, user.PlatformUserID, resolved.RequestID, card)
	}
	writeJSON(writer, http.StatusOK, map[string]any{"card": card})
}
