package web

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
)

type tenantSummaryResponse struct {
	TenantID    string `json:"tenant_id"`
	DisplayName string `json:"display_name"`
	Role        string `json:"role"`
	Status      string `json:"status"`
}

type memberCursorPayload struct {
	DisplayName    string `json:"name"`
	PlatformUserID string `json:"id"`
}

func memberPageRequest(request *http.Request) (identity.MemberPageRequest, error) {
	query := request.URL.Query()
	page := identity.MemberPageRequest{Limit: 50, Query: strings.TrimSpace(query.Get("q"))}
	if raw := strings.TrimSpace(query.Get("limit")); raw != "" {
		limit, err := strconv.Atoi(raw)
		if err != nil || limit < 1 || limit > 200 {
			return identity.MemberPageRequest{}, errors.New("limit must be between 1 and 200")
		}
		page.Limit = limit
	}
	if raw := strings.TrimSpace(query.Get("cursor")); raw != "" {
		encoded, err := base64.RawURLEncoding.DecodeString(raw)
		if err != nil {
			return identity.MemberPageRequest{}, errors.New("cursor is invalid")
		}
		var cursor memberCursorPayload
		if err := json.Unmarshal(encoded, &cursor); err != nil || strings.TrimSpace(cursor.PlatformUserID) == "" {
			return identity.MemberPageRequest{}, errors.New("cursor is invalid")
		}
		page.After = &identity.MemberCursor{DisplayName: cursor.DisplayName, PlatformUserID: cursor.PlatformUserID}
	}
	return page, nil
}

func memberNextCursor(cursor *identity.MemberCursor) string {
	if cursor == nil {
		return ""
	}
	encoded, _ := json.Marshal(memberCursorPayload{DisplayName: cursor.DisplayName, PlatformUserID: cursor.PlatformUserID})
	return base64.RawURLEncoding.EncodeToString(encoded)
}

func (c *consoleAPI) tenants(writer http.ResponseWriter, request *http.Request) {
	user, ok := sessionUser(request)
	if !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	if c.dependencies.Identities == nil {
		serverError(writer, "list tenants", errors.New("identity store is not configured"))
		return
	}
	switch request.Method {
	case http.MethodGet:
		summaries, err := c.dependencies.Identities.ListTenantSummaries(request.Context(), user.PlatformUserID, user.IsSystemAdmin)
		if err != nil {
			serverError(writer, "list tenants", err)
			return
		}
		result := make([]tenantSummaryResponse, 0, len(summaries))
		for _, summary := range summaries {
			result = append(result, tenantSummaryResponse{TenantID: summary.TenantID, DisplayName: summary.DisplayName, Role: string(summary.Role), Status: summary.Status})
		}
		writeJSON(writer, http.StatusOK, map[string]any{"tenants": result})
	case http.MethodPost:
		if !user.IsSystemAdmin {
			writeJSON(writer, http.StatusForbidden, map[string]any{"error": "forbidden: system administrator required"})
			return
		}
		var body struct {
			TenantID                   string `json:"tenant_id"`
			DisplayName                string `json:"display_name"`
			InitialAdminPlatformUserID string `json:"initial_admin_platform_user_id"`
		}
		if err := decodeJSONBody(request, &body); err != nil {
			badRequest(writer, err.Error())
			return
		}
		body.TenantID = strings.TrimSpace(body.TenantID)
		body.DisplayName = strings.TrimSpace(body.DisplayName)
		body.InitialAdminPlatformUserID = strings.TrimSpace(body.InitialAdminPlatformUserID)
		if err := c.dependencies.Identities.CreateTenant(request.Context(), body.TenantID, body.DisplayName, body.InitialAdminPlatformUserID); err != nil {
			badRequest(writer, err.Error())
			return
		}
		writer.WriteHeader(http.StatusCreated)
	default:
		methodNotAllowed(writer, http.MethodGet+", "+http.MethodPost)
	}
}

func (c *consoleAPI) updateTenant(writer http.ResponseWriter, request *http.Request) {
	user, ok := sessionUser(request)
	if !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !user.IsSystemAdmin {
		writeJSON(writer, http.StatusForbidden, map[string]any{"error": "forbidden: system administrator required"})
		return
	}
	if request.Method != http.MethodPatch {
		methodNotAllowed(writer, http.MethodPatch)
		return
	}
	tenantID := strings.TrimPrefix(request.URL.Path, "/api/v1/tenants/")
	if strings.TrimSpace(tenantID) == "" || strings.Contains(tenantID, "/") {
		badRequest(writer, "tenant ID is invalid")
		return
	}
	var body struct {
		Status string `json:"status"`
	}
	if err := decodeJSONBody(request, &body); err != nil {
		badRequest(writer, err.Error())
		return
	}
	if err := c.dependencies.Identities.SetTenantStatus(request.Context(), tenantID, body.Status); err != nil {
		switch {
		case errors.Is(err, identity.ErrTenantNotFound):
			notFound(writer, "tenant does not exist")
		case errors.Is(err, identity.ErrTenantNeedsAdmin):
			conflict(writer, err.Error())
		default:
			badRequest(writer, err.Error())
		}
		return
	}
	writer.WriteHeader(http.StatusNoContent)
}

func canAdminTenant(user identity.SessionUser, tenantID string) bool {
	for _, membership := range user.Tenants {
		if membership.TenantID == tenantID && membership.Status != "suspended" && membership.Role == identity.RoleAdmin {
			return true
		}
	}
	return false
}

func (c *consoleAPI) users(writer http.ResponseWriter, request *http.Request) {
	user, ok := sessionUser(request)
	if !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	if c.dependencies.Identities == nil {
		serverError(writer, "users", errors.New("identity store is not configured"))
		return
	}
	if !user.IsSystemAdmin {
		writeJSON(writer, http.StatusForbidden, map[string]any{"error": "forbidden: system administrator required"})
		return
	}
	switch request.Method {
	case http.MethodGet:
		pageRequest, err := memberPageRequest(request)
		if err != nil {
			badRequest(writer, err.Error())
			return
		}
		page, err := c.dependencies.Identities.ListPlatformUsers(request.Context(), pageRequest)
		if err != nil {
			serverError(writer, "list platform users", err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"users": page.Members, "next_cursor": memberNextCursor(page.Next)})
	case http.MethodPut:
		var body struct {
			PlatformUserID string `json:"platform_user_id"`
			Status         string `json:"status"`
			IsSystemAdmin  bool   `json:"is_system_admin"`
		}
		if err := decodeJSONBody(request, &body); err != nil {
			badRequest(writer, err.Error())
			return
		}
		body.PlatformUserID = strings.TrimSpace(body.PlatformUserID)
		body.Status = strings.TrimSpace(body.Status)
		if body.PlatformUserID == user.PlatformUserID {
			writeJSON(writer, http.StatusForbidden, map[string]any{"error": "forbidden: a system administrator cannot change their own system role or status"})
			return
		}
		if err := c.dependencies.Identities.UpdatePlatformUserAccess(request.Context(), body.PlatformUserID, body.Status, body.IsSystemAdmin); err != nil {
			badRequest(writer, err.Error())
			return
		}
		if c.dependencies.LoginSessions != nil {
			_ = c.dependencies.LoginSessions.DeleteForUser(request.Context(), body.PlatformUserID)
		}
		writer.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(writer, http.MethodGet+", "+http.MethodPut)
	}
}

func (c *consoleAPI) localUsers(writer http.ResponseWriter, request *http.Request) {
	user, ok := sessionUser(request)
	if !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	if !user.IsSystemAdmin {
		writeJSON(writer, http.StatusForbidden, map[string]any{"error": "forbidden: system administrator required"})
		return
	}
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return
	}
	var body struct {
		Username    string `json:"username"`
		DisplayName string `json:"display_name"`
		Email       string `json:"email"`
	}
	if err := decodeJSONBody(request, &body); err != nil {
		badRequest(writer, err.Error())
		return
	}
	temporaryPassword, err := identity.GenerateTemporaryPassword()
	if err != nil {
		serverError(writer, "generate temporary credential", err)
		return
	}
	hash, err := identity.HashLocalPassword(temporaryPassword)
	if err != nil {
		serverError(writer, "hash temporary credential", err)
		return
	}
	created, err := c.dependencies.Identities.CreateLocalUser(request.Context(), body.Username, body.DisplayName, body.Email, hash, true)
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{
		"platform_user_id":     created.PlatformUserID,
		"username":             strings.ToLower(strings.TrimSpace(body.Username)),
		"temporary_password":   temporaryPassword,
		"must_change_password": true,
	})
}

func (c *consoleAPI) resetLocalPassword(writer http.ResponseWriter, request *http.Request) {
	user, ok := sessionUser(request)
	if !ok || !user.IsSystemAdmin {
		writeJSON(writer, http.StatusForbidden, map[string]any{"error": "forbidden: system administrator required"})
		return
	}
	if request.Method != http.MethodPost {
		methodNotAllowed(writer, http.MethodPost)
		return
	}
	var body struct {
		PlatformUserID string `json:"platform_user_id"`
	}
	if err := decodeJSONBody(request, &body); err != nil {
		badRequest(writer, err.Error())
		return
	}
	c.resetLocalCredential(writer, request, strings.TrimSpace(body.PlatformUserID))
}

func (c *consoleAPI) resetLocalCredential(writer http.ResponseWriter, request *http.Request, platformUserID string) {
	if platformUserID == "" {
		badRequest(writer, "platform_user_id is required")
		return
	}
	oneTimeSecret, err := identity.GenerateTemporaryPassword()
	if err != nil {
		serverError(writer, "generate temporary credential", err)
		return
	}
	hash, err := identity.HashLocalPassword(oneTimeSecret)
	if err != nil {
		serverError(writer, "hash temporary credential", err)
		return
	}
	if err := c.dependencies.Identities.SetLocalPassword(request.Context(), platformUserID, hash, true); err != nil {
		if errors.Is(err, identity.ErrLocalCredentialNotFound) {
			notFound(writer, "local credential does not exist")
			return
		}
		serverError(writer, "reset local credential", err)
		return
	}
	if c.dependencies.LoginSessions != nil {
		_ = c.dependencies.LoginSessions.DeleteForUser(request.Context(), platformUserID)
	}
	writeJSON(writer, http.StatusOK, map[string]any{"temporary_password": oneTimeSecret, "must_change_password": true})
}

func (c *consoleAPI) tenantMembers(writer http.ResponseWriter, request *http.Request) {
	user, ok := sessionUser(request)
	if !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return
	}
	if c.dependencies.Identities == nil {
		serverError(writer, "tenant members", errors.New("identity store is not configured"))
		return
	}
	tenantID := resolveTenantParam(request)
	if request.Method == http.MethodPut {
		var body struct {
			TenantID                 string `json:"tenant_id"`
			PlatformUserID           string `json:"platform_user_id"`
			Role                     string `json:"role"`
			Status                   string `json:"status"`
			ConversationContentAudit *bool  `json:"conversation_content_audit,omitempty"`
		}
		if err := decodeJSONBody(request, &body); err != nil {
			badRequest(writer, err.Error())
			return
		}
		tenantID = strings.TrimSpace(body.TenantID)
		if !canAdminTenant(user, tenantID) {
			writeJSON(writer, http.StatusForbidden, map[string]any{"error": "forbidden: tenant admin required"})
			return
		}
		targetUserID := strings.TrimSpace(body.PlatformUserID)
		targetRole := identity.Role(strings.TrimSpace(body.Role))
		targetStatus := strings.TrimSpace(body.Status)
		if targetUserID == user.PlatformUserID && !user.IsSystemAdmin && (targetRole != identity.RoleAdmin || targetStatus != "active") {
			writeJSON(writer, http.StatusForbidden, map[string]any{"error": "forbidden: a tenant admin cannot change their own tenant role"})
			return
		}
		if body.ConversationContentAudit != nil && *body.ConversationContentAudit && (targetRole != identity.RoleAdmin || targetStatus != "active") {
			badRequest(writer, "conversation content audit requires an active tenant administrator")
			return
		}
		if err := c.dependencies.Identities.SetTenantMembership(request.Context(), tenantID, targetUserID, targetRole, targetStatus); err != nil {
			badRequest(writer, err.Error())
			return
		}
		if body.ConversationContentAudit != nil && targetRole == identity.RoleAdmin && targetStatus == "active" {
			if err := c.dependencies.Identities.SetConversationContentAudit(request.Context(), tenantID, targetUserID, *body.ConversationContentAudit); err != nil {
				badRequest(writer, err.Error())
				return
			}
		}
		if c.dependencies.LoginSessions != nil {
			_ = c.dependencies.LoginSessions.DeleteForUser(request.Context(), targetUserID)
		}
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	if request.Method != http.MethodGet {
		methodNotAllowed(writer, http.MethodGet+", "+http.MethodPut)
		return
	}
	if tenantID == "" {
		badRequest(writer, "tenant is required")
		return
	}
	if !canAdminTenant(user, tenantID) {
		writeJSON(writer, http.StatusForbidden, map[string]any{"error": "forbidden: tenant admin required"})
		return
	}
	pageRequest, err := memberPageRequest(request)
	if err != nil {
		badRequest(writer, err.Error())
		return
	}
	if strings.EqualFold(strings.TrimSpace(request.URL.Query().Get("view")), "candidates") {
		page, listErr := c.dependencies.Identities.ListTenantMemberCandidates(request.Context(), tenantID, pageRequest)
		if listErr != nil {
			serverError(writer, "list tenant member candidates", listErr)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"candidates": page.Members, "next_cursor": memberNextCursor(page.Next)})
		return
	}
	page, listErr := c.dependencies.Identities.ListTenantMembers(request.Context(), tenantID, pageRequest)
	if listErr != nil {
		serverError(writer, "list tenant members", listErr)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"members": page.Members, "next_cursor": memberNextCursor(page.Next)})
}
