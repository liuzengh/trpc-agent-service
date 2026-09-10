package web

import (
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/identity"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
)

func resolveTenantParam(request *http.Request) string {
	if tenantID := strings.TrimSpace(request.URL.Query().Get("tenant")); tenantID != "" {
		return tenantID
	}
	if tenantID := strings.TrimSpace(request.URL.Query().Get("tenant_id")); tenantID != "" {
		return tenantID
	}
	return strings.TrimSpace(request.Header.Get("X-Active-Tenant"))
}

func canAccessTenant(user identity.SessionUser, tenantID string) bool {
	for _, membership := range user.Tenants {
		if membership.TenantID == tenantID && membership.Status != "suspended" {
			return true
		}
	}
	return false
}

func canWriteTenant(user identity.SessionUser, tenantID string) bool {
	for _, membership := range user.Tenants {
		if membership.TenantID == tenantID && membership.Status != "suspended" && membership.Role == identity.RoleAdmin {
			return true
		}
	}
	return false
}

func canReadSession(user identity.SessionUser, entry storage.Session) bool {
	return entry.OwnerPlatformUserID != "" && entry.OwnerPlatformUserID == user.PlatformUserID
}

func canAuditConversationContent(user identity.SessionUser, tenantID string) bool {
	for _, membership := range user.Tenants {
		if membership.TenantID == tenantID && membership.Status != "suspended" && membership.Role == identity.RoleAdmin && membership.ConversationContentAudit {
			return true
		}
	}
	return false
}

func sessionUser(request *http.Request) (identity.SessionUser, bool) {
	return identity.UserFromContext(request.Context())
}

func requireTenantRead(writer http.ResponseWriter, request *http.Request, tenantID string) bool {
	user, ok := sessionUser(request)
	if !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return false
	}
	if !canAccessTenant(user, tenantID) {
		writeJSON(writer, http.StatusForbidden, map[string]any{"error": "forbidden: not a member of tenant " + tenantID})
		return false
	}
	return true
}

func requireTenantWrite(writer http.ResponseWriter, request *http.Request, tenantID string) bool {
	user, ok := sessionUser(request)
	if !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return false
	}
	if !canAccessTenant(user, tenantID) {
		writeJSON(writer, http.StatusForbidden, map[string]any{"error": "forbidden: not a member of tenant " + tenantID})
		return false
	}
	if !canWriteTenant(user, tenantID) {
		writeJSON(writer, http.StatusForbidden, map[string]any{"error": "forbidden: write requires tenant administrator role in tenant " + tenantID})
		return false
	}
	return true
}

func requireSystemAdmin(writer http.ResponseWriter, request *http.Request) bool {
	user, ok := sessionUser(request)
	if !ok {
		http.Error(writer, "unauthorized", http.StatusUnauthorized)
		return false
	}
	if !user.IsSystemAdmin {
		writeJSON(writer, http.StatusForbidden, map[string]any{"error": "forbidden: system administrator required"})
		return false
	}
	return true
}
